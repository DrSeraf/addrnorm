# addrnorm/pipeline.py
from __future__ import annotations

import os, time, re
from unidecode import unidecode
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pycountry, yaml

from .rules import clean, normalize, validate
from .parsers.address_extract import extract_from_address
from .utils import io_utils, loggingx, report as report_mod

OUTPUT_COLS = ["street", "district", "locality", "region", "country", "zip"]

# эвристики
RE_HAS_DIGIT = re.compile(r"\d")
RE_CITYZIP_MIX = re.compile(r"[A-Za-z].*\d|\d.*[A-Za-z]")
RE_STREET_WORD = re.compile(
    r"\b(st|street|road|rd|ave|avenue|blvd|lane|ln|hwy|highway|rue|soi|jalan|jl\.|av|calle|via|str|str\.)\b",
    re.I,
)
RE_ZIP_TOKEN = re.compile(r"^[A-Z0-9][A-Z0-9 \-]{2,9}$", re.I)
RE_ZIP_PURE_DIG = re.compile(r"^\d{4,6}$")

class Context:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.report_data = report_mod.init_report(self)
        self.samples: Dict[str, Dict[str, list]] = {}
        self.profiles_data: Dict[str, Any] = {}
        self.total_rows = 0
        self.processed_rows = 0
        self.start_ts = time.time()
        self.current_alpha2 = None

def _load_profiles(profiles: list[str]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    base_dir = os.path.dirname(__file__)
    for name in profiles:
        path = os.path.join(base_dir, "rules", "profiles", f"{name}.yml")
        if not os.path.exists(path):
            path = os.path.join("addrnorm", "rules", "profiles", f"{name}.yml")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        for k, v in data.items():
            if isinstance(v, dict):
                merged.setdefault(k, {}).update(v)
            elif isinstance(v, list):
                merged.setdefault(k, []).extend(v)
            else:
                merged[k] = v
    return merged

def _load_user_rules(rules_path: str | None) -> Dict[str, Any]:
    """
    Загружаем rules.yaml и приводим к структуре профилей:
      - country_zip_regex (по названию страны) -> zip_patterns[ALPHA2]
      - synonyms -> locality_aliases (глобальные)
    Остальные известные флаги прокидываем как есть.
    """
    if not rules_path:
        return {}
    if not os.path.exists(rules_path):
        return {}
    try:
        with open(rules_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        return {}

    merged: Dict[str, Any] = {}

    # ZIP паттерны: имя страны -> alpha2
    zip_src = data.get("country_zip_regex") or {}
    if isinstance(zip_src, dict) and zip_src:
        zip_patterns: Dict[str, str] = {}
        for country_name, pat in zip_src.items():
            if not country_name or not pat:
                continue
            try:
                c = pycountry.countries.lookup(str(country_name))
                a2 = getattr(c, "alpha_2", None)
                if a2:
                    zip_patterns[a2] = str(pat)
            except Exception:
                continue
        if zip_patterns:
            merged["zip_patterns"] = zip_patterns

    # Синонимы → алиасы городов (глобально)
    syn = data.get("synonyms") or {}
    if isinstance(syn, dict) and syn:
        merged["locality_aliases"] = {str(k).strip().lower(): str(v) for k, v in syn.items() if k and v}

    # Прочие флаги сохраняем на будущее
    for k in ("drop_unit_attrs", "drop_emails_phones_from_street", "fix_echo_locality_region", "drop_non_addressy_single_tokens"):
        if k in data:
            merged[k] = data.get(k)

    return merged

def _compose_street(existing_street: str | None, parsed: dict) -> str:
    if existing_street:
        return existing_street
    road = (parsed.get("road") or "").strip()
    houseno = (parsed.get("house_number") or "").strip()
    unit = (parsed.get("unit") or "").strip()
    parts = [p for p in (road, houseno) if p]
    s = ", ".join(parts)
    if unit:
        s = f"{s}, {unit}" if s else unit
    return s

def _promote_address_like(row: dict) -> dict:
    """Если address пуст, но в одном из полей лежит полный адрес — перенести в address и очистить поле-источник."""
    if row.get("address"):
        return row

    def looks_like_addr(s: str) -> bool:
        # ≥2 запятых ИЛИ (есть цифры и слово улицы) ИЛИ (есть цифры и есть город/zip в других полях)
        if s.count(",") >= 2:
            return True
        if RE_HAS_DIGIT.search(s) and RE_STREET_WORD.search(s):
            return True
        return False

    # кейс: locality выглядит как ZIP (например "10220"), а street содержит запятые/цифры → это адрес в street
    loc = (row.get("locality") or "").strip()
    street = (row.get("street") or "").strip()
    if not row.get("address") and street and (RE_ZIP_PURE_DIG.match(loc) or RE_ZIP_TOKEN.match(loc)):
        if street.count(",") >= 1 or RE_HAS_DIGIT.search(street):
            row["address"] = street
            row["street"] = ""
            return row

    for key in ("region", "street", "district", "locality"):
        val = row.get(key)
        if not val:
            continue
        s = str(val)
        if looks_like_addr(s):
            row["address"] = s
            row[key] = ""
            break
    return row

def _repair_misaligned_row(row: dict) -> dict:
    """Чистим явные смещения: страна с мусором, регион как улица, смешанные city+zip в нецелевых столбцах."""
    if not row.get("address"):
        return row
    if row.get("country"):
        try:
            pycountry.countries.lookup(str(row["country"]))
        except Exception:
            row["country"] = ""
    if row.get("region") and RE_HAS_DIGIT.search(str(row["region"])):
        row["region"] = ""
    if not row.get("country") and row.get("district"):
        try:
            pycountry.countries.lookup(str(row["district"]))
            row["country"] = row["district"]; row["district"] = ""
        except Exception:
            pass
    for key in ("country", "region", "district"):
        val = row.get(key)
        if val and RE_CITYZIP_MIX.search(str(val)):
            row[key] = ""
    return row

def run_job(**kwargs):
    ctx = Context(**kwargs)
    # 1) профили
    prof = _load_profiles(ctx.profiles)
    # 2) rules.yaml (если передали путь)
    user_rules = _load_user_rules(kwargs.get("rules_path") or kwargs.get("rules"))
    # 3) мёрдж
    for k, v in (user_rules or {}).items():
        if isinstance(v, dict):
            prof.setdefault(k, {}).update(v)
        elif isinstance(v, list):
            prof.setdefault(k, []).extend(v)
        else:
            prof[k] = v
    ctx.profiles_data = prof

    input_path: str = kwargs["input_path"]
    ctx.input_path = input_path
    output_path: str = ctx.output_path

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    os.makedirs(ctx.samples_dir, exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)

    processed = 0
    header_written = False
    t0 = time.time()

    for chunk in io_utils.read_csv_chunks(input_path, ctx.chunksize, ctx.encoding, ctx.sep):
        ctx.total_rows += len(chunk)
        df = io_utils.filter_target_columns(chunk)

        row_ids = {idx: (ctx.processed_rows + i + 1) for i, idx in enumerate(df.index)}

        # CLEAN
        for col in ["street","district","locality","region","country","zip","address"]:
            before = df[col].copy()
            df[col] = df[col].map(lambda v: clean.clean_value(v))
            changed_mask = (before.fillna("") != df[col].fillna(""))
            for idx in df.index[changed_mask]:
                target_col = col if col != "address" else "street"
                loggingx.collect_sample(target_col, "clean", before.get(idx), df[col].get(idx), row_ids[idx], ctx)
                report_mod.update_report(ctx.report_data, target_col, "clean")

        # PROMOTE_ADDRESS → REPAIR
        for idx, row in df.iterrows():
            fixed = _promote_address_like({k: row.get(k) for k in ["street","district","locality","region","country","zip","address"]})
            for k, v in fixed.items(): df.at[idx, k] = v
        for idx, row in df.iterrows():
            fixed = _repair_misaligned_row({k: row.get(k) for k in ["street","district","locality","region","country","zip","address"]})
            for k, v in fixed.items(): df.at[idx, k] = v

        # EXTRACT — Libpostal, с параллелизмом и опцией составления address из полей
        if (ctx.mode in ("fill-missing-only","extract-all-to-fill")) or ctx.street_from_address or getattr(ctx, "libpostal_always", False):
            # Подготовим очередь адресов для парсинга
            to_parse: List[Tuple[Any, str]] = []  # (idx, address_string)
            use_libpostal = bool(ctx.libpostal_url)
            for idx, row in df.iterrows():
                addr = (row.get("address") or "").strip()
                if not addr and getattr(ctx, "libpostal_always", False):
                    parts = [
                        str(row.get("street") or "").strip(),
                        str(row.get("district") or "").strip(),
                        str(row.get("locality") or "").strip(),
                        str(row.get("region") or "").strip(),
                        str(row.get("zip") or "").strip(),
                        str(row.get("country") or "").strip(),
                    ]
                    addr = ", ".join([p for p in parts if p])
                if not addr:
                    continue
                to_parse.append((idx, addr))

            def _do_parse(a: str) -> dict:
                return extract_from_address(a, ctx.mode, ctx.libpostal_url if use_libpostal else None)

            parsed_map: Dict[Any, dict] = {}
            if to_parse:
                if int(getattr(ctx, "concurrency", 1) or 1) > 1:
                    workers = int(getattr(ctx, "concurrency", 1) or 1)
                    with ThreadPoolExecutor(max_workers=workers) as ex:
                        results = ex.map(lambda item: (item[0], _do_parse(item[1])), to_parse, chunksize=64)
                        for idx2, parsed in results:
                            parsed_map[idx2] = parsed
                else:
                    for idx2, addr in to_parse:
                        parsed_map[idx2] = _do_parse(addr)

            # Применяем parsed к строкам
            for idx, row in df.iterrows():
                parsed = parsed_map.get(idx) or {}
                if not parsed:
                    continue
                full_name = None
                if "fullname" in df.columns:
                    try:
                        full_name = str(row.get("fullname") or "").strip()
                    except Exception:
                        full_name = None
                if ctx.street_from_address:
                    if not row.get("street"):
                        ns = _compose_street(row.get("street"), parsed)
                        if ns and ns != row.get("street"):
                            df.at[idx,"street"] = ns
                            loggingx.collect_sample("street","extracted", row.get("street"), ns, row_ids[idx], ctx)
                            report_mod.update_report(ctx.report_data, "street", "extracted")
                else:
                    for target, src in [("street",None),("locality","city"),("district","suburb"),
                                        ("region","state"),("country","country"),("zip","zip")]:
                        if ctx.mode == "fill-missing-only" and row.get(target):
                            continue
                        nv = _compose_street("", parsed) if target=="street" else (parsed.get(src or "") or "")
                        # защита от «подхвата» ФИО как города/региона
                        if nv and full_name and target in ("locality","region"):
                            if unidecode(nv).strip().lower() == unidecode(full_name).strip().lower():
                                nv = ""
                        if nv and (row.get(target) != nv):
                            df.at[idx, target] = nv
                            loggingx.collect_sample(target,"extracted", None, nv, row_ids[idx], ctx)
                            report_mod.update_report(ctx.report_data, target, "extracted")

                # echo-fix после EXTRACT: если locality == region, очищаем region
                lk = unidecode(str(df.at[idx, "locality"] or "")).strip().lower()
                rk = unidecode(str(df.at[idx, "region"] or "")).strip().lower()
                if lk and rk and lk == rk:
                    df.at[idx, "region"] = ""

        # NORMALIZE
        for idx, row in df.iterrows():
            row_dict = {k: row.get(k) for k in OUTPUT_COLS}
            normd = normalize.normalize_fields(row_dict, ctx)
            for col in OUTPUT_COLS:
                b, a = row_dict.get(col), normd.get(col)
                if b != a:
                    loggingx.collect_sample(col, "removed" if (b and not a) else "normalize", b, a, row_ids[idx], ctx)
                    report_mod.update_report(ctx.report_data, col, "removed" if (b and not a) else "normalize")
                    df.at[idx, col] = a
                else:
                    report_mod.update_report(ctx.report_data, col, "unchanged")

        # VALIDATE
        for idx, row in df.iterrows():
            alpha2 = None
            country = row.get("country")
            if country:
                try:
                    c = pycountry.countries.lookup(country)
                    alpha2 = getattr(c, "alpha_2", None)
                except Exception:
                    pass
            ctx.current_alpha2 = alpha2

            before_loc, before_reg = row.get("locality"), row.get("region")
            row_dict = {k: row.get(k) for k in OUTPUT_COLS}
            row_dict, flags = validate.validate_fields(row_dict, ctx)

            if "locality_fuzzy_fixed" in flags and row_dict.get("locality") != before_loc:
                loggingx.collect_sample("locality","fuzzy_fixed", before_loc, row_dict.get("locality"), row_ids[idx], ctx)
                report_mod.update_report(ctx.report_data, "locality", "fuzzy_fixed")
            if "region_fuzzy_fixed" in flags and row_dict.get("region") != before_reg:
                loggingx.collect_sample("region","fuzzy_fixed", before_reg, row_dict.get("region"), row_ids[idx], ctx)
                report_mod.update_report(ctx.report_data, "region", "fuzzy_fixed")
            if "bad_zip_format" in flags:
                ctx.report_data["conflicts"]["zip_format_fail"] += 1

            for col in OUTPUT_COLS:
                df.at[idx, col] = row_dict.get(col)

        # WRITE: extras (как есть) → целевые 6 → address (в конце)
        all_cols = list(df.columns)
        target_set = set(io_utils.TARGET_COLS)
        extras = [c for c in all_cols if c not in target_set]
        out_columns = []
        out_columns.extend(extras)
        out_columns.extend(OUTPUT_COLS)
        out_columns.append("address")
        # уникализуем, сохраняя порядок и существование в df
        seen = set()
        ordered_cols = []
        for c in out_columns:
            if c in seen:
                continue
            if c in df.columns:
                ordered_cols.append(c)
                seen.add(c)
        io_utils.write_chunk(df[ordered_cols].copy(), output_path, ctx.sep, ctx.quote_all, header=not header_written, columns=ordered_cols)
        header_written = True

        ctx.processed_rows += len(df)
        dt = max(1e-6, time.time() - t0)
        speed = ctx.processed_rows / dt
        loggingx.log_progress(ctx.total_rows, ctx.processed_rows, speed, "—", ctx.quiet)

    ctx.report_data["input"]["rows_total"] = ctx.processed_rows
    input_basename = os.path.splitext(os.path.basename(input_path))[0]
    loggingx.flush_samples(ctx.samples_dir, input_basename, ctx.samples)
    report_mod.finalize_report(ctx.report_data, ctx, ctx.report_path)
