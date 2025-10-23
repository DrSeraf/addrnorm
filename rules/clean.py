# -*- coding: utf-8 -*-
"""
addrnorm.clean
Пост-очистка результатов нормализации адресов:
- валидация/починка zip по стране (маски, whitelist длины и пр.)
- маппинг синонимов/алиасов (Bkk -> Bangkok, Chon Buri -> Chonburi и т.п.)
- удаление мусора и неадресных токенов из street (имена, email, телефон, хештеги и пр.)
- устранение дублей locality/region (echo: "Nonthaburi,Nonthaburi")
- лёгкая нормализация регистра и пробелов
- всё безопасно по умолчанию, агрессивные правки включаются правилами

Можно использовать как библиотеку:
    from addrnorm.clean import Cleaner
    df = Cleaner().clean_df(df)

или как CLI:
    python -m addrnorm.clean --in input.csv --out output.csv [--rules rules.yaml]
"""

from __future__ import annotations
import re
import json
import sys
import argparse
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Pattern
import unicodedata

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

import pandas as pd


_DEFAULT_COUNTRY_ZIP = {
    # длины и regex-паттерны для стран (минимальный набор; расширяй правилами)
    "THAILAND": r"^(10|11|12|13|14|15|16|17|18|20|21|22|23|24|25|26|27|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56|57|58|60|61|62|63|64|65|66|67|70|71|72|73|74|75|76|77)\d{3}$",
    "INDONESIA": r"^\d{5}$",
    "MALAYSIA": r"^\d{5}$",
    "AUSTRALIA": r"^\d{4}$",
    "UNITED STATES": r"^\d{5}(-\d{4})?$",
    "US": r"^\d{5}(-\d{4})?$",
    "UNITED KINGDOM": r"^(GIR ?0AA|[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2})$",
    "UK": r"^(GIR ?0AA|[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2})$",
    "BRAZIL": r"^\d{5}-?\d{3}$",
    "BRASIL": r"^\d{5}-?\d{3}$",
}

# алиасы/синонимы (локалити и регион), безопасные для международки
_DEFAULT_SYNONYMS = {
    # Бангкок и пр.
    "BKK": "Bangkok",
    "BANGKOKK": "Bangkok",
    "BANGKOK-NOI": "Bangkok Noi",
    "BANG NA": "Bang Na",
    "BANG LAMUNG": "Bang Lamung",
    "CHON BURI": "Chonburi",
    "CHIANGMAI": "Chiang Mai",
    "NONTHABURI": "Nonthaburi",
    "PHUKETT": "Phuket",
    # распространённые опечатки
    "BANGKOG": "Bangkok",
    "RAYONGG": "Rayong",
}

# токены, которые с высокой вероятностью НЕ являются частью улицы
# (удаляются из street полностью)
_STREET_TRASH_PATTERNS: List[Pattern] = [
    re.compile(r"\b(?:email|e-mail|mail|gmail|hotmail|yahoo|outlook|live)\b", re.I),
    re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),  # email
    re.compile(r"\+?\d[\d \-()]{6,}\d"),                         # телефон
    re.compile(r"#\w+"),                                        # хэштеги/метки
]

# атрибуты помещений — по ТЗ их надо просто удалять
_STREET_UNIT_ATTRS = re.compile(
    r"\b(?:unit|apt|apartment|room|rm|bldg|building|flr|floor|tower|blk|block|lvl|level|dept|suite|ste)\b\.?",
    re.I
)

# «шумовые» слова — оставляем цифры/дом/сой/роуд и т.п., выкидываем одиночные бессмысленные токены
_NON_ADDRESSY = re.compile(r"^[A-Za-z]{2,}$")

# --- БАЗОВАЯ ЧИСТКА ДЛЯ ПАЙПЛАЙНА ---
_WS_RE = re.compile(r"\s+")
_PUNCT_EDGE_RE = re.compile(r"^[\s,;/\-_.]+|[\s,;/\-_.]+$")

def clean_value(value: str | None) -> str | None:
    """
    Базовая чистка строк для этапа CLEAN пайплайна:
      - None/null/NaN → None
      - Unicode NFKC нормализация
      - trim + схлопывание пробелов
      - удаление лишней пунктуации по краям
    Совместимо с ожиданиями pipeline.py
    """
    if value is None:
        return None
    s = str(value).strip()
    low = s.lower()
    # расширенный список пустых значений
    null_tokens = {
        "nan", "none", "null", "na", "n/a", "n.a.", "n.a", "n a",
        "-", "—", "not available", "not applicable", "unknown"
    }
    if not s or low in null_tokens:
        return None
    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    s = _WS_RE.sub(" ", s)
    s = _PUNCT_EDGE_RE.sub("", s)
    s = s.strip()
    return s or None


def _safe_upper(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def _norm_spaces(s: str) -> str:
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    return s.strip(" ,")


def _load_rules(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        if path.lower().endswith((".yaml", ".yml")):
            if yaml is None:
                raise RuntimeError("PyYAML не установлен, а передан YAML-файл правил.")
            return yaml.safe_load(f) or {}
        return json.load(f) or {}


@dataclass
class CleanerConfig:
    # глобальные дефолты; могут расширяться правилами
    country_zip: Dict[str, str]
    synonyms: Dict[str, str]
    drop_unit_attrs: bool = True
    drop_emails_phones_from_street: bool = True
    fix_echo_locality_region: bool = True
    # если True — странные одиночные токены в street (Sdfasdf) удаляем
    drop_non_addressy_single_tokens: bool = True


class Cleaner:
    def __init__(self, rules: Optional[Dict[str, Any]] = None):
        rules = rules or {}
        cz = dict(_DEFAULT_COUNTRY_ZIP)
        cz.update({k.upper(): v for k, v in (rules.get("country_zip_regex", {}) or {}).items()})
        syn = dict(_DEFAULT_SYNONYMS)
        syn.update({k.upper(): v for k, v in (rules.get("synonyms", {}) or {}).items()})

        self.cfg = CleanerConfig(
            country_zip=cz,
            synonyms=syn,
            drop_unit_attrs=rules.get("drop_unit_attrs", True),
            drop_emails_phones_from_street=rules.get("drop_emails_phones_from_street", True),
            fix_echo_locality_region=rules.get("fix_echo_locality_region", True),
            drop_non_addressy_single_tokens=rules.get("drop_non_addressy_single_tokens", True),
        )

    # ---------- ПУБЛИЧНЫЕ API ----------

    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # работаем только по известным колонкам
        cols = {c.lower(): c for c in df.columns}
        street = cols.get("street")
        district = cols.get("district")
        locality = cols.get("locality")
        region = cols.get("region")
        country = cols.get("country")
        zipc = cols.get("zip")

        if street:
            df[street] = df[street].map(self._clean_street)
        if locality:
            df[locality] = df[locality].map(self._apply_synonyms_safe)
        if region:
            df[region] = df[region].map(self._apply_synonyms_safe)
        if self.cfg.fix_echo_locality_region and locality and region:
            df = self._fix_echo(df, locality, region)

        if zipc:
            df[zipc] = df.apply(
                lambda r: self._validate_zip(r.get(zipc), r.get(country)), axis=1
            )

        # финальная косметика — пробелы/запятые
        for c in (street, district, locality, region, country, zipc):
            if c:
                df[c] = df[c].map(lambda x: _norm_spaces(x) if isinstance(x, str) else x)

        return df

    # ---------- ВНУТРЕННИЕ МЕТОДЫ ----------

    def _apply_synonyms_safe(self, x: Any) -> Any:
        if not isinstance(x, str) or not x.strip():
            return x
        u = _safe_upper(x)
        if u in self.cfg.synonyms:
            return self.cfg.synonyms[u]
        return x

    def _clean_street(self, s: Any) -> Any:
        if not isinstance(s, str):
            return s
        txt = s.strip()
        if not txt:
            return txt

        # 1) удаляем email/телефон/метки
        if self.cfg.drop_emails_phones_from_street:
            for pat in _STREET_TRASH_PATTERNS:
                txt = pat.sub("", txt)

        # 2) удаляем атрибуты помещения (и числа сразу после них)
        if self.cfg.drop_unit_attrs:
            txt = _STREET_UNIT_ATTRS.sub("", txt)
            txt = re.sub(r"\b(?:FL|FLOOR|LVL|LEVEL|RM|ROOM|APT|SUITE|STE|UNIT|BLDG|BLK|BLOCK)\b\.?\s*\d+[A-Z\-\/]*",
                         "", txt, flags=re.I)

        # 3) убираем мусорные одинокие токены «словесного вида»
        if self.cfg.drop_non_addressy_single_tokens:
            tokens = [t for t in re.split(r"[,\s]+", txt) if t]
            if len(tokens) == 1 and _NON_ADDRESSY.match(tokens[0]) and not re.search(r"\d", tokens[0]):
                # оставим пусто, если это одиночное слово без цифр и признаков «улицы»
                return ""

        # 4) нормализуем пробелы/знаки
        txt = _norm_spaces(txt)

        # 5) финальная зачистка висячих пунктуаций
        txt = txt.strip(" ,;")
        return txt

    def _fix_echo(self, df: pd.DataFrame, locality: str, region: str) -> pd.DataFrame:
        def _fix(lv, rv):
            if isinstance(lv, str) and isinstance(rv, str):
                if lv.strip() and rv.strip():
                    if _safe_upper(lv) == _safe_upper(rv):
                        # если полностью совпали — region обнулим (или наоборот; выберем обнулить region)
                        return lv, ""
            return lv, rv

        pair = df[[locality, region]].apply(lambda r: _fix(r[locality], r[region]), axis=1)
        df[[locality, region]] = list(pair)
        return df

    def _validate_zip(self, zip_val: Any, country_val: Any) -> Any:
        if not isinstance(zip_val, str) or not zip_val.strip():
            return zip_val
        z = zip_val.strip()
        c = (_safe_upper(country_val) if isinstance(country_val, str) else "").strip()

        # «мусорные» ZIP типа PHAYATHAI2, ABC123 — выбросим, если не матчится паттерну страны
        pattern = self.cfg.country_zip.get(c)
        if pattern:
            if not re.match(pattern, z, flags=re.I):
                return ""  # по ТЗ: лучше пусто, чем мусор
            # косметика по странам
            if c in ("BRAZIL", "BRASIL"):
                z = re.sub(r"(\d{5})[- ]?(\d{3})", r"\1-\2", z)
            if c in ("UNITED KINGDOM", "UK"):
                z = z.upper().replace(" ", "")
                # вставим пробел перед последними 3 символами
                if len(z) > 3:
                    z = z[:-3] + " " + z[-3:]
        else:
            # если страна не известна правилам, но ZIP очевидный мусор — уберём алфавитные хвосты
            # например PHAYATHAI2 -> "" (всё не цифры и не валидно)
            if not re.search(r"\d", z):
                return ""
        return z


# ---------------- CLI для одиночного прогона ----------------

def _cli():
    ap = argparse.ArgumentParser(description="Post-clean for addrnorm output")
    ap.add_argument("--in", dest="src", required=True)
    ap.add_argument("--out", dest="dst", required=True)
    ap.add_argument("--rules", dest="rules", default=None,
                    help="YAML/JSON с правилами (опционально)")
    args = ap.parse_args()

    rules = _load_rules(args.rules) if args.rules else {}
    df = pd.read_csv(args.src, dtype=str, keep_default_na=False)
    cleaner = Cleaner(rules)
    out = cleaner.clean_df(df)
    out.to_csv(args.dst, index=False)


if __name__ == "__main__":
    _cli()
