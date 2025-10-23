# addrnorm/utils/io_utils.py
from __future__ import annotations
import csv
from typing import Iterable, List, Dict, Optional
import pandas as pd
import re
import sys

TARGET_COLS = ["street","district","locality","region","country","zip","address"]
OUTPUT_COLS = ["street","district","locality","region","country","zip"]

def _normalize_header(raw: List[str]) -> List[str]:
    # приводим имена к ожидаемым; прочее оставляем как есть
    return [h.strip() for h in raw]

def _row_to_record(row: List[str], header: List[str]) -> Dict[str,str]:
    """
    Собираем запись:
      - сохраняем ВСЕ исходные колонки как есть (не теряем данные)
      - гарантируем наличие TARGET_COLS
      - address дополняем "хвостом" из нецелевых колонок (кроме явных контактов)
    """
    rec: Dict[str, str] = {}
    n = min(len(row), len(header))

    CONTACT_COL_RE = re.compile(
        r"(email|e-mail|mail|phone|mobile|tel|fax|contact|whatsapp|line|wechat|skype|name|full\s*name|first\s*name|last\s*name)",
        re.I,
    )
    EMAIL_RE  = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
    PHONE_RE  = re.compile(r"(?:\+?\d[\d \-()]{6,}\d)|(?:\b\d{8,}\b)")

    # 1) Сохраняем все входные столбцы как есть
    for i in range(n):
        col = header[i].strip()
        rec[col] = row[i].strip()

    # extras -> address, только если это уместно:
    # - если во входе есть колонка address ИЛИ
    # - если нет ни одной из целевых address-колонок (street/locality/region/country/zip)
    has_address_col = any(h.strip().lower() == "address" for h in header)
    has_any_target = any(h.strip().lower() in {"street","district","locality","region","country","zip"} for h in header)

    extras: List[str] = []
    if has_address_col or not has_any_target:
        for i in range(n):
            col = header[i].strip()
            if col in {"street","district","locality","region","country","zip","address"}:
                continue
            if CONTACT_COL_RE.search(col):
                continue  # столбец явно контактный/именной — игнор
            v = row[i].strip()
            if not v or v.lower() in {"nan","null","none","na","n/a","n.a.","n.a","n a","-","—"}:
                continue
            if EMAIL_RE.search(v) or PHONE_RE.search(v):
                continue  # явный контакт в значении — игнор
            # легкая эвристика «адресности»: наличие цифр и букв ИЛИ запятая в значении
            if not (re.search(r"\d", v) and re.search(r"[A-Za-zА-Яа-я]", v)) and ("," not in v):
                continue
            extras.append(v)

    base = rec.get("address","").strip()
    tail = ", ".join(extras)
    rec["address"] = f"{base}, {tail}".strip(", ") if tail else base

    # 3) Гарантируем наличие целевых столбцов (если их не было в входном заголовке)
    for c in TARGET_COLS:
        rec.setdefault(c, "")
    return rec


def read_csv_chunks(path: str, chunksize: int, encoding: str, sep: str) -> Iterable[pd.DataFrame]:
    """
    Надёжное чтение «грязного» CSV:
      - точный разбор по csv.reader (quotechar='"', escapechar='\\')
      - любые НЕцелевые колонки склеиваются в address
      - количество колонок в строке может отличаться — не падаем
    """
    # Увеличим лимит размера поля для csv (по умолчанию 128 КБ),
    # чтобы не падать на длинных колонках в больших файлах.
    # Ставим максимально возможный лимит, понижая до допустимого значения
    try:
        max_limit = getattr(sys, "maxsize", 2_147_483_647)
        if max_limit < 10 * 1024 * 1024:
            max_limit = 10 * 1024 * 1024
        while True:
            try:
                csv.field_size_limit(int(max_limit))
                break
            except OverflowError:
                # уменьшаем и пробуем снова
                max_limit = int(max_limit / 10)
                if max_limit <= 1_000_000:  # нижняя планка ~1МБ
                    csv.field_size_limit(1_000_000)
                    break
    except Exception:
        # в худшем случае остаётся лимит по умолчанию
        pass

    with open(path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=sep, quotechar='"', escapechar="\\")
        try:
            header = next(reader)
        except StopIteration:
            return
        header = _normalize_header(header)

        buf: List[Dict[str,str]] = []
        for row in reader:
            # выравниваем длину
            if len(row) < len(header):
                row += [""] * (len(header) - len(row))
            rec = _row_to_record(row, header)
            buf.append(rec)
            if len(buf) >= chunksize:
                # сохраняем все входные колонки + TARGET_COLS
                cols = list(dict.fromkeys(header + TARGET_COLS))
                yield pd.DataFrame.from_records(buf, columns=cols)
                buf.clear()
        if buf:
            cols = list(dict.fromkeys(header + TARGET_COLS))
            yield pd.DataFrame.from_records(buf, columns=cols)

def filter_target_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Консервативно: не выкидываем лишние колонки, только гарантируем наличие TARGET_COLS.
    Возвращаем DataFrame как есть (с сохранением всех входных столбцов).
    """
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = ""
    return df

def write_chunk(df: pd.DataFrame, out_path: str, sep: str, quote_all: bool, header: bool, columns: Optional[List[str]] = None):
    quoting = csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL
    df.to_csv(
        out_path,
        mode="w" if header else "a",
        index=False,
        header=header,
        sep=sep,
        quoting=quoting,
        encoding="utf-8",
        columns=(columns or OUTPUT_COLS),
    )
