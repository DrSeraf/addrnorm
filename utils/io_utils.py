# addrnorm/utils/io_utils.py
from __future__ import annotations
import csv
from typing import Iterable, List, Dict
import pandas as pd

TARGET_COLS = ["street","district","locality","region","country","zip","address"]
OUTPUT_COLS = ["street","district","locality","region","country","zip"]

def _normalize_header(raw: List[str]) -> List[str]:
    # приводим имена к ожидаемым; прочее оставляем как есть
    return [h.strip() for h in raw]

def _row_to_record(row: List[str], header: List[str]) -> Dict[str,str]:
    rec = {k: "" for k in TARGET_COLS}
    n = min(len(row), len(header))
    # сначала забираем целевые поля по именам
    for i in range(n):
        col = header[i]
        val = row[i].strip()
        if col in TARGET_COLS:
            rec[col] = val
    # теперь соберём «лишние» в address (даже если колонки address нет в исходнике)
    extras: List[str] = []
    for i in range(n):
        col = header[i]
        if col not in {"street","district","locality","region","country","zip","address"}:
            v = row[i].strip()
            if v and v.lower() not in {"nan","null","none"}:
                extras.append(v)
    # если в исходнике есть address — добавим к нему хвост; если нет — просто создадим
    base = rec.get("address","").strip()
    tail = ", ".join(extras)
    if base and tail:
        rec["address"] = f"{base}, {tail}"
    elif tail:
        rec["address"] = tail
    else:
        rec["address"] = base
    return rec

def read_csv_chunks(path: str, chunksize: int, encoding: str, sep: str) -> Iterable[pd.DataFrame]:
    """
    Надёжное чтение «грязного» CSV:
      - точный разбор по csv.reader (quotechar='"', escapechar='\\')
      - любые НЕцелевые колонки склеиваются в address
      - количество колонок в строке может отличаться — не падаем
    """
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
                yield pd.DataFrame.from_records(buf, columns=TARGET_COLS)
                buf.clear()
        if buf:
            yield pd.DataFrame.from_records(buf, columns=TARGET_COLS)

def filter_target_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Уже приходят только целевые + address в нужном порядке
    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = ""
    return df[TARGET_COLS]

def write_chunk(df: pd.DataFrame, out_path: str, sep: str, quote_all: bool, header: bool):
    quoting = csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL
    df.to_csv(
        out_path,
        mode="w" if header else "a",
        index=False,
        header=header,
        sep=sep,
        quoting=quoting,
        encoding="utf-8",
        columns=OUTPUT_COLS,
    )
