"""
Базовая очистка текстовых значений для всех полей.
"""

from __future__ import annotations
import re
import unicodedata

# регулярки для нормализации
_WS_RE = re.compile(r"\s+")
_PUNCT_EDGE_RE = re.compile(r"^[\s,;/\-_.]+|[\s,;/\-_.]+$")

def clean_value(value: str | None) -> str | None:
    """
    Базовая чистка строк:
      - None/null/NaN → None
      - Unicode NFKC нормализация
      - trim + схлопывание пробелов
      - удаление лишних знаков препинания по краям
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None

    # Unicode нормализация (приведение к единому виду)
    s = unicodedata.normalize("NFKC", s)

    # заменяем множественные пробелы на один
    s = _WS_RE.sub(" ", s)

    # убираем запятые, точки и прочее по краям
    s = _PUNCT_EDGE_RE.sub("", s)

    # повторно трим
    s = s.strip()

    return s or None
