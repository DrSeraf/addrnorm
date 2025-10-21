"""
Нормализация значений: country / region / locality / district / street / zip.
- Страны: pycountry + алиасы/обрезка хвостов из профилей
- Регионы/города/районы: алиасы (глобальные и по стране), мягкий title-case для латиницы
- Street: мягкая чистка + нормализация суффиксов (по профилю), без «склейки» из частей
- ZIP: вычленяем финальный индекс из строк вроде "BANGKOK 10220" → "10220"
"""

from __future__ import annotations
import re
from typing import Optional, Tuple, Dict

import pycountry
from unidecode import unidecode

# ---------- regex & helpers ----------

_LATIN_RE = re.compile(r"[A-Za-z]")
_WS_RE    = re.compile(r"\s+")
# было: _SEP_RE = re.compile(r"\s*[,;/]\s*")  # ломало "77/1"
_SEP_RE   = re.compile(r"\s*[,;]\s*")         # убрали "/"
_EDGE_RE  = re.compile(r"^[\s,;/\-_.]+|[\s,;/\-_.]+$")

# ZIP: сначала пробуем чисто-цифровые 4–6, иначе берём последний похожий токен
_ONLY_DIGITS_ZIP = re.compile(r"\b\d{4,6}\b")
# Требуем хотя бы одну цифру в ZIP-токене, чтобы отсеять слова вроде "ADDRESS"
_GENERIC_ZIP_TOK = re.compile(r"(?=.*\d)[A-Z0-9][A-Z0-9\- ]{2,9}$", re.IGNORECASE)

# --- street cleaning helpers (optional via profiles flags) ---
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
_PHONE_RE = re.compile(r"\+?\d[\d \-()]{6,}\d")
_TAG_RE   = re.compile(r"#\w+")
_UNIT_RE  = re.compile(r"\b(?:unit|apt|apartment|room|rm|bldg|building|flr|floor|tower|blk|block|lvl|level|dept|suite|ste)\b\.?", re.I)
_NON_ADDRESSY = re.compile(r"^[A-Za-z]{2,}$")

def _is_empty(s: Optional[str]) -> bool:
    return not s or str(s).strip() == "" or str(s).strip().lower() in {"nan", "null", "none"}

def _smart_title(s: str) -> str:
    if not _LATIN_RE.search(s):
        return s.strip()
    parts = [p.capitalize() if len(p) > 2 else p.upper() for p in s.split()]
    return " ".join(parts).strip()

def _cleanup_basic(s: str) -> str:
    s = s.strip()
    s = _EDGE_RE.sub("", s)
    s = _WS_RE.sub(" ", s)
    return s

def _apply_alias(value: str, aliases: Dict[str, str]) -> str:
    key = value.strip().lower()
    return aliases.get(key, value)

def _strip_stop_suffixes(country: str, suffixes: list[str]) -> str:
    s = country.strip().lower()
    for suf in suffixes or []:
        suf = suf.strip().lower()
        if suf and s.endswith(suf):
            s = s[: -len(suf)].strip(", ").strip()
    return s

def _pycountry_match_country(name: str):
    try:
        return pycountry.countries.lookup(name)
    except Exception:
        pass
    # сравним по unidecode/lower для common/official/name
    name_key = unidecode(name).lower()
    for c in pycountry.countries:
        for field in ("common_name", "official_name", "name"):
            v = getattr(c, field, None)
            if v and unidecode(v).lower() == name_key:
                return c
    return None

# ---------- country ----------

def _normalize_country(country: Optional[str], ctx) -> Tuple[Optional[str], Optional[str]]:
    if _is_empty(country):
        return None, None
    raw = _cleanup_basic(str(country))

    # обрежем «хвосты»
    trimmed = _strip_stop_suffixes(raw, ctx.profiles_data.get("stop_country_suffixes", []))
    # алиасы
    aliased = _apply_alias(trimmed, ctx.profiles_data.get("country_aliases", {}) or {})

    c = _pycountry_match_country(aliased)
    if c:
        name = getattr(c, "common_name", getattr(c, "name", None))
        return name, getattr(c, "alpha_2", None)

    # если не нашли — вернём аккуратный вид
    return _smart_title(aliased), None

# ---------- region / locality / district ----------

def _normalize_region(region: Optional[str], alpha2: Optional[str], ctx) -> Optional[str]:
    if _is_empty(region):
        return None
    s = _cleanup_basic(str(region))
    s = _apply_alias(s, ctx.profiles_data.get("region_aliases", {}) or {})
    if alpha2:
        s = _apply_alias(s, (ctx.profiles_data.get("region_aliases_by_country", {}).get(alpha2) or {}))
    return _smart_title(s) or None

def _normalize_locality(locality: Optional[str], alpha2: Optional[str], ctx) -> Optional[str]:
    if _is_empty(locality):
        return None
    s = _cleanup_basic(str(locality))
    s = _apply_alias(s, ctx.profiles_data.get("locality_aliases", {}) or {})
    if alpha2:
        s = _apply_alias(s, (ctx.profiles_data.get("locality_aliases_by_country", {}).get(alpha2) or {}))
    return _smart_title(s) or None

def _normalize_district(district: Optional[str], alpha2: Optional[str], ctx) -> Optional[str]:
    if _is_empty(district):
        return None
    s = _cleanup_basic(str(district))
    s = _apply_alias(s, ctx.profiles_data.get("district_aliases", {}) or {})
    if alpha2:
        s = _apply_alias(s, (ctx.profiles_data.get("district_aliases_by_country", {}).get(alpha2) or {}))
    return _smart_title(s) or None

# ---------- street / zip ----------

def _normalize_street(street: Optional[str], ctx) -> Optional[str]:
    if _is_empty(street):
        return None
    s = str(street).strip()
    # Быстрый отсев: если строка состоит только из цифр и знаков → пусто
    if s and all((ch.isdigit() or not ch.isalnum()) for ch in s):
        return None

    # опциональная зачистка по флагам из профиля/правил (консервативно: по умолчанию выключено)
    flags = ctx.profiles_data or {}
    if flags.get("drop_emails_phones_from_street", False):
        s = _EMAIL_RE.sub("", s)
        s = _PHONE_RE.sub("", s)
        s = _TAG_RE.sub("", s)
    if flags.get("drop_unit_attrs", False):
        s = _UNIT_RE.sub("", s)
        s = re.sub(r"\b(?:FL|FLOOR|LVL|LEVEL|RM|ROOM|APT|SUITE|STE|UNIT|BLDG|BLK|BLOCK)\b\.?\s*\d+[A-Z\-\/]*", "", s, flags=re.I)
    if flags.get("drop_non_addressy_single_tokens", False):
        tokens = [t for t in re.split(r"[,\s]+", s) if t]
        if len(tokens) == 1 and _NON_ADDRESSY.match(tokens[0]) and not re.search(r"\d", tokens[0]):
            return None
    # нормализуем разделители
    s = _SEP_RE.sub(", ", s)
    s = _EDGE_RE.sub("", s)
    s = _WS_RE.sub(" ", s)

    # Если после базовой нормализации в строке нет «слов» длиной ≥ 3 букв,
    # а только числа/знаки/1–2 буквы с номерами (напр. "8/25", "M.3", "A-12") — очищаем
    tokens = [t for t in re.split(r"[\s,;./\\-]+", s) if t]
    has_word3 = any(sum(1 for ch in t if str(ch).isalpha()) >= 3 for t in tokens)
    has_any_letter = any(str(ch).isalpha() for ch in s)
    if (not has_word3) and (has_any_letter or tokens):
        # нет «полноценных» слов — считаем это номером/меткой без названия улицы
        return None

    # приводим общеупотребимые суффиксы (только латиница) из профиля
    suffix_map = ctx.profiles_data.get("street_suffix_normalization", {}) or {}
    if suffix_map and _LATIN_RE.search(s):
        tokens = s.split()
        for i, t in enumerate(tokens):
            low = t.lower().strip(".")
            if low in suffix_map:
                tokens[i] = suffix_map[low]
        s = " ".join(tokens)

    return _smart_title(s) or None

def _normalize_zip(zip_code: Optional[str], alpha2: Optional[str], ctx) -> Optional[str]:
    if _is_empty(zip_code):
        return None
    z = str(zip_code).strip().upper()
    # если в ZIP попали слова (BANGKOK 10220) — вытащим индекс; если нет подходящего токена — обнулим
    m = _ONLY_DIGITS_ZIP.search(z) or _GENERIC_ZIP_TOK.search(z)
    if not m:
        return None
    z = m.group(0).strip()
    # финальная чистка пробелов
    z = _WS_RE.sub(" ", z).strip()
    return z or None

# ---------- public API ----------

def normalize_fields(row: dict, ctx) -> dict:
    """
    Вход:  row = {street, district, locality, region, country, zip}
    Выход: те же ключи, нормализованные.
    """
    street   = row.get("street")
    district = row.get("district")
    locality = row.get("locality")
    region   = row.get("region")
    country  = row.get("country")
    zip_code = row.get("zip")

    # страна первой — получим alpha2 для локальных алиасов
    country_norm, alpha2 = _normalize_country(country, ctx)
    region_norm   = _normalize_region(region, alpha2, ctx)
    locality_norm = _normalize_locality(locality, alpha2, ctx)
    district_norm = _normalize_district(district, alpha2, ctx)
    street_norm   = _normalize_street(street, ctx)
    zip_norm      = _normalize_zip(zip_code, alpha2, ctx)

    # echo-fix: locality == region → очищаем region (по флагу; консервативно: по умолчанию выключено)
    if (ctx.profiles_data or {}).get("fix_echo_locality_region", False):
        lk = unidecode(locality_norm or "").strip().lower()
        rk = unidecode(region_norm or "").strip().lower()
        if lk and rk and lk == rk:
            region_norm = None

    return {
        "street":   street_norm,
        "district": district_norm,
        "locality": locality_norm,
        "region":   region_norm,
        "country":  country_norm,
        "zip":      zip_norm,
    }
