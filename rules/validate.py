"""
Офлайн-валидация: страна/город/регион/ZIP + мягкие автопочинки для locality/region.
- Страны валидируем через pycountry (в normalize мы уже нашли alpha2).
- Города валидируем по GeoNames (geonamescache) внутри страны.
- Регионы: сначала профили (profiles.region_lists_by_country / aliases), иначе — только мягкая проверка.
- ZIP: по regex-шаблонам из профилей (profiles.zip_patterns[alpha2]).

Режимы:
  ctx.validate in {"off","loose","strict"}
    off   — ничего не валидируем
    loose — fuzzy-починки (>= ctx.fuzzy_threshold) разрешены и применяются
    strict— только точные совпадения, fuzzy не применяем (но флаги пишем)

Возвращает: (row, flags: list[str])
"""

from __future__ import annotations
import re
from typing import Dict, List, Optional, Tuple
from unidecode import unidecode
from geonamescache import GeonamesCache

# --------- ленивые кэши GeoNames ---------
_gc: Optional[GeonamesCache] = None
_cities_by_country: Optional[Dict[str, Dict[str, dict]]] = None  # alpha2 -> {normalized_name: city_info}
# Быстрый префиксный индекс по ключам городов: alpha2 -> {prefix2 -> [keys...]}
_city_prefix_index: Optional[Dict[str, Dict[str, list[str]]]] = None
# Кэш результатов валидации локалити: (alpha2, key) -> фиксированное имя (или исходное)
_locality_validate_cache: Dict[tuple[str, str], str] = {}

def _init_caches():
    global _gc, _cities_by_country, _city_prefix_index
    if _gc is None:
        _gc = GeonamesCache()
    if _cities_by_country is None:
        _cities_by_country = {}
        _city_prefix_index = {}
        # Собираем карту: alpha2 -> { name_lower: info, ... } для городов
        for gid, info in _gc.get_cities().items():
            cc = info.get("countrycode")  # ISO alpha-2
            name = info.get("name", "")
            if not cc or not name:
                continue
            key = unidecode(name).strip().lower()
            if not key:
                continue
            _cities_by_country.setdefault(cc, {})
            # Берём наиболее населённую запись для этого ключа (если дубликаты)
            prev = _cities_by_country[cc].get(key)
            if (prev is None) or (info.get("population", 0) > prev.get("population", 0)):
                _cities_by_country[cc][key] = info
            # также добавим альтернативные названия если доступны
            for alt in info.get("alternatenames", []) or []:
                k2 = unidecode(str(alt)).strip().lower()
                if not k2:
                    continue
                p2 = _cities_by_country[cc].get(k2)
                if (p2 is None) or (info.get("population", 0) > p2.get("population", 0)):
                    _cities_by_country[cc][k2] = info
        # Построим префиксный индекс (двухбуквенный)
        for cc, pool in _cities_by_country.items():
            pref_map: Dict[str, list[str]] = {}
            for k in pool.keys():
                if not k:
                    continue
                p2 = k[:2]
                pref_map.setdefault(p2, []).append(k)
            _city_prefix_index[cc] = pref_map

# --------- helpers ---------

def _norm_key(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return unidecode(str(s)).strip().lower() or None

def _fuzzy_candidates(name_key: str, pool: Dict[str, dict], top_k: int, alpha2: Optional[str] = None) -> List[Tuple[str, int]]:
    """
    Очень лёгкая эвристика без heavy rapidfuzz (чтобы не тащить зависимость сюда):
    считаем похожесть по префиксу/вхождению и грубой длине. Вернём (candidate, score 0..100).
    Это дёшево и достаточно для предварительной починки. (При желании позже заменим на rapidfuzz.)
    """
    if not name_key or not pool:
        return []
    # Сузим набор кандидатов по двум первым буквам, если есть индекс
    keys_iterable = None
    if alpha2 and _city_prefix_index is not None:
        pref = name_key[:2] if len(name_key) >= 2 else name_key[:1]
        keys_iterable = (_city_prefix_index.get(alpha2, {}).get(pref) or None)
    keys_iterable = keys_iterable or pool.keys()

    cands: List[Tuple[str,int]] = []
    for cand in keys_iterable:
        score = 0
        if cand == name_key:
            score = 100
        elif cand.startswith(name_key) or name_key.startswith(cand):
            score = 92
        elif name_key in cand or cand in name_key:
            score = 88
        else:
            # грубая близость по длине
            dl = abs(len(cand) - len(name_key))
            score = max(0, 85 - dl*3)
        if score >= 80:
            cands.append((cand, score))
    cands.sort(key=lambda x: x[1], reverse=True)
    return cands[:top_k]

# --------- валидация страны/города/региона/ZIP ---------

def _validate_zip(zip_code: Optional[str], alpha2: Optional[str], zip_patterns: dict, flags: List[str]) -> Optional[str]:
    if not zip_code or not alpha2:
        return zip_code
    pat = (zip_patterns or {}).get(alpha2)
    if not pat:
        return zip_code
    try:
        rgx = re.compile(pat, re.IGNORECASE)
        if not rgx.fullmatch(str(zip_code).strip()):
            flags.append("bad_zip_format")
    except re.error:
        # некорректный паттерн в профиле — пометим
        flags.append("zip_pattern_error")
    return zip_code

def _validate_locality(locality: Optional[str], alpha2: Optional[str], ctx, flags: List[str]) -> Optional[str]:
    if not locality:
        return None
    if ctx.validate == "off":
        return locality

    _init_caches()
    pool = _cities_by_country.get(alpha2, {}) if (alpha2 and _cities_by_country) else {}

    key = _norm_key(locality)
    if not key:
        return None

    # точное попадание
    if pool and key in pool:
        return locality  # норм

    # кэш на повторяющиеся проверки
    cache_key = (alpha2 or "", key)
    cached = _locality_validate_cache.get(cache_key)
    if cached is not None:
        # если кэшированный отличается — это была фиксация ранее
        if cached != locality:
            flags.append("locality_fuzzy_fixed")
        return cached

    # fuzzy в loose
    if ctx.validate == "loose" and pool:
        cands = _fuzzy_candidates(key, pool, getattr(ctx, "max_fuzzy_candidates", 3) or 3, alpha2)
        if cands:
            cand, score = cands[0]
            if score >= ctx.fuzzy_threshold:
                # применяем автопочинку
                fixed = pool[cand]["name"]
                if fixed != locality:
                    flags.append("locality_fuzzy_fixed")
                _locality_validate_cache[cache_key] = fixed
                return fixed

    # strict или не нашли
    flags.append("city_not_found")
    _locality_validate_cache[cache_key] = locality
    return locality

def _validate_region(region: Optional[str], alpha2: Optional[str], ctx, flags: List[str]) -> Optional[str]:
    if not region:
        return None
    if ctx.validate == "off":
        return region

    # Если есть whitelists в профилях — проверим по ним
    region_lists = ctx.profiles_data.get("region_lists_by_country", {})
    whitelist = region_lists.get(alpha2, []) if alpha2 else []
    if whitelist:
        key = _norm_key(region)
        wl_keys = { _norm_key(x) for x in whitelist if x }
        if key in wl_keys:
            return region
        # в loose попробуем мягкую починку к whitelist
        if ctx.validate == "loose":
            # дешёвый поиск «лучшего» совпадения
            best = None
            best_score = -1
            for cand in wl_keys:
                if not cand:
                    continue
                if cand == key:
                    best, best_score = cand, 100
                    break
                score = 92 if (cand.startswith(key) or key.startswith(cand)) else (88 if (key in cand or cand in key) else 0)
                if score > best_score:
                    best, best_score = cand, score
            if best and best_score >= ctx.fuzzy_threshold:
                # найдём оригинал по ключу
                for orig in whitelist:
                    if _norm_key(orig) == best:
                        if orig != region:
                            flags.append("region_fuzzy_fixed")
                        return orig
        # не нашли — отметим
        flags.append("region_not_in_whitelist")
        return region

    # если whitelist нет — просто пропускаем (валидация регионов по всему миру офлайн неполная)
    return region

# --------- публичный API ---------

def validate_fields(row: dict, ctx) -> tuple[dict, list[str]]:
    """
    Валидация с мягкими автопочинками.
    На входе row уже после normalize: {street, district, locality, region, country, zip}
    В ctx ожидается:
      - validate ("off"/"loose"/"strict")
      - fuzzy_threshold (int)
      - profiles_data (dict) с ключами: zip_patterns, region_lists_by_country (опц.)
      - alpha2 страны можно держать в ctx (если normalize его сохранил), но тут мы не знаем — берём из ctx.temp_alpha2, если есть.
        Поэтому pipeline должен передавать alpha2 через ctx.current_alpha2 либо добавлять в row (лучше — в ctx).
    """
    flags: List[str] = []

    # alpha2 нам нужен здесь; normalize._normalize_country возвращает alpha2,
    # поэтому pipeline должен положить его в ctx на время проверки текущего ряда
    alpha2 = getattr(ctx, "current_alpha2", None)

    # ZIP по паттерну
    row["zip"] = _validate_zip(row.get("zip"), alpha2, ctx.profiles_data.get("zip_patterns", {}), flags)

    # locality по GeoNames
    row["locality"] = _validate_locality(row.get("locality"), alpha2, ctx, flags)

    # region по whitelist (если задан)
    row["region"] = _validate_region(row.get("region"), alpha2, ctx, flags)

    # В strict-режиме при плохом формате ZIP — очищаем ZIP
    if ctx.validate == "strict" and ("bad_zip_format" in flags):
        row["zip"] = None

    return row, flags
