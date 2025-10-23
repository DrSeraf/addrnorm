"""
Извлечение частей адреса из address-строки.
Основной путь — libpostal REST API:
  - /parser  с JSON {"query":"..."}  → [{label,value},...]
Fallback:
  - /parse   с JSON {"address":"..."} (совместимость со старыми образами)
  - локальная эвристика (regex), если REST недоступен
"""
from __future__ import annotations
import re
from typing import Dict, Optional
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry  # type: ignore
except Exception:  # urllib3 may not expose Retry the same way; fallback no-retry
    Retry = None  # type: ignore

_RE_POSTCODE = re.compile(r"\b[A-Z0-9][A-Z0-9 \-]{2,9}\b", re.IGNORECASE)
_RE_HOUSENUM = re.compile(r"\b(\d+[A-Za-z\/\-]?)\b")
_RE_COMMON_SEPS = re.compile(r"[,;/]+")

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=(Retry(total=2, backoff_factor=0.1, status_forcelist=(429, 500, 502, 503, 504)) if Retry else 0))
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _session = s
    return _session


def _parse_via_libpostal(url: str, address: str) -> Dict[str, str]:
    # 1) предпочитаем /parser с {"query": "..."}
    try:
        s = _get_session()
        r = s.post(url, json={"query": address}, timeout=2)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return {it.get("label"): it.get("value") for it in data if it.get("label") and it.get("value")}
            if isinstance(data, dict):
                return data
        # если 404/400 — попробуем /parse с {"address": "..."}
        if r.status_code in (400, 404):
            alt = url.replace("/parser", "/parse")
            r2 = s.post(alt, json={"address": address}, timeout=2)
            if r2.status_code == 200:
                data = r2.json()
                if isinstance(data, list):
                    return {it.get("label"): it.get("value") for it in data if it.get("label") and it.get("value")}
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}

def _fallback_regex(address: str) -> Dict[str, str]:
    res: Dict[str, str] = {}
    parts = [p.strip() for p in _RE_COMMON_SEPS.split(address) if p.strip()]
    # находим индекс
    for p in reversed(parts):
        m = _RE_POSTCODE.search(p)
        if m:
            res["postcode"] = m.group(0).strip()
            break
    # дорожка/город/страна (очень грубо)
    if parts:
        res["road"] = parts[0]
        if len(parts) >= 2:
            res["city"] = parts[-2]
        if len(parts) >= 3:
            res["country"] = parts[-1]
    m = _RE_HOUSENUM.search(address)
    if m:
        res["house_number"] = m.group(1)
    return res

def extract_from_address(address: str, mode: str, libpostal_url: Optional[str] = None) -> Dict[str, str]:
    if not address or not isinstance(address, str):
        return {}
    address = address.strip()
    if not address:
        return {}

    parsed: Dict[str, str] = {}
    if libpostal_url:
        parsed = _parse_via_libpostal(libpostal_url, address)
    if not parsed:
        parsed = _fallback_regex(address)

    out: Dict[str, str] = {}
    # нормализуем ярлыки libpostal к нашим ключам
    label_map = {
        "house_number": "house_number",
        "road": "road",
        "unit": "unit",
        "suburb": "suburb",
        "city": "city",
        "state": "state",
        "postcode": "zip",     # <-- важно
        "country": "country",
    }
    for src, dst in label_map.items():
        v = parsed.get(src) or parsed.get(src.replace("_","-"))
        if v:
            out[dst] = str(v).strip()
    return out
