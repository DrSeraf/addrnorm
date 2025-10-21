# addrnorm/rules/clean.py
from __future__ import annotations

import re
from typing import Any

# --- basic patterns
WS_RE              = re.compile(r"\s+")
ZERO_WIDTH_RE      = re.compile(r"[\u200B-\u200D\uFEFF]")
UNICODE_DASH_RE    = re.compile(r"[\u2012\u2013\u2014\u2015\u2212]")
UNICODE_QUOTE_RE   = re.compile(r"[“”„‟«»]")
UNICODE_APOST_RE   = re.compile(r"[’‘‚‛]")
MULTI_COMMA_RE     = re.compile(r"(,\s*){2,}")
TRAILING_SEP_RE    = re.compile(r"^[\s,;:/\-\._|]+|[\s,;:/\-\._|]+$")

# contacts (to be removed from any field)
EMAIL_RE           = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
PHONE_RE           = re.compile(r"(?:\+?\d[\d \-()]{6,}\d)|(?:\b\d{8,}\b)")

# tokens that mean "no data"
NULL_TOKEN_RE      = re.compile(r"^(?:nan|null|none|n/a|na|—|-|empty|\.)$", re.I)

# sometimes people put city + zip into one string; cleaning won't split, but keep tidy
CITY_ZIP_GLUE_RE   = re.compile(r"\s{2,}")

def _normalize_unicode(s: str) -> str:
    # normalize punctuation variants
    s = ZERO_WIDTH_RE.sub("", s)
    s = UNICODE_DASH_RE.sub("-", s)
    s = UNICODE_QUOTE_RE.sub('"', s)
    s = UNICODE_APOST_RE.sub("'", s)
    return s

def _strip_contacts(s: str) -> str:
    # remove obvious emails/phones completely
    s = EMAIL_RE.sub("", s)
    s = PHONE_RE.sub("", s)
    return s

def _tidy_separators(s: str) -> str:
    # unify commas/semicolons to ", "
    s = re.sub(r"\s*[,;]\s*", ", ", s)
    # collapse repeated commas
    s = MULTI_COMMA_RE.sub(", ", s)
    # collapse spaces around slashes, but KEEP the slash (important for house numbers 77/1)
    s = re.sub(r"\s*/\s*", "/", s)
    # collapse spaces around hyphens in simple numeric/name ranges (do not remove hyphen)
    s = re.sub(r"\s*-\s*", "-", s)
    # normalize whitespace
    s = WS_RE.sub(" ", s).strip()
    # remove leading/trailing separators
    s = TRAILING_SEP_RE.sub("", s).strip()
    # compact accidental double spaces left by removals
    s = CITY_ZIP_GLUE_RE.sub(" ", s)
    return s

def _lower_noise_upper_words(s: str) -> str:
    # this cleaner should NOT change case meaningfully; normalization handles casing.
    # but we can trim obvious surrounding quotes/brackets
    s = s.strip(' "\'()[]{}')
    return s

def clean_value(v: Any) -> str:
    """
    Lightweight, safe cleaner:
    - trims whitespace & zero-width chars
    - normalizes unicode dashes/quotes
    - removes emails/phones anywhere in the field
    - unifies separators: ", " / "/" / "-"
    - keeps house numbers like "77/1" intact
    - strips leading/trailing punctuation
    - returns "" for null-like tokens
    """
    if v is None:
        return ""

    s = str(v)
    if not s:
        return ""

    s = _normalize_unicode(s)
    s = s.strip()

    # null-like values
    if NULL_TOKEN_RE.match(s):
        return ""

    # remove contacts early (so leftover commas will be tidied next)
    s = _strip_contacts(s)

    # tidy separators and spaces
    s = _tidy_separators(s)

    # final small trims
    s = _lower_noise_upper_words(s)

    # after all cleaning, consider again if becomes empty or null-like
    if not s or NULL_TOKEN_RE.match(s):
        return ""

    return s
