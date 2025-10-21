"""
Создание и обновление отчёта JSON.
"""
from __future__ import annotations
import json
from datetime import datetime, timezone

FIELDS = ["street","district","locality","region","country","zip"]
CATS   = ["clean","normalize","extracted","removed","fuzzy_fixed","unchanged"]

def _empty_counter():
    return {k: 0 for k in ["total_changed","clean","normalize","extracted","removed","fuzzy_fixed","unchanged"]}

def init_report(ctx) -> dict:
    return {
        "input": {
            "file": ctx.__dict__.get("input_path",""),
            "rows_total": 0,
            "chunksize": ctx.chunksize,
            "encoding": ctx.encoding,
            "sep": ctx.sep
        },
        "run": {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "duration_sec": None,
            "mode": ctx.mode,
            "street_from_address": ctx.street_from_address,
            "profiles": ctx.profiles,
            "validate": ctx.validate,
            "fuzzy_threshold": ctx.fuzzy_threshold,
            "libpostal_url": ctx.libpostal_url,
            "concurrency": ctx.concurrency
        },
        "counters": {field: _empty_counter() for field in FIELDS},
        "conflicts": {
            "locality_vs_address": 0,
            "region_vs_address": 0,
            "country_vs_address": 0,
            "zip_format_fail": 0,
            "region_mismatch_locality": 0
        },
        "top_unrecognized": {
            "locality": [],
            "region": [],
            "country": []
        },
        "notes": []
    }

def update_report(report_dict: dict, field: str, change_type: str):
    if field not in FIELDS:
        return
    c = report_dict["counters"][field]
    if change_type in CATS:
        c[change_type] += 1
    if change_type != "unchanged":
        c["total_changed"] += 1

def finalize_report(report_dict: dict, ctx, output_path: str):
    report_dict["run"]["finished_at"] = datetime.now(timezone.utc).isoformat()
    # duration_sec можно заполнить в pipeline, если будем замерять время — сейчас опустим
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, ensure_ascii=False, indent=2)
