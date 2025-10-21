"""
Логирование прогресса и сбор примеров для samples.txt
"""
from __future__ import annotations
import sys
from collections import defaultdict
from typing import Dict, List

COLUMNS = ["street","district","locality","region","country","zip"]
CATEGORIES = ["clean","normalize","extracted","removed","fuzzy_fixed"]
PER_COLUMN_LIMIT = 20

def log_progress(total_rows: int, processed: int, speed: float, eta: str, quiet: bool=False):
    if quiet:
        return
    msg = f"[PROGRESS] {processed:,}/{total_rows:,} rows | speed≈{int(speed):,}/s | ETA≈{eta}"
    print(msg, file=sys.stderr)

def _ensure_struct(samples_dict: dict):
    if not samples_dict:
        for col in COLUMNS:
            samples_dict[col] = {cat: [] for cat in CATEGORIES}

def collect_sample(column: str, change_type: str, before: str, after: str, row_id: int, ctx):
    """
    Стратифицированное наполнение:
    - цель: 20 примеров на колонку
    - равномерно по категориям (≈4 на категорию). Если категорий мало — перераспределим при flush.
    """
    if column not in COLUMNS or change_type not in CATEGORIES:
        return
    _ensure_struct(ctx.samples)
    bucket = ctx.samples[column][change_type]
    # лимит на категорию — динамический (чуть больше 4, чтобы потом отобрать)
    if len(bucket) < 6:  # небольшой запас, лишнее отрежем при flush
        bucket.append({
            "row": row_id,
            "before": str(before),
            "after": str(after),
            "type": change_type,
            "rule": "",  # можем заполнять в будущем
        })

def flush_samples(samples_dir: str, input_basename: str, samples_dict: dict):
    if not samples_dict:
        return
    path = f"{samples_dir.rstrip('/').rstrip('\\\\')}/{input_basename}.samples.txt"
    lines: List[str] = []
    for col in COLUMNS:
        lines.append(f"===== COLUMN: {col} =====")
        # ровно 20 примеров, распределяем равномерно по CATEGORIES
        target_per_cat = max(1, PER_COLUMN_LIMIT // len(CATEGORIES))  # 4
        picked: List[dict] = []
        # 1) первичный набор равномерно
        for cat in CATEGORIES:
            picked.extend(samples_dict.get(col, {}).get(cat, [])[:target_per_cat])
        # 2) если меньше 20 — добираем оставшимся
        if len(picked) < PER_COLUMN_LIMIT:
            for cat in CATEGORIES:
                pool = samples_dict.get(col, {}).get(cat, [])
                take = min(len(pool), PER_COLUMN_LIMIT - len(picked))
                if take > 0:
                    # добавляем те, что ещё не включили
                    already = set((x["row"], x["before"], x["after"], x["type"]) for x in picked)
                    for item in pool:
                        key = (item["row"], item["before"], item["after"], item["type"])
                        if key in already:
                            continue
                        picked.append(item)
                        if len(picked) >= PER_COLUMN_LIMIT:
                            break
                if len(picked) >= PER_COLUMN_LIMIT:
                    break
        # 3) вывод
        for it in picked:
            lines.append(f"[TYPE={it['type']}] [ROW={it['row']}]")
            lines.append(f"BEFORE: {repr(it['before'])}")
            lines.append(f"AFTER : {repr(it['after'])}")
            rule = it.get("rule","")
            if rule:
                lines.append(f"RULE  : {rule}")
            lines.append("---")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
