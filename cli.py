# -*- coding: utf-8 -*-
"""
addrnorm.cli
ОСНОВНОЙ CLI нормализации + автоматический post-clean из addrnorm.clean.

Новые флаги:
  --no-post-clean        отключить пост-очистку (по умолчанию включена)
  --rules PATH           путь к YAML/JSON правилам (иначе берём из $ADDRNORM_RULES, если есть)

Все прежние флаги сохраняются без изменений.
"""

from __future__ import annotations
import os
import sys
import argparse
import json
from typing import Optional, Dict, Any

import pandas as pd

# === твой существующий импорт пайплайна ===
# from addrnorm.pipeline import run_normalization   # пример: оставь как у тебя
# Допустим, у тебя есть функция, которая возвращает DataFrame
# Ниже я смоделирую вызов, заменив на "stub_run", чтобы файл был самодостаточным.
def stub_run(input_csv: str, **kwargs) -> pd.DataFrame:
    # ЭТО ЗАГЛУШКА на место твоего реального пайплайна!
    # У тебя здесь должен быть существующий код, который возвращает уже нормализованный DF.
    # Я оставляю как есть: читать и возвращать без изменений — только чтобы файл компилировался.
    return pd.read_csv(input_csv, dtype=str, keep_default_na=False)

# === post-clean ===
from .rules.clean import Cleaner, load_rules



def _parse_args(argv=None):
    p = argparse.ArgumentParser(prog="addrnorm", description="Address normalizer with post-clean step")
    # --- СТАРЫЕ ПАРАМЕТРЫ (оставь как у тебя) ---
    p.add_argument("input", help="Входной CSV")
    p.add_argument("-o", "--output", required=True, help="Выходной CSV")
    p.add_argument("--report", default=None, help="Путь отчёта JSON")
    p.add_argument("--samples-dir", default=None)
    p.add_argument("--libpostal-url", default=None)
    p.add_argument("--profiles", default="base")
    p.add_argument("--chunksize", type=int, default=10000)
    p.add_argument("--mode", default="fill-missing-only")
    p.add_argument("--validate", default="loose")
    p.add_argument("--fuzzy-threshold", type=int, default=90)
    # --- НОВЫЕ ПАРАМЕТРЫ ДЛЯ POST-CLEAN ---
    p.add_argument("--no-post-clean", action="store_true",
                   help="Отключить пост-очистку (по умолчанию включена)")
    p.add_argument("--rules", default=os.getenv("ADDRNORM_RULES"),
                   help="Путь к YAML/JSON с правилами (по умолчанию $ADDRNORM_RULES)")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    # === 1) твой существующий запуск нормализации ===
    # df = run_normalization( ... )   # <- используй свою функцию
    df = stub_run(
        args.input,
        report=args.report,
        profiles=args.profiles,
        chunksize=args.chunksize,
        mode=args.mode,
        validate=args.validate,
        fuzzy_threshold=args.fuzzy_threshold,
        libpostal_url=args.libpostal_url,
        samples_dir=args.samples_dir,
    )

    # === 2) POST-CLEAN — включён по умолчанию ===
    if not args.no_post_clean:
        rules: Dict[str, Any] = {}
        if args.rules:
            try:
                rules = _load_rules(args.rules)
            except Exception as e:
                print(f"[post-clean] Не удалось загрузить правила ({args.rules}): {e}", file=sys.stderr)
        cleaner = Cleaner(rules)
        df = cleaner.clean_df(df)

    # === 3) запись результата ===
    df.to_csv(args.output, index=False)

    # Отчёт формирует твой исходный код — здесь не трогаю.
    # Если надо, можно дописать маркер, что post-clean применился.
    if args.report:
        try:
            # не перезатираем твой отчёт; если он уже сформирован — можно слить метку
            rep = {}
            if os.path.exists(args.report):
                with open(args.report, "r", encoding="utf-8") as f:
                    try:
                        rep = json.load(f) or {}
                    except Exception:
                        rep = {}
            rep.setdefault("_post_clean", {})["enabled"] = not args.no_post_clean
            rep["_post_clean"]["rules"] = bool(args.rules)
            with open(args.report, "w", encoding="utf-8") as f:
                json.dump(rep, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[post-clean] Не удалось обновить report: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
