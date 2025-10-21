#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI для addrnorm — парсер аргументов и запуск пайплайна
"""

from __future__ import annotations
import argparse
from .pipeline import run_job

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Address normalization pipeline")
    p.add_argument("input", help="Входной CSV-файл")
    p.add_argument("-o", "--output", required=True, help="Путь для выходного CSV")
    p.add_argument("--report", required=True, help="Путь для JSON отчёта")
    p.add_argument("--samples-dir", required=True, help="Папка для samples.txt")

    # режимы
    p.add_argument("--mode", default="fill-missing-only",
                   choices=["fill-missing-only", "extract-all-to-fill"])
    p.add_argument("--street-from-address", default=False, action="store_true",
                   help="Брать street только из address, не трогая другие поля")

    # справочники и валидация
    p.add_argument("--profiles", default="base", help="Список профилей через запятую (base,TH,...")
    p.add_argument("--validate", default="loose", choices=["loose", "strict", "off"])
    p.add_argument("--fuzzy-threshold", type=int, default=90)
    p.add_argument("--prefer", default="existing", choices=["existing", "address", "parsed"])

    # libpostal
    p.add_argument("--libpostal-url", default=None, help="URL REST сервиса libpostal")
    p.add_argument("--concurrency", type=int, default=50)

    # io
    p.add_argument("--chunksize", type=int, default=200000)
    p.add_argument("--encoding", default="utf-8")
    p.add_argument("--sep", default=",")
    p.add_argument("--quote-all", action="store_true")

    # логи
    p.add_argument("--progress-every", type=int, default=20000)
    p.add_argument("--quiet", action="store_true")

    return p


def main():
    args = build_parser().parse_args()
    profiles = [p.strip() for p in args.profiles.split(",") if p.strip()]

    run_job(
        input_path=args.input,
        output_path=args.output,
        report_path=args.report,
        samples_dir=args.samples_dir,
        chunksize=args.chunksize,
        profiles=profiles,
        mode=args.mode,
        street_from_address=args.street_from_address,
        validate=args.validate,
        fuzzy_threshold=args.fuzzy_threshold,
        libpostal_url=args.libpostal_url,
        concurrency=args.concurrency,
        prefer=args.prefer,
        encoding=args.encoding,
        sep=args.sep,
        quote_all=args.quote_all,
        progress_every=args.progress_every,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()
