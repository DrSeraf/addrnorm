"""
addrnorm.cli — тонкий CLI-обёртка над пайплайном.

Запуск поддержан как модулем (`python -m addrnorm.cli`), так и прямым
вызовом файла (`python cli.py`). Во втором случае импорт пайплайна
происходит через динамический пакетный импорт (добавляем родительскую
директорию в sys.path и импортируем `<pkg>.pipeline`).
"""

from __future__ import annotations
import os, sys, importlib
import argparse
from typing import List

# Надёжный импорт run_job как пакетного модуля, даже при `python cli.py`
def _import_run_job():
    try:
        # Когда запускаем как пакет: addrnorm.cli → сработает относительный импорт
        from .pipeline import run_job as _rj  # type: ignore
        return _rj
    except Exception:
        # Когда запускаем как скрипт: добавим родитель в sys.path и импортнём по имени пакета
        base_dir = os.path.dirname(os.path.abspath(__file__))
        pkg_name = os.path.basename(base_dir)
        parent = os.path.dirname(base_dir)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        mod = importlib.import_module(f"{pkg_name}.pipeline")
        return getattr(mod, "run_job")

run_job = _import_run_job()


def _split_profiles(s: str | None) -> List[str]:
    if not s:
        return ["base"]
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p] or ["base"]


def _parse_args(argv=None):
    p = argparse.ArgumentParser(prog="addrnorm", description="Address normalization CLI")
    p.add_argument("input", help="Путь к входному CSV")
    p.add_argument("-o", "--output", required=True, help="Путь к выходному CSV")

    # Общие параметры чтения/записи
    p.add_argument("--encoding", default="utf-8", help="Кодировка входного CSV (по умолчанию utf-8)")
    p.add_argument("--sep", default=",", help="Разделитель CSV (по умолчанию ,)")
    p.add_argument("--quote-all", action="store_true", help="Кавычить все поля в выходном CSV")

    # Управление пайплайном
    p.add_argument("--profiles", default="base", help="Список профилей через запятую (например: base,TH)")
    p.add_argument("--rules", default=None, help="Путь к rules.yaml (ZIP-паттерны, синонимы)")
    p.add_argument("--chunksize", type=int, default=10000, help="Размер чанка чтения CSV")
    p.add_argument("--mode", choices=["fill-missing-only", "extract-all-to-fill"], default="fill-missing-only",
                   help="Политика заполнения из address")
    p.add_argument("--street-from-address", action="store_true",
                   help="Извлекать только street из address (road + house_number) при наличии")
    p.add_argument("--libpostal-url", default=None, help="URL Libpostal REST, например http://localhost:8080/parser")
    p.add_argument("--validate", choices=["off", "loose", "strict"], default="loose",
                   help="Режим офлайн-валидации")
    p.add_argument("--fuzzy-threshold", type=int, default=90, help="Порог для мягких починок")
    p.add_argument("--concurrency", type=int, default=1, help="Зарезервировано под параллельность (сейчас 1)")
    p.add_argument("--quiet", action="store_true", help="Тише: не печатать прогресс")

    # Выходные артефакты
    p.add_argument("--report", default=None, help="Путь к report.json (если не задан — рядом с output)")
    p.add_argument("--samples-dir", default=None, help="Директория для samples_*.txt (если не задана — рядом с output)")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)

    # По умолчанию — класть артефакты рядом с output
    out_dir = os.path.dirname(args.output) or "."
    report_path = args.report or os.path.join(out_dir, "report.json")
    samples_dir = args.samples_dir or out_dir

    # Автовыбор rules.yaml по умолчанию, если не указан и файл лежит рядом с пакетом/в CWD
    rules_path = args.rules
    if not rules_path:
        for cand in ("rules.yaml", os.path.join(out_dir, "rules.yaml")):
            if os.path.exists(cand):
                rules_path = cand
                break

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(samples_dir, exist_ok=True)

    run_job(
        input_path=args.input,
        output_path=args.output,
        report_path=report_path,
        samples_dir=samples_dir,
        rules_path=rules_path,
        # чтение/запись
        encoding=args.encoding,
        sep=args.sep,
        quote_all=args.quote_all,
        # режимы
        profiles=_split_profiles(args.profiles),
        chunksize=args.chunksize,
        mode=args.mode,
        street_from_address=args.street_from_address,
        libpostal_url=args.libpostal_url,
        validate=args.validate,
        fuzzy_threshold=args.fuzzy_threshold,
        concurrency=args.concurrency,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()
