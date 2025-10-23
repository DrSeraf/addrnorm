"""
Streamlit UI для AddrNorm: загрузка CSV и правил, настройка параметров, запуск пайплайна,
предпросмотр и выгрузка результатов.
"""
from __future__ import annotations

import os
import io
import sys
import json
import time
import shutil
import zipfile
import tempfile
import importlib
from typing import List, Optional

import pandas as pd
import streamlit as st


def _import_run_job():
    """Надёжный импорт addrnorm.pipeline.run_job, даже при запуске app.py напрямую."""
    try:
        from .pipeline import run_job as _rj  # type: ignore
        return _rj
    except Exception:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        pkg_name = os.path.basename(base_dir)
        parent = os.path.dirname(base_dir)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        mod = importlib.import_module(f"{pkg_name}.pipeline")
        return getattr(mod, "run_job")


run_job = _import_run_job()
def _import_loggingx():
    try:
        from .utils import loggingx as _lx  # type: ignore
        return _lx
    except Exception:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        pkg_name = os.path.basename(base_dir)
        parent = os.path.dirname(base_dir)
        if parent not in sys.path:
            sys.path.insert(0, parent)
        return importlib.import_module(f"{pkg_name}.utils.loggingx")

loggingx = _import_loggingx()


def _split_profiles(s: str | None) -> List[str]:
    if not s:
        return ["base"]
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p] or ["base"]


st.set_page_config(page_title="AddrNorm UI", layout="wide")
st.title("AddrNorm — Нормализация адресов")
st.caption("Загрузите CSV, настройте параметры, запустите, скачайте результаты")

with st.sidebar:
    st.header("Параметры")
    enc = st.selectbox(
        "Кодировка входного CSV",
        ["utf-8", "utf-16", "cp1251", "latin1"],
        index=0,
        help="Как читать входной CSV. Поменяйте, если видите 'кракозябры' в данных."
    )
    sep_label = st.selectbox(
        "Разделитель",
        [",", ";", "\\t", "|"],
        index=0,
        help="Символ, разделяющий столбцы в CSV. \\t — это таб (TSV)."
    )
    sep = "\t" if sep_label == "\\t" else sep_label
    quote_all = st.checkbox(
        "Кавычить все поля в выходном CSV",
        value=False,
        help="Заключать каждое поле результата в кавычки. Удобно для импорта в строгие системы, но увеличивает размер файла."
    )

    profiles_text = st.text_input(
        "Профили (через запятую)",
        value="base,TH",
        help="Набор преднастроек для стран/общих правил. Влияет на алиасы городов/регионов, шаблоны ZIP, whitelist регионов и суффиксы улиц. Пример: base,PK."
    )
    chunksize = st.number_input(
        "Размер чанка",
        min_value=1000,
        max_value=200000,
        value=10000,
        step=1000,
        help="Сколько строк обрабатывать за один проход. Больше — быстрее, но требуется больше памяти."
    )
    mode = st.radio(
        "Политика извлечения",
        ["fill-missing-only", "extract-all-to-fill"],
        index=0,
        help="fill-missing-only: извлекать из address только недостающие поля. extract-all-to-fill: парсить address и дополнять/переопределять значения."
    )
    street_from_address = st.checkbox(
        "Собирать street из address (road + house_number)",
        value=False,
        help="Если из address извлечены road и house_number — собрать их в поле street. Если выключено, street берётся только из одноимённой колонки."
    )
    libpostal_url = st.text_input(
        "Libpostal URL (опц.)",
        value="http://localhost:8080/parser",
        help="REST API Libpostal для разметки address. Оставьте пустым, чтобы использовать локальные эвристики (хуже качество, но без внешнего сервиса)."
    )
    concurrency = st.number_input(
        "Параллельные запросы к Libpostal",
        min_value=1, max_value=64, value=4, step=1,
        help="Количество одновременных запросов к Libpostal. Увеличьте для ускорения при достаточных ресурсах CPU/WSL2."
    )
    libpostal_always = st.checkbox(
        "Всегда вызывать Libpostal (если address пуст — собрать из полей)",
        value=True,
        help="Даже если столбца address нет: составить адрес из street/district/locality/region/zip/country и отправить в Libpostal."
    )
    validate = st.selectbox(
        "Валидация",
        ["off", "loose", "strict"],
        index=1,
        help="off — не валидировать. loose — мягкие автопочинки городов и проверка ZIP по паттернам. strict — без автопочинок, неверные ZIP очищаются."
    )
    fuzzy_threshold = st.slider(
        "Порог fuzzy для починок",
        min_value=70,
        max_value=100,
        value=85,
        step=1,
        help="Минимальное сходство (0–100) для автопочинки названий в режиме 'loose'."
    )
    estimate_total = st.checkbox(
        "Оценивать прогресс (подсчитать строки заранее)",
        value=True,
        help="Быстрая оценка количества строк для корректного прогресса и ETA. Может немного замедлить старт на больших файлах."
    )
    st.divider()
    st.subheader("Сохранение результатов")
    save_dir = st.text_input(
        "Директория сохранения",
        value=r"D:\Desktop",
        help="Куда сохранять финальный CSV, отчёт (report.json) и примеры (samples)."
    )
    temp_dir = st.text_input(
        "Временная директория (опц.)",
        value="",
        help="Куда складывать временные файлы работы (копия входного CSV и пр.). Укажите путь на диске с большим свободным местом (например, D:\\Temp в Windows или /home/<user>/tmp в WSL). Если пусто — используется системный TEMP."
    )
    cleanup_temp = st.checkbox(
        "Удалять временную директорию после завершения",
        value=True,
        help="Автоматически удалить рабочую папку после завершения задачи, чтобы не занимать место."
    )
    out_name_hint = "<имя входного файла>Done.csv"
    out_name_input = st.text_input(
        "Имя выходного CSV",
        value="",
        placeholder=out_name_hint,
        help="Оставьте пустым — будет использовано имя входного файла с суффиксом 'Done'."
    )

st.subheader("Шаг 1 — Загрузка файлов")
uploaded_csv = st.file_uploader(
    "Входной CSV",
    type=["csv"],
    help="Исходный файл с адресами. Поддерживаются произвольные колонки: целевые поля street/district/locality/region/country/zip/address будут выделены автоматически."
)
uploaded_rules = st.file_uploader(
    "rules.yaml / rules.json (опц.)",
    type=["yaml", "yml", "json"],
    help="Пользовательские правила: ZIP-паттерны (country_zip_regex), синонимы (synonyms), флаги очистки (drop_*). Перекрывают настройки профилей."
)

run_clicked = st.button(
    "Запустить нормализацию",
    type="primary",
    disabled=(uploaded_csv is None),
    help="Старт пайплайна с параметрами из боковой панели. Результаты и отчёты появятся ниже."
)

if run_clicked and uploaded_csv is not None:
    with st.spinner("Обработка… это может занять время на больших файлах"):
        # Временная директория для прогона
        work_parent = (temp_dir.strip() or None)
        if work_parent:
            try:
                os.makedirs(work_parent, exist_ok=True)
            except Exception as e:
                st.warning(f"Не удалось создать временную директорию {work_parent}: {e}. Будет использован системный TEMP.")
                work_parent = None
        workdir = tempfile.mkdtemp(prefix="addrnorm_", dir=work_parent)
        input_path = os.path.join(workdir, "input.csv")
        with open(input_path, "wb") as f:
            f.write(uploaded_csv.read())

        rules_path: Optional[str] = None
        if uploaded_rules is not None:
            # сохранить rules
            ext = os.path.splitext(uploaded_rules.name)[1].lower() or ".yaml"
            rules_path = os.path.join(workdir, f"rules{ext}")
            with open(rules_path, "wb") as f:
                f.write(uploaded_rules.read())

        # Определяем директорию сохранения и итоговые имена
        final_dir = save_dir.strip() or os.path.join(workdir, "out")
        try:
            os.makedirs(final_dir, exist_ok=True)
        except Exception as e:
            st.warning(f"Не удалось создать директорию {final_dir}: {e}. Будет использована временная директория.")
            final_dir = os.path.join(workdir, "out")
            os.makedirs(final_dir, exist_ok=True)

        # Имя выходного CSV: <ввод>Done.csv, если не задано вручную
        in_stem = os.path.splitext(uploaded_csv.name)[0]
        out_name = (out_name_input.strip() or f"{in_stem}Done.csv")
        if not out_name.lower().endswith(".csv"):
            out_name += ".csv"
        out_stem = os.path.splitext(out_name)[0]

        output_path = os.path.join(final_dir, out_name)
        report_path = os.path.join(final_dir, f"{out_stem}.report.json")
        samples_dir = final_dir

        # Прогресс-бар
        progress = st.progress(0.0, text="Подготовка…")

        # Оценка общего количества строк (без заголовка)
        total_est = 0
        if estimate_total:
            try:
                with open(input_path, "rb") as fh:
                    total_est = sum(1 for _ in fh) - 1
                    if total_est < 0:
                        total_est = 0
            except Exception:
                total_est = 0

        # Перехват логирования прогресса из пайплайна
        orig_log_progress = getattr(loggingx, "log_progress", None)

        def _ui_log_progress(total_rows: int, processed: int, speed: float, eta_text: str, quiet: bool = False):
            denom = total_est if total_est > 0 else max(processed, 1)
            ratio = min(1.0, processed / denom)
            progress.progress(ratio, text=f"Обработано {processed:,} из ~{(denom):,}  |  ≈{int(speed):,}/с")

        if orig_log_progress is not None:
            loggingx.log_progress = _ui_log_progress  # type: ignore

        # Запуск пайплайна
        t0 = time.time()
        run_job(
            input_path=input_path,
            output_path=output_path,
            report_path=report_path,
            samples_dir=samples_dir,
            # чтение/запись
            encoding=enc,
            sep=sep,
            quote_all=quote_all,
            # режимы
            profiles=_split_profiles(profiles_text),
            chunksize=int(chunksize),
            mode=mode,
            street_from_address=street_from_address,
            libpostal_url=(libpostal_url.strip() or None),
            validate=validate,
            fuzzy_threshold=int(fuzzy_threshold),
            concurrency=int(concurrency),
            libpostal_always=bool(libpostal_always),
            quiet=True,
            rules_path=rules_path,
        )
        # Восстановим логгер прогресса
        if orig_log_progress is not None:
            loggingx.log_progress = orig_log_progress  # type: ignore

        dt = time.time() - t0
        progress.progress(1.0, text=f"Готово за {dt:.1f} c")

        # Чтение артефактов
        st.success(f"Готово за {dt:.1f} c")
        # preview CSV
        try:
            preview_df = pd.read_csv(output_path, dtype=str, keep_default_na=False, nrows=200)
            st.subheader("Итоговый CSV (первые 200 строк)")
            st.dataframe(preview_df)
        except Exception as e:
            st.error(f"Не удалось прочитать результат: {e}")

        # Кнопки скачивания
        with open(output_path, "rb") as f:
            st.download_button("Скачать normalized.csv", f.read(), file_name="normalized.csv", mime="text/csv")

        # report.json
        if os.path.exists(report_path):
            with open(report_path, "r", encoding="utf-8") as f:
                rep = json.load(f)
            st.subheader("Отчёт (report.json)")
            cols = rep.get("counters", {})
            conflicts = rep.get("conflicts", {})
            st.json({"counters": cols, "conflicts": conflicts})
            with open(report_path, "rb") as f:
                st.download_button("Скачать report.json", f.read(), file_name="report.json", mime="application/json")

        # samples: переименуем в тот же stem, что и CSV (если файл создан пайплайном)
        in_basename = os.path.splitext(os.path.basename(input_path))[0]
        default_samples = os.path.join(samples_dir, f"{in_basename}.samples.txt")
        desired_samples = os.path.join(samples_dir, f"{out_stem}.samples.txt")
        samples_path = desired_samples if os.path.exists(desired_samples) else (default_samples if os.path.exists(default_samples) else None)
        if samples_path and (samples_path != desired_samples):
            try:
                shutil.copyfile(samples_path, desired_samples)
                samples_path = desired_samples
            except Exception:
                pass
        if samples_path and os.path.exists(samples_path):
            with open(samples_path, "r", encoding="utf-8") as f:
                content = f.read()
            st.subheader("Примеры изменений (samples)")
            st.text_area("samples", value=content, height=200)
            with open(samples_path, "rb") as f:
                st.download_button("Скачать samples.txt", f.read(), file_name=os.path.basename(samples_path))

        # Всё в ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for p in (output_path, report_path, (samples_path or "")):
                if p and os.path.exists(p):
                    z.write(p, arcname=os.path.basename(p))
        st.download_button("Скачать все артефакты (ZIP)", buf.getvalue(), file_name="addrnorm_output.zip", mime="application/zip")

        # Информация о сохранении
        st.info(f"Результаты сохранены в: {final_dir}\nCSV: {os.path.basename(output_path)}\nReport: {os.path.basename(report_path)}")

        # Очистка временной директории
        if cleanup_temp:
            try:
                shutil.rmtree(workdir, ignore_errors=True)
            except Exception:
                pass
