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
    enc = st.selectbox("Кодировка входного CSV", ["utf-8", "utf-16", "cp1251", "latin1"], index=0)
    sep_label = st.selectbox("Разделитель", [",", ";", "\\t", "|"] , index=0)
    sep = "\t" if sep_label == "\\t" else sep_label
    quote_all = st.checkbox("Кавычить все поля в выходном CSV", value=False)

    profiles_text = st.text_input("Профили (через запятую)", value="base,TH")
    chunksize = st.number_input("Размер чанка", min_value=1000, max_value=200000, value=10000, step=1000)
    mode = st.radio("Политика извлечения", ["fill-missing-only", "extract-all-to-fill"], index=0)
    street_from_address = st.checkbox("Собирать street из address (road + house_number)", value=False)
    libpostal_url = st.text_input("Libpostal URL (опц.)", value="http://localhost:8080/parser")
    validate = st.selectbox("Валидация", ["off", "loose", "strict"], index=1)
    fuzzy_threshold = st.slider("Порог fuzzy для починок", min_value=70, max_value=100, value=85, step=1)

st.subheader("Шаг 1 — Загрузка файлов")
uploaded_csv = st.file_uploader("Входной CSV", type=["csv"]) 
uploaded_rules = st.file_uploader("rules.yaml / rules.json (опц.)", type=["yaml","yml","json"]) 

run_clicked = st.button("Запустить нормализацию", type="primary", disabled=(uploaded_csv is None))

if run_clicked and uploaded_csv is not None:
    with st.spinner("Обработка… это может занять время на больших файлах"):
        # Временная директория для прогона
        workdir = tempfile.mkdtemp(prefix="addrnorm_")
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

        out_dir = os.path.join(workdir, "out")
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, "normalized.csv")
        report_path = os.path.join(out_dir, "report.json")
        samples_dir = out_dir

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
            concurrency=1,
            quiet=True,
            rules_path=rules_path,
        )
        dt = time.time() - t0

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

        # samples
        in_basename = os.path.splitext(os.path.basename(input_path))[0]
        samples_path = os.path.join(samples_dir, f"{in_basename}.samples.txt")
        if os.path.exists(samples_path):
            with open(samples_path, "r", encoding="utf-8") as f:
                content = f.read()
            st.subheader("Примеры изменений (samples)")
            st.text_area("samples", value=content, height=200)
            with open(samples_path, "rb") as f:
                st.download_button("Скачать samples.txt", f.read(), file_name=os.path.basename(samples_path))

        # Всё в ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for p in (output_path, report_path, samples_path):
                if p and os.path.exists(p):
                    z.write(p, arcname=os.path.basename(p))
        st.download_button("Скачать все артефакты (ZIP)", buf.getvalue(), file_name="addrnorm_output.zip", mime="application/zip")

        # Скопировать out в кэш сессии для повторной загрузки (опц.)
        st.info(f"Результаты сохранены во временной директории: {out_dir}")

