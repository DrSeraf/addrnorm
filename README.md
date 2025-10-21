# AddrNorm — Address Normalization Toolkit

Инструмент для очистки и нормализации адресов со всего мира.  
Работает локально, использует **Libpostal REST API** для парсинга строк,  
а также YAML-профили для канонизации стран, регионов и городов.

---

## 🚀 Возможности
- Читает CSV любых форматов (гибкий парсер с защитой от ошибок)
- Нормализует и выравнивает столбцы:
  `street, district, locality, region, country, zip`
- Извлекает недостающие части адреса из полного `address`
- Поддерживает **Libpostal REST** (`http://localhost:8080/parser`)
- Ведёт подробный отчёт (`report.json`) и примеры изменений (`samples.txt`)
- Работает на Windows / WSL2 / Linux

---

## 📦 Установка
```bash
git clone https://github.com/yourname/addrnorm.git
cd addrnorm
pip install -r requirements.txt
```

---

## 🧱 Запуск Libpostal в Docker
```bash
docker rm -f libpostal-rest >/dev/null 2>&1 || true
docker run -d --name libpostal-rest --restart unless-stopped -p 8080:8080 oozman/libpostal-rest:latest
```

Проверка:
```bash
curl -s -X POST -H "Content-Type: application/json" -d '{"query":"100 main st buffalo ny"}' http://localhost:8080/parser
```

---

## ⚙️ Пример запуска нормализации
```bash
python -m addrnorm.cli addrnorm/examples/sample_input.csv   -o addrnorm/out/normalized.csv   --report addrnorm/out/report.json   --samples-dir addrnorm/out   --libpostal-url http://localhost:8080/parser   --profiles base,TH   --chunksize 10000   --mode fill-missing-only   --validate loose   --fuzzy-threshold 85
```

---

## 📂 Структура проекта
```
addrnorm/
├── cli.py
├── pipeline.py
├── normalize.py
├── rules/
│   ├── clean.py
│   ├── normalize.py
│   ├── validate.py
│   └── profiles/
│       ├── base.yml
│       └── TH.yml
├── parsers/
│   └── address_extract.py
└── utils/
    ├── io_utils.py
    ├── loggingx.py
    └── report.py
```

---

## 🧾 Выходные файлы
- `normalized.csv` — очищенные адреса  
- `report.json` — статистика изменений  
- `samples_*.txt` — примеры логов по колонкам

---

## 📜 Лицензия
MIT License © 2025
