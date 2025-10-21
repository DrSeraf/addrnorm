# AddrNorm — как это устроено и как пользоваться

Этот документ объясняет логику работы инструмента нормализации адресов, формат входа/выхода, роли профилей и пользовательского файла правил, а также ключевые опции CLI.

## Что делает AddrNorm

- Принимает CSV с колонками адреса в любом «состоянии» (частично заполнены, разъехались из‑за запятых в адресе и т.п.).
- Приводит данные к целевым полям в фиксированном порядке: `street, district, locality, region, country, zip`.
- Извлекает недостающие части из полной строки `address` (через Libpostal REST или локальную эвристику).
- Нормализует значения (алиасы стран/городов/регионов, регистр, разделители и др.).
- Валидирует `zip` по шаблонам и города по GeoNames (офлайн через `geonamescache`).
- Записывает результат, собирает отчёт и примеры изменений.

## Быстрый старт

```bash
# вариант 1: запуск как скрипт из корня проекта
python cli.py examples/sample_input.csv -o out/normalized.csv --report out/report.json --samples-dir out --profiles base,TH --mode fill-missing-only --validate loose --fuzzy-threshold 85 --rules rules.yaml

# вариант 2: запуск как пакет (из родительской директории, где addrnorm — пакет)
python -m addrnorm.cli addrnorm/examples/sample_input.csv -o addrnorm/out/normalized.csv --report addrnorm/out/report.json --samples-dir addrnorm/out --profiles base,TH --mode fill-missing-only --validate loose --fuzzy-threshold 85 --rules addrnorm/rules.yaml
```

- Если `--rules` не указан, CLI попробует автоматически взять `rules.yaml` из текущей директории или директории `--output`.
- Для лучшего извлечения полей из `address` можно поднять Libpostal REST и указать `--libpostal-url http://localhost:8080/parser`.

## Вход и выход

- Входной CSV может иметь произвольный набор колонок. Важные для пайплайна имена: `street, district, locality, region, country, zip, address`.
- Если `address` присутствует, все колонки правее автоматически «склеиваются» хвостом обратно в `address` (см. `utils/io_utils.py`). Если `address` отсутствует — все нецелевые колонки объединяются в новый `address`.
- Выходной CSV всегда содержит 6 колонок в порядке: `street, district, locality, region, country, zip`.
- Отчёт `report.json` и примеры `*.samples.txt` формируются рядом с `--output` (переопределяется `--report`, `--samples-dir`).

## Архитектура и файлы

- `cli.py` — тонкая оболочка CLI, парсит флаги и вызывает пайплайн.
- `pipeline.py` — оркестратор процессов чтения чанками, очистки, извлечения, нормализации, валидации и записи.
- `rules/` — правила очистки/нормализации/валидации и профили:
  - `rules/profiles/base.yml` и др. — преднастроенные профили (алиасы, нормализация суффиксов улиц, whitelists регионов, шаблоны ZIP по странам и т.п.).
  - `rules/*.py` — реализация этапов: `clean.py`, `normalize.py`, `validate.py`.
- `parsers/address_extract.py` — извлечение из `address` через Libpostal REST или локальную эвристику (fallback).
- `utils/` — утилиты (чтение/запись CSV, логирование прогресса, сбор примеров, отчёт).

## Этапы пайплайна

1) CLEAN (rules/clean.py)
- Базовая чистка всех значений: Unicode NFKC, тримминг, схлопывание пробелов, снятие висячей пунктуации.
- Сбор стратифицированных примеров изменений для `*.samples.txt` и счётчиков в `report.json`.

2) PROMOTE_ADDRESS (pipeline.py)
- Если `address` пуст, но один из столбцов фактически содержит полный адрес (много запятых, цифры + слова улицы) — переносим его в `address` и очищаем исходную ячейку.
- Частный кейс: если `locality` выглядит как ZIP, а `street` похоже на адрес — весь `street` переносится в `address`.

3) REPAIR (pipeline.py)
- Осторожный ремонт «съехавших» строк: чистим `region`, если он похож на улицу; перемещаем страну из `district`, если там на самом деле страна; стираем явные смеси `city+zip` в нецелевых полях (пусть заполнится из `address`).

4) EXTRACT (parsers/address_extract.py)
- Если `--libpostal-url` указан — пробуем REST (`/parser` c `{query}`), иначе используем fallback‑эвристику.
- Политики:
  - `--mode fill-missing-only` — заполняем только пустые целевые поля.
  - `--mode extract-all-to-fill` — агрессивнее достраиваем недостающее.
  - `--street-from-address` — особый режим: составляем `street = road + house_number (+ unit)`.

5) NORMALIZE (rules/normalize.py)
- Страны: `pycountry` + алиасы из профилей; сохраняется `alpha2` для последующей валидации.
- Регион/город/район: применяются глобальные и страновые алиасы; мягкий Title Case для латиницы.
- Улица: нормализация разделителей и распространённых суффиксов (на латинице) по профилю.
- ZIP: вычленяется финальный индекс (если в строке есть слова) — обязательно должен содержать цифры; чистка пробелов.

6) VALIDATE (rules/validate.py)
- ZIP — проверка по регулярным выражениям из профилей (`profiles_data.zip_patterns[ALPHA2]`).
- Город — офлайн‑проверка по базе GeoNames (через `geonamescache`):
  - `validate=loose`: допускает мягкие автопочинки (fuzzy по лёгкой эвристике) при `score >= --fuzzy-threshold`.
  - `validate=strict`: только точные совпадения, но флаги конфликтов фиксируются.
- Регион — при наличии whitelist в профиле: проверка / мягкая автопочинка.

7) WRITE
- Запись чанка только целевых колонок в `--output` с параметрами `--sep` и `--quote-all`.
- Итоговый отчёт и `*.samples.txt` — после завершения всех чанков.

## Профили и файл правил

### Профили (`rules/profiles/*.yml`)
- Задают базовые алиасы, whitelists регионов, нормализацию суффиксов улиц и т.д.
- Можно указать несколько через `--profiles base,TH`. Порядок не критичен, словари мёрджатся, списки конкатенируются.

### Пользовательский `rules.yaml`
Файл свободной формы, поддерживаются ключи:

- `country_zip_regex`: карта «имя страны (любой формат, распознаётся pycountry) → regex ZIP».
  - На этапе загрузки конвертируется в `zip_patterns` с ключами ISO alpha‑2 (например, `TH`, `AU`).
- `synonyms`: глобальные синонимы для городов (применяются в нормализации `locality`).
- Дополнительные флаги прокидываются в `profiles_data` (на будущее):
  - `drop_unit_attrs`, `drop_emails_phones_from_street`, `fix_echo_locality_region`, `drop_non_addressy_single_tokens`.

Пример:

```yaml
country_zip_regex:
  THAILAND: '^(10|11|12|...|77)\d{3}$'
  AUSTRALIA: '^\d{4}$'
  UNITED STATES: '^\d{5}(-\d{4})?$'

synonyms:
  BKK: Bangkok
  'CHON BURI': Chonburi
```

## CLI опции

Основные параметры `cli.py`:

- I/O:
  - `input` — путь к входному CSV.
  - `-o, --output` — путь к выходному CSV.
  - `--report` — путь к `report.json` (по умолчанию рядом с `--output`).
  - `--samples-dir` — директория, куда складываются `*.samples.txt` (по умолчанию рядом с `--output`).
  - `--encoding` — кодировка входного CSV (по умолчанию `utf-8`).
  - `--sep` — разделитель CSV (по умолчанию `,`).
  - `--quote-all` — кавычить все поля в выходном CSV.
- Управление пайплайном:
  - `--profiles base,TH` — список YAML‑профилей.
  - `--rules rules.yaml` — пользовательские правила (ZIP‑паттерны, синонимы).
  - `--chunksize` — размер чанка чтения CSV (по умолчанию `10000`).
  - `--mode` — `fill-missing-only` или `extract-all-to-fill`.
  - `--street-from-address` — собирать только `street` из `address`.
  - `--libpostal-url` — URL Libpostal REST (`/parser`).
  - `--validate` — `off | loose | strict`.
  - `--fuzzy-threshold` — порог сходства для мягких починок.
  - `--concurrency` — зарезервировано (сейчас 1).
  - `--quiet` — не печатать прогресс.

## Логи и отчёт

- `report.json` — агрегирует счётчики изменений по колонкам и флаги конфликтов (`zip_format_fail`, `region_not_in_whitelist` и др.).
- `*.samples.txt` — для каждой колонки собирается до 20 показательных примеров изменений (clean/normalize/extracted/removed/fuzzy_fixed).

## Советы по качеству результата

- Для лучшего `EXTRACT` используйте Libpostal REST с английской/латинизированной формой адреса.
- Добавляйте профиль страны (`TH`, `AU` и т.п.) — это улучшит алиасы/whitelist регионов.
- Уточняйте ZIP‑паттерны в `rules.yaml` — это уберёт мусорные индексы и приведёт формат (например, UK вставка пробела перед последними 3 символами).
- Проверяйте `*.samples.txt` — это быстрый способ понять, где пайплайн делает нежелательные правки.

## Ограничения

- Полная точность парсинга без Libpostal ограничена простыми эвристиками.
- Офлайн база городов (GeoNames через `geonamescache`) неполная для некоторых стран/локалей.
- Валидация регионов зависит от whitelist в профиле; если его нет — только лёгкая проверка/алиасы.

## Где смотреть код

- CLI: `cli.py`
- Пайплайн: `pipeline.py`
- Правила: `rules/clean.py`, `rules/normalize.py`, `rules/validate.py`
- Профили: `rules/profiles/base.yml`, `rules/profiles/TH.yml`
- Извлечение: `parsers/address_extract.py`
- Утилиты: `utils/io_utils.py`, `utils/loggingx.py`, `utils/report.py`

## Мини‑FAQ

- «Почему в ZIP попадают слова?» — В `normalize` ZIP теперь обязателен цифровой токен; если его нет, поле очищается. Дополнительно, на этапе `validate` применяется країно‑специфичный regex.
- «Почему `address` пропал из вывода?» — Он используется только как источник для извлечения полей. Выходной CSV всегда состоит из 6 целевых колонок.
- «Как добавить собственные алиасы городов?» — Через `rules.yaml` → `synonyms`, либо через профиль (если нужно по стране).
- «Чем отличаются `profiles` и `rules.yaml`?» — Профили — преднастроенные YAML для стран/общих правил. `rules.yaml` — пользовательская надстройка, которая мёрджится поверх профилей во время запуска.

