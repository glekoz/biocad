# Biocad TSV Processor

Система обработки TSV-файлов с генерацией PDF и REST API для доступа к данным.

## Архитектура

```
┌─────────────────────────────────────────────────────────┐
│                     Директория файлов                   │
│  *.tsv  →  completed/   errors/   pdfs/<unit_guid>/     │
└────────────────────┬────────────────────────────────────┘
                     │ polling
              ┌──────▼──────┐        ┌──────────────┐
              │   Worker    │──────▶ │  PostgreSQL  │
              │  (обработка)│        │  (записи)    │
              └─────────────┘        └──────┬───────┘
                                            │
                                     ┌──────▼───────┐
                                     │   REST API   │
                                     │  :PORT/api/  │
                                     └──────────────┘
```

**Worker** — фоновый процесс, который:
1. Следит за директорией и подхватывает новые `.tsv` файлы
2. Парсит и валидирует строки (параллельно, до `BIOCAD_MAX_WORKERS` файлов одновременно)
3. Сохраняет записи в PostgreSQL пакетами по `BIOCAD_BATCH_SIZE` строк
4. Генерирует PDF-файлы, сгруппированные по `unit_guid`
5. Перемещает обработанные файлы в `completed/` или `errors/`

**REST API** — HTTP-сервер для чтения данных из БД.

---

## Формат TSV-файла

Первая строка — заголовки (обязательны, порядок зафиксирован):

```
n	mqtt	invid	unit_guid	msg_id	text	context	class	level	area	addr	block	type	bit	invert_bit
```

Пример файла `data.tsv`:

```
n	mqtt	invid	unit_guid	msg_id	text	context	class	level	area	addr	block	type	bit	invert_bit
1		G-044322	01749246-95f6-57db-b7c3-2ae0e8be671f	cold7_Defrost_status	Разморозка		waiting	100	LOCAL	cold7_status.Defrost_status
2		G-044322	01749246-95f6-57db-b7c3-2ae0e8be671f	cold7_VentSK_status	Вентилятор		working	100	LOCAL	cold7_status.VentSK_status
```

Поля `n` и `level` обязаны быть целыми числами. Файл будет отклонён, если заголовки не совпадают или количество полей неверно.

---

## Структура рабочей директории

```
<BIOCAD_DIR_PATH>/
├── *.tsv           ← кладите сюда входные файлы
├── completed/      ← успешно обработанные файлы
├── errors/         ← файлы с ошибками парсинга/сохранения
└── pdfs/
    └── <unit_guid>/
        └── *.pdf   ← сгенерированные PDF по unit_guid
```

---

## Переменные окружения

Создайте файл `.env` в корне проекта:

```env
# PostgreSQL
BIOCAD_PG_USER=postgres
BIOCAD_PG_PASSWORD=secret
BIOCAD_PG_HOST=localhost
BIOCAD_PG_PORT=5432
BIOCAD_PG_DBNAME=biocad
BIOCAD_PG_SSLMODE=disable
BIOCAD_PG_POOL_MAX=10

# При запуске через docker-compose: "true" — использует имя сервиса "db" как хост
BIOCAD_PG_IS_CONTAINERIZED=true

# REST-сервер
BIOCAD_SERVER_PORT=8080
BIOCAD_SERVER_READ_TIMEOUT_SECONDS=15    # опционально, по умолчанию 15
BIOCAD_SERVER_WRITE_TIMEOUT_SECONDS=15   # опционально, по умолчанию 15
BIOCAD_SERVER_IDLE_TIMEOUT_SECONDS=60    # опционально, по умолчанию 60

# Worker
BIOCAD_DIR_PATH=/data                    # абсолютный путь к рабочей директории
BIOCAD_POLL_INTERVAL_MS=5000             # опционально, по умолчанию 5000
BIOCAD_MAX_WORKERS=3                     # опционально, по умолчанию 3
BIOCAD_BATCH_SIZE=1000                   # опционально, по умолчанию 1000
BIOCAD_FILE_PROCESSING_TIMEOUT_SECONDS=300  # опционально, по умолчанию 300
```

---

## Запуск через Docker Compose

Запускает все три сервиса (PostgreSQL, worker, REST API) в контейнерах. Миграции применяются автоматически при старте worker.

```bash
# Установить путь к .env и запустить
$env:BIOCAD_ENV_FILE=".env"; docker-compose --env-file $env:BIOCAD_ENV_FILE up --build
```

> **Примечание:** В `.env` установите `BIOCAD_PG_IS_CONTAINERIZED=true` и `BIOCAD_DIR_PATH` — путь внутри контейнера (например, `/data`). Том `./tmp-dir` монтируется в этот путь.

**Остановка:**
```bash
docker-compose down
```

**Остановка с удалением данных БД:**
```bash
docker-compose down -v
```

---

## Запуск как отдельные процессы

Подходит для разработки, когда PostgreSQL уже запущен локально.

### 1. Применить миграции

```bash
goose -dir worker/internal/repository/migrations \
  postgres "postgres://postgres:secret@localhost:5432/biocad?sslmode=disable" up
```

### 2. Запустить Worker

```bash
cd worker
go run ./cmd/main.go
```

Или собрать и запустить бинарник:
```bash
cd worker
go build -o biocad-worker ./cmd/main.go
./biocad-worker
```

### 3. Запустить REST API

```bash
cd rest
go run ./cmd/main.go
```

Или:
```bash
cd rest
go build -o biocad-web ./cmd/main.go
./biocad-web
```

> **Примечание:** Оба процесса читают переменные окружения напрямую. Установите их в оболочке или используйте `.env` файл (если сервис его поддерживает через `godotenv`). При локальном запуске можно использовать `BIOCAD_PG_IS_CONTAINERIZED=false` и указать реальный `BIOCAD_PG_HOST`.

---

## REST API

Базовый URL: `http://localhost:<BIOCAD_SERVER_PORT>`

### Получить записи по unit_guid

```
GET /api/v1/records/{unit_guid}?page=1&limit=10
```

**Пример запроса:**
```bash
curl "http://localhost:8080/api/v1/records/01749246-95f6-57db-b7c3-2ae0e8be671f?page=1&limit=2"
```

**Пример ответа:**
```json
{
  "records": [
    {
      "id": 1,
      "n": 1,
      "mqtt": null,
      "invid": "G-044322",
      "unit_guid": "01749246-95f6-57db-b7c3-2ae0e8be671f",
      "msg_id": "cold7_Defrost_status",
      "text": "Разморозка",
      "context": null,
      "class": "waiting",
      "level": 100,
      "area": "LOCAL",
      "addr": "cold7_status.Defrost_status",
      "block": null,
      "type": null,
      "bit": null,
      "invert_bit": null,
      "created_at": "2026-02-22T10:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 2,
    "total": 18
  }
}
```

| Параметр   | Тип    | По умолчанию | Описание                    |
|------------|--------|--------------|-----------------------------|
| `unit_guid`| string | —            | GUID юнита (path parameter) |
| `page`     | int    | 1            | Номер страницы (от 1)       |
| `limit`    | int    | 10           | Записей на странице (1–100) |

---

### Получить список файлов с ошибками

```
GET /api/v1/errors?page=1&limit=10
```

**Пример запроса:**
```bash
curl "http://localhost:8080/api/v1/errors?page=1&limit=5"
```

**Пример ответа:**
```json
{
  "files": [
    {
      "id": 1,
      "filename": "broken.tsv",
      "error": "invalid file format: wrong number of fields given: 10, expected: 15",
      "created_at": "2026-02-22T09:00:00Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 5,
    "total": 1
  }
}
```

---

## Типовой сценарий использования

1. Запустите систему (Docker Compose или отдельные процессы)
2. Скопируйте `.tsv` файл в `<BIOCAD_DIR_PATH>/` (или `./tmp-dir/` при Docker)
3. Worker подхватит файл в течение `BIOCAD_POLL_INTERVAL_MS` миллисекунд
4. После обработки файл переместится в `completed/`, а PDF появятся в `pdfs/<unit_guid>/`
5. Запросите данные через REST API

```bash
# Добавить файл
cp data.tsv ./tmp-dir/

# Через 5 секунд запросить данные
curl "http://localhost:8080/api/v1/records/01749246-95f6-57db-b7c3-2ae0e8be671f"
```