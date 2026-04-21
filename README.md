<div align="center">

# DB Manager
### A full-stack PostgreSQL database management tool built from scratch

**Browse, query, and manage multiple PostgreSQL databases from a clean web UI - with production-grade performance and security**

[![CI](https://github.com/tomadams2909/db_management_app/actions/workflows/ci.yml/badge.svg)](https://github.com/tomadams2909/db_management_app/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.13-blue)
![Flask](https://img.shields.io/badge/Flask-3.1-000000?logo=flask&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791?logo=postgresql&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

[What it is](#what-it-is) · [Architecture](#architecture) · [Stack](#stack) · [Getting Started](#getting-started) · [Features](#features) · [Testing](#testing)

</div>

---

## What it is

A self-hosted PostgreSQL management tool - think a lightweight TablePlus or pgAdmin, built entirely from scratch. Connect multiple databases, browse and edit data, run raw SQL, manage schema, and bulk upload from CSV or Excel - all from a single web interface.

```
Browse        →  table overview with row counts, sizes, and column stats
View & Edit   →  inline cell editing, add/delete rows, filter, sort, paginate
Schema ops    →  create/drop tables, add/drop/rename columns, change types
Bulk upload   →  CSV and Excel import with preview before commit
Export        →  CSV and Excel export with BYTEA columns excluded
Execute SQL   →  raw query runner with results displayed in the UI
Blob download →  BYTEA binary files detected by magic bytes and downloaded
```

---

## Screenshots

### Database overview - table stats, sizes, and row counts
![Database overview](docs/screenshots/browse-overview.png)

### Table view - filter, sort, and paginate with inline editing
![Table view](docs/screenshots/table-view.png)

### Schema operations and raw SQL execution
![Operations page](docs/screenshots/operations.png)

### Bulk upload - CSV/Excel preview before commit
![Upload preview](docs/screenshots/upload-preview.png)

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Flask Routes  (app.py)              │
│  /browse  /export  /upload  /ops  /health  /api  │
└──────────────────────┬──────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────┐
│           DatabaseAPI class  (api.py)            │
│   Orchestrates caching, cache invalidation,      │
│   threshold decisions, and post-write reloads    │
└───┬───────────────┬──────────────────┬───────────┘
    │               │                  │
┌───▼──────┐ ┌──────▼──────┐ ┌────────▼────────┐
│  db/read │ │  db/schema  │ │  db/table_cache │
│  keyset  │ │  DDL ops    │ │  in-memory rows │
│  pagination│ │             │ │  filter/sort    │
└───┬──────┘ └──────┬──────┘ └─────────────────┘
    │               │
┌───▼───────────────▼─────────────────────────────┐
│         SQLAlchemy 2.0  +  psycopg2              │
│              PostgreSQL 17                        │
└─────────────────────────────────────────────────┘
```

**Two-tier caching** - small tables (≤ 50k rows) are loaded into memory on first access; filter, sort, and paginate are served from Python with zero DB round trips. Large tables use keyset pagination and hit the DB on every request.

**Keyset pagination** - bidirectional cursor-based pagination on an indexed PK column. O(1) cost regardless of page depth, unlike OFFSET which scans all preceding rows.

**Parallel metadata fetching** - sidebar tables, column metadata, and row data are fetched concurrently via `ThreadPoolExecutor` on every table page load.

---

## Stack

| Layer | Technology |
|---|---|
| Backend | Flask 3.1 · Werkzeug |
| Database | PostgreSQL 17 |
| ORM | SQLAlchemy 2.0 |
| DB Driver | psycopg2-binary |
| Templating | Jinja2 |
| Auth | Session login (browser) · Bearer token (API) · functools.wraps decorator |
| Rate limiting | flask-limiter |
| File handling | openpyxl · filetype (magic bytes) |
| Testing | pytest |
| CI | GitHub Actions |
| Deployment | Docker Compose |

---

## Getting Started

### Docker (recommended)

```bash
git clone https://github.com/tomadams2909/db_management_app.git
cd db_management_app
docker compose up --build
```

Docker starts a PostgreSQL 17 instance, waits for it to be healthy, then starts the Flask app. No configuration needed.

| Service | URL |
|---|---|
| DB Manager | http://localhost:5000 |
| Health check | http://localhost:5000/health |

<details>
<summary>Manual setup (without Docker)</summary>

**Prerequisites:** Python 3.11+, a running PostgreSQL instance

```bash
git clone https://github.com/tomadams2909/db_management_app.git
cd db_management_app

python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your PostgreSQL connection strings and a secret key
```

Add your database URLs to `.env`:
```
LOCAL_DB_URL=postgresql://user:password@localhost:5432/yourdb
FLASK_SECRET_KEY=your-secret-key
API_TOKEN=your-api-token
```

```bash
python app.py
```

</details>

### Authentication

**Browser:** go to `/login` and enter the `API_TOKEN` value from your `.env` file. The session persists until you log out.

**API clients (curl, scripts):** pass the token as a Bearer header on any write route:

```bash
curl -X POST http://localhost:5000/delete-row \
  -H "Authorization: Bearer your-api-token" \
  -d "..."
```

Read-only routes (`/browse`, `/export`, `/api/*`) are open. Login is rate-limited to 10 attempts per minute.

---

## Features

### Keyset pagination

Large tables (> 50,000 rows) use bidirectional keyset (cursor) pagination instead of OFFSET. `OFFSET N` forces the database to scan and discard all preceding rows - cost grows linearly with depth. Keyset pagination uses `WHERE pk > :cursor` on an indexed column, keeping every page request O(1) regardless of position.

Direction is handled by flipping the `ORDER BY` clause and reversing the result set in Python, so rows always display in the same order whether navigating forward or back.

### Two-tier caching

Small tables are loaded entirely into memory on first access. Subsequent filter, sort, and paginate operations are served from Python with no database round trip. The threshold is 50,000 rows.

Large tables are never cached - keyset pagination queries the database directly on every request, avoiding loading tens of thousands of rows into memory.

After any write, the cache is invalidated and immediately reloaded for small tables so the UI always reflects current state without a manual refresh.

### BYTEA blob handling

Binary columns (`bytea`) are excluded from all main queries - a `__blob__` placeholder is injected instead. Blobs are only fetched on explicit download request. File type is detected from the magic bytes of the binary data using the `filetype` library - no filename or extension is needed. The MIME type is mapped to a clean file extension for the download.

### Parallel metadata fetching

The table view page fires three concurrent requests via `ThreadPoolExecutor`: sidebar table list, column metadata, and row data. All three resolve before the page renders.

### Schema management

Full DDL support from the UI:

- Create tables with configurable PK strategy - `SERIAL`, `UUID` with `gen_random_uuid()`, custom, or none
- Drop tables with confirmation name check
- Add columns with `NULL` / `NOT NULL` control
- Drop, rename, and retype columns (with `USING` cast clause)
- Clear table data while preserving schema

### Bulk upload

CSV and Excel files are parsed, previewed (first 5 rows), and confirmed before any data is written. The PK column is stripped before insert so the database auto-assigns it. All rows are inserted in a single transaction - if any row fails, the entire upload rolls back and the error message includes the row number that caused the failure.

### Security

- **SQL injection prevention** - every table and column name is passed through `validate_identifier()` (regex whitelist) and double-quoted before being interpolated into SQL. User-supplied values always use SQLAlchemy bound parameters.
- **Secrets in environment** - no credentials in source code.
- **Session auth + Bearer token** - browser login via `/login` with session cookie; API clients use `Authorization: Bearer`. All write and schema routes protected by both.
- **Rate limiting** - login capped at 10/min, export at 10/min, SQL execution at 30/min, destructive ops at 5/min.
- **Upload cap** - 50 MB maximum file size with a clean 413 response.

---

## Testing

```bash
# Unit tests - no database required, runs instantly
pytest tests/test_validate_identifier.py tests/test_sql_injection_blocked.py -v

# Integration tests - requires PostgreSQL at localhost:5433
pytest tests/test_db_integration.py -v -m integration

# Full suite
pytest -v
```

| File | What it covers |
|---|---|
| `test_validate_identifier.py` | 14 unit tests - valid identifiers pass, every injection pattern rejected |
| `test_sql_injection_blocked.py` | 30 tests - every db module function rejects bad identifiers before touching the DB |
| `test_db_integration.py` | CRUD, bulk insert, transaction rollback, pagination, filter, export against a real scratch table |

---

## Project Structure

```
db_manager/
├── app.py                  # Flask routes, auth decorator, middleware, error handlers
├── api.py                  # DatabaseAPI - orchestrates caching and DB calls
│
├── db/
│   ├── read.py             # SELECT queries, keyset pagination, blob fetch
│   ├── edit.py             # UPDATE
│   ├── delete.py           # DELETE
│   ├── upload.py           # Single row INSERT
│   ├── bulk_upload.py      # Transactional bulk INSERT from CSV/Excel
│   ├── schema.py           # DDL - CREATE/DROP/ALTER TABLE
│   ├── export.py           # CSV and Excel export
│   ├── sql_exec.py         # Raw SQL execution
│   ├── utils.py            # Engine cache, identifier validation, type map
│   └── table_cache.py      # In-memory row cache, filter, sort, paginate
│
├── config/
│   ├── settings.py         # dotenv loader
│   └── databases.py        # Database enum
│
├── templates/              # Jinja2 HTML templates
├── style/                  # CSS
├── tests/                  # pytest suite
├── docs/screenshots/       # UI screenshots
│
├── Dockerfile
├── docker-compose.yml
├── .github/workflows/ci.yml
├── .env.example
├── requirements.txt
└── pyproject.toml
```

---

## License

MIT - see [LICENSE](LICENSE)

---

<div align="center">
Built with Flask · SQLAlchemy · PostgreSQL · Docker
</div>
