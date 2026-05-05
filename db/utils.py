from sqlalchemy import create_engine, text
from functools import lru_cache
import re


def validate_identifier(name: str) -> str:
    """Reject any identifier that isn't a plain SQL name (letters, digits, underscore).
    Raises ValueError on anything that could be used for SQL injection via identifier injection.
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name

def normalize_conn_string(conn: str) -> str:
    if conn.startswith("postgresql://"):
        return conn.replace("postgresql://", "postgresql+psycopg2://", 1)
    return conn

@lru_cache(maxsize=10)
def get_engine(db_conn_string: str):
    return create_engine(
        normalize_conn_string(db_conn_string),
        future=True,
        pool_pre_ping=True
    )

# ── Column type cache ─────────────────────────────────────────────────────────

_col_type_cache: dict[tuple, dict] = {}

def get_column_types_cached(db_conn_string: str, table: str) -> dict[str, str]:
    key = (db_conn_string, table)
    if key not in _col_type_cache:
        engine = get_engine(db_conn_string)
        query = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table
            ORDER BY ordinal_position
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"table": table}).fetchall()
        _col_type_cache[key] = {row[0]: row[1] for row in rows}
    return _col_type_cache[key]

def invalidate_column_cache(db_conn_string: str, table: str) -> None:
    _col_type_cache.pop((db_conn_string, table), None)

# ── Table columns cache (for forms/validation) ────────────────────────────────

_table_columns_cache: dict[tuple, list] = {}

def get_table_columns(db_conn_string: str, table: str) -> list[dict]:
    key = (db_conn_string, table)
    if key not in _table_columns_cache:
        engine = get_engine(db_conn_string)
        query = text("""
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = :table
            ORDER BY ordinal_position
        """)
        with engine.connect() as conn:
            rows = conn.execute(query, {"table": table}).mappings().all()
        _table_columns_cache[key] = [dict(r) for r in rows]
    return _table_columns_cache[key]

def invalidate_table_columns_cache(db_conn_string: str, table: str) -> None:
    _table_columns_cache.pop((db_conn_string, table), None)

# ── Tables list cache ─────────────────────────────────────────────────────────

_tables_list_cache: dict[str, list] = {}

def get_tables_cached(db_conn_string: str) -> list[str]:
    if db_conn_string not in _tables_list_cache:
        engine = get_engine(db_conn_string)
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        with engine.connect() as conn:
            rows = conn.execute(query).fetchall()
        _tables_list_cache[db_conn_string] = [row[0] for row in rows]
    return _tables_list_cache[db_conn_string]

def invalidate_tables_list_cache(db_conn_string: str) -> None:
    _tables_list_cache.pop(db_conn_string, None)

# ── Row count cache ───────────────────────────────────────────────────────────

_row_count_cache: dict[tuple, int] = {}

def get_row_count_cached(db_conn_string: str, table: str) -> int:
    key = (db_conn_string, table)
    if key not in _row_count_cache:
        engine = get_engine(db_conn_string)
        query = text(f'SELECT COUNT(*) FROM "{validate_identifier(table)}"')
        with engine.connect() as conn:
            _row_count_cache[key] = conn.execute(query).scalar()
    return _row_count_cache[key]

def invalidate_row_count_cache(db_conn_string: str, table: str) -> None:
    _row_count_cache.pop((db_conn_string, table), None)

# ── Type map + validation ─────────────────────────────────────────────────────

SQL_TYPE_MAP = {
    "integer": int,
    "bigint": int,
    "smallint": int,
    "numeric": float,
    "decimal": float,
    "real": float,
    "double precision": float,
    "boolean": lambda v: v.lower() in ("true", "1", "yes"),
    "character varying": str,
    "character": str,
    "text": str,
    "date": str,
    "timestamp without time zone": str,
    "timestamp with time zone": str,
}

def validate_row_data(columns: list[dict], form_data: dict) -> dict:
    validated = {}
    for col in columns:
        name = col["column_name"]
        sql_type = col["data_type"]
        nullable = col["is_nullable"] == "YES"
        value = form_data.get(name)
        if value in ("", None):
            if nullable:
                validated[name] = None
                continue
            if col.get("column_default"):
                continue  # let the DB use its default (e.g. serial/sequence)
            raise ValueError(f"{name} is required")
        caster = SQL_TYPE_MAP.get(sql_type)
        if not caster:
            raise ValueError(f"Unsupported SQL type: {sql_type}")
        try:
            validated[name] = caster(value)
        except Exception as exc:
            raise ValueError(f"Invalid value for {name} ({sql_type})") from exc
    return validated