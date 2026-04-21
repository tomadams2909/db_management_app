from sqlalchemy import text
from .utils import get_engine, get_column_types_cached, get_tables_cached, get_row_count_cached, invalidate_row_count_cache, validate_identifier
import filetype

BYTEA_PLACEHOLDER = "__blob__"
CACHE_ROW_THRESHOLD = 50_000

# Maps SQL types to sort cast expressions
SORT_CAST = {
    "integer": "::integer",
    "bigint": "::bigint",
    "smallint": "::smallint",
    "numeric": "::numeric",
    "decimal": "::numeric",
    "real": "::real",
    "double precision": "::double precision",
    "boolean": "::boolean",
    "date": "::date",
    "timestamp without time zone": "::timestamp",
    "timestamp with time zone": "::timestamptz",
}


def get_column_types(db_conn_string: str, table: str) -> dict[str, str]:
    """Return {column_name: data_type} — uses cache."""
    return get_column_types_cached(db_conn_string, table)


def get_tables(db_conn_string: str) -> list[str]:
    return get_tables_cached(db_conn_string)


def get_db_overview(db_conn_string: str) -> dict:
    """
    Single query that returns per-table stats AND database-level stats.
    Uses pg_stat_user_tables + pg_total_relation_size — one round trip total.
    Returns:
      {
        "tables": [{"table", "rows", "size_bytes", "size_pretty", "col_count"}, ...],
        "db_size_pretty": str,
        "db_size_bytes": int,
        "total_rows": int,
        "total_tables": int,
        "total_cols": int,
      }
    """
    engine = get_engine(db_conn_string)

    query = text("""
        SELECT
            t.table_name,
            COALESCE(s.n_live_tup, 0)                        AS row_estimate,
            pg_total_relation_size(
                (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
            )                                                  AS size_bytes,
            pg_size_pretty(
                pg_total_relation_size(
                    (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
                )
            )                                                  AS size_pretty,
            COUNT(c.column_name)                               AS col_count
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s
               ON s.schemaname = t.table_schema
              AND s.relname    = t.table_name
        LEFT JOIN information_schema.columns c
               ON c.table_schema = t.table_schema
              AND c.table_name   = t.table_name
        WHERE t.table_schema = 'public'
          AND t.table_type   = 'BASE TABLE'
        GROUP BY t.table_name, t.table_schema, s.n_live_tup
        ORDER BY t.table_name
    """)

    db_size_query = text("""
        SELECT
            pg_database_size(current_database()) AS size_bytes,
            pg_size_pretty(pg_database_size(current_database())) AS size_pretty
    """)

    with engine.connect() as conn:
        rows      = conn.execute(query).fetchall()
        db_size   = conn.execute(db_size_query).fetchone()

    tables = [
        {
            "table":       r[0],
            "rows":        int(r[1]),
            "size_bytes":  int(r[2]),
            "size_pretty": r[3],
            "col_count":   int(r[4]),
        }
        for r in rows
    ]

    return {
        "tables":        tables,
        "db_size_pretty": db_size[1] if db_size else "—",
        "db_size_bytes":  int(db_size[0]) if db_size else 0,
        "total_rows":     sum(t["rows"] for t in tables),
        "total_tables":   len(tables),
        "total_cols":     sum(t["col_count"] for t in tables),
    }


def get_row_count(
    db_conn_string: str,
    table: str,
    filter_col: str = None,
    filter_val: str = None,
) -> int:
    # If a filter is active we must hit the DB — can't use the count cache
    if filter_col and filter_val:
        engine = get_engine(db_conn_string)
        safe_table = validate_identifier(table)
        safe_filter_col = validate_identifier(filter_col)
        where_clause = f'WHERE CAST("{safe_filter_col}" AS TEXT) ILIKE :filter_val'
        params = {"filter_val": f"%{filter_val}%"}
        query = text(f'SELECT COUNT(*) FROM "{safe_table}" {where_clause}')
        with engine.connect() as conn:
            return conn.execute(query, params).scalar()
    return get_row_count_cached(db_conn_string, table)


def fetch_all_rows(
    db_conn_string: str,
    table: str,
    col_types: dict[str, str],
) -> tuple[list[str], list[dict]]:
    """
    Fetch every non-BYTEA row from the table for caching.
    Returns (all_columns, rows) where bytea cols have placeholder values.
    """
    engine = get_engine(db_conn_string)

    bytea_cols = [c for c, t in col_types.items() if t == "bytea"]
    select_cols = [c for c, t in col_types.items() if t != "bytea"]
    select_clause = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"

    query = text(f'SELECT {select_clause} FROM "{validate_identifier(table)}" ORDER BY 1')

    with engine.connect() as conn:
        result = conn.execute(query)
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

    # Inject placeholder for bytea cols
    for row in rows:
        for col in bytea_cols:
            row[col] = BYTEA_PLACEHOLDER

    all_columns = list(col_types.keys())
    return all_columns, rows


def get_rows(
    db_conn_string: str,
    table: str,
    page: int = 1,
    page_size: int = 100,
    filter_col: str = None,
    filter_val: str = None,
    sort_col: str = None,
    sort_dir: str = "asc",
) -> tuple[list[str], list[dict], list[str]]:
    """
    Standard offset-based query. Used as fallback only.
    Returns (columns, rows, bytea_cols).
    """
    engine = get_engine(db_conn_string)
    offset = (page - 1) * page_size

    col_types = get_column_types_cached(db_conn_string, table)
    bytea_cols = [c for c, t in col_types.items() if t == "bytea"]
    select_cols = [c for c, t in col_types.items() if t != "bytea"]
    safe_table = validate_identifier(table)

    where_clause = ""
    params = {"limit": page_size, "offset": offset}
    if filter_col and filter_val:
        if col_types.get(filter_col) != "bytea":
            safe_filter_col = validate_identifier(filter_col)
            where_clause = f'WHERE CAST("{safe_filter_col}" AS TEXT) ILIKE :filter_val'
            params["filter_val"] = f"%{filter_val}%"

    if sort_col and col_types.get(sort_col) != "bytea":
        safe_sort_col = validate_identifier(sort_col)
        col_type = col_types.get(sort_col, "text")
        cast = SORT_CAST.get(col_type, "")
        direction = "DESC" if sort_dir == "desc" else "ASC"
        order_clause = f'ORDER BY "{safe_sort_col}"{cast} {direction} NULLS LAST'
    else:
        order_clause = "ORDER BY 1"

    select_clause = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"
    query = text(f"""
        SELECT {select_clause}
        FROM "{safe_table}"
        {where_clause}
        {order_clause}
        LIMIT :limit OFFSET :offset
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

    for row in rows:
        for col in bytea_cols:
            row[col] = BYTEA_PLACEHOLDER

    all_columns = list(col_types.keys())
    return all_columns, rows, bytea_cols


def get_rows_keyset(
    db_conn_string: str,
    table: str,
    pk_col: str,
    last_pk_val: str = None,
    direction: str = "next",
    page_size: int = 100,
    filter_col: str = None,
    filter_val: str = None,
    sort_col: str = None,
    sort_dir: str = "asc",
) -> tuple[list[str], list[dict], list[str], bool, str | None, str | None]:
    """
    Keyset pagination for large tables (> CACHE_ROW_THRESHOLD rows).
    Returns (columns, rows, bytea_cols, has_more, first_pk, last_pk).
    - has_more: True if there are more rows in the current direction
    - first_pk: pk value of first row returned (for Prev navigation)
    - last_pk: pk value of last row returned (for Next navigation)
    """
    engine = get_engine(db_conn_string)

    col_types = get_column_types_cached(db_conn_string, table)
    bytea_cols = [c for c, t in col_types.items() if t == "bytea"]
    select_cols = [c for c, t in col_types.items() if t != "bytea"]
    safe_table = validate_identifier(table)
    safe_pk_col = validate_identifier(pk_col)
    select_clause = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"

    # Build WHERE clause
    conditions = []
    params = {"page_size": page_size + 1}  # fetch one extra to detect has_more

    if filter_col and filter_val and col_types.get(filter_col) != "bytea":
        safe_filter_col = validate_identifier(filter_col)
        conditions.append(f'CAST("{safe_filter_col}" AS TEXT) ILIKE :filter_val')
        params["filter_val"] = f"%{filter_val}%"

    if last_pk_val is not None:
        if direction == "next":
            conditions.append(f'"{safe_pk_col}" > :last_pk_val')
        else:
            conditions.append(f'"{safe_pk_col}" < :last_pk_val')
        params["last_pk_val"] = last_pk_val

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # Build ORDER BY
    if sort_col and col_types.get(sort_col) != "bytea":
        safe_sort_col = validate_identifier(sort_col)
        col_type = col_types.get(sort_col, "text")
        cast = SORT_CAST.get(col_type, "")
        sort_direction = "DESC" if sort_dir == "desc" else "ASC"
        order_clause = f'ORDER BY "{safe_sort_col}"{cast} {sort_direction} NULLS LAST, "{safe_pk_col}" {sort_direction}'
    else:
        pk_direction = "DESC" if direction == "prev" else "ASC"
        order_clause = f'ORDER BY "{safe_pk_col}" {pk_direction}'

    query = text(f"""
        SELECT {select_clause}
        FROM "{safe_table}"
        {where_clause}
        {order_clause}
        LIMIT :page_size
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]

    # Detect has_more by fetching one extra row
    has_more = len(rows) > page_size
    if has_more:
        rows = rows[:page_size]

    # If going prev, rows come back in reverse order — flip them
    if direction == "prev":
        rows.reverse()

    # Inject bytea placeholders
    for row in rows:
        for col in bytea_cols:
            row[col] = BYTEA_PLACEHOLDER

    all_columns = list(col_types.keys())
    first_pk = str(rows[0][pk_col]) if rows else None
    last_pk = str(rows[-1][pk_col]) if rows else None

    return all_columns, rows, bytea_cols, has_more, first_pk, last_pk


def get_blob(
    db_conn_string: str,
    table: str,
    pk_col: str,
    pk_val: str,
    column: str,
) -> tuple[bytes, str, str]:
    """
    Fetch a single BYTEA cell. Returns (data, mimetype, extension).
    File type is detected from magic bytes — no filename needed.
    """
    

    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_pk_col = validate_identifier(pk_col)
    safe_column = validate_identifier(column)
    query = text(f'SELECT "{safe_column}" FROM "{safe_table}" WHERE "{safe_pk_col}" = :pk_val')
    with engine.connect() as conn:
        result = conn.execute(query, {"pk_val": pk_val}).scalar()

    if result is None:
        return None, "application/octet-stream", "bin"

    kind = filetype.guess(bytearray(result))
    mime = kind.mime if kind else "application/octet-stream"

    # Map mimetype to a clean file extension
    MIME_TO_EXT = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
        "image/tiff": "tiff",
        "application/pdf": "pdf",
        "application/zip": "zip",
        "application/x-tar": "tar",
        "application/gzip": "gz",
        "application/json": "json",
        "text/plain": "txt",
        "text/csv": "csv",
        "text/html": "html",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/msword": "doc",
        "application/vnd.ms-excel": "xls",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
        "audio/mpeg": "mp3",
        "audio/wav": "wav",
    }

    extension = kind.extension if kind else MIME_TO_EXT.get(mime, "bin")
    return result, mime, extension