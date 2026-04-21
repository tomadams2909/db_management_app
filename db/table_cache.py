from datetime import datetime

_cache: dict[tuple, dict] = {}


def is_cached(db_url: str, table: str) -> bool:
    return (db_url, table) in _cache


def get_cached(db_url: str, table: str) -> dict | None:
    return _cache.get((db_url, table))


def store_cache(db_url: str, table: str, rows: list[dict], col_types: dict) -> None:
    _cache[(db_url, table)] = {
        "rows": rows,
        "col_types": col_types,
        "cached_at": datetime.now().strftime("%H:%M:%S"),
    }


def invalidate_table_cache(db_url: str, table: str) -> None:
    _cache.pop((db_url, table), None)


# ── Python-side filter ────────────────────────────────────────────────────────

def filter_rows(rows: list[dict], filter_col: str, filter_val: str) -> list[dict]:
    """Case-insensitive substring filter on a single column — mirrors SQL ILIKE."""
    if not filter_col or not filter_val:
        return rows
    needle = filter_val.lower()
    return [
        row for row in rows
        if needle in str(row.get(filter_col, "") or "").lower()
    ]


# ── Python-side sort ──────────────────────────────────────────────────────────

# Types that should sort as numbers
_NUMERIC_TYPES = {
    "integer", "bigint", "smallint", "numeric",
    "decimal", "real", "double precision",
}

# Types that should sort as dates
_DATE_TYPES = {
    "date", "timestamp without time zone", "timestamp with time zone",
}


def _sort_key(col: str, col_types: dict):
    """Return a sort key function for a given column, type-aware."""
    col_type = col_types.get(col, "text")

    if col_type in _NUMERIC_TYPES:
        def key(row):
            v = row.get(col)
            if v is None or v == "":
                return (1, 0)
            try:
                return (0, float(v))
            except (ValueError, TypeError):
                return (1, 0)
    elif col_type in _DATE_TYPES:
        def key(row):
            v = row.get(col)
            if v is None or v == "":
                return (1, "")
            return (0, str(v))
    else:
        def key(row):
            v = row.get(col)
            if v is None or v == "":
                return (1, "")
            return (0, str(v).lower())

    return key


def sort_rows(
    rows: list[dict],
    sort_col: str,
    sort_dir: str,
    col_types: dict,
) -> list[dict]:
    """Sort rows in Python, type-aware, NULLs last."""
    if not sort_col or sort_col not in col_types:
        return rows
    return sorted(
        rows,
        key=_sort_key(sort_col, col_types),
        reverse=(sort_dir == "desc"),
    )


# ── Python-side paginate ──────────────────────────────────────────────────────

def paginate_rows(
    rows: list[dict],
    page: int,
    page_size: int,
) -> list[dict]:
    """Return the correct slice of rows for the requested page."""
    offset = (page - 1) * page_size
    return rows[offset: offset + page_size]