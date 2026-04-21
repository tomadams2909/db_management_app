from db import (
    update_table_value, delete_row, add_row, validate_row_data, get_table_columns,
    get_tables, get_rows, get_row_count, get_column_types,
    create_table, drop_table, add_column, drop_column, rename_column, change_column_type, clear_table,
    export_table_csv, export_table_excel, EXCEL_ROW_LIMIT,
    parse_upload_file, bulk_insert,
    execute_sql,
    get_blob,
    fetch_all_rows,
    get_rows_keyset,
    CACHE_ROW_THRESHOLD,
    is_cached, get_cached, store_cache,
    invalidate_table_cache,
    invalidate_column_cache, invalidate_table_columns_cache,
    invalidate_tables_list_cache, invalidate_row_count_cache,
    filter_rows, sort_rows, paginate_rows,
    get_db_overview,
)
from config.databases import Database


class DatabaseAPI:

    # ── Read ──────────────────────────────────────────────────────────────────

    def list_tables(self, database: Database) -> list[str]:
        return get_tables(database.url)

    def get_db_overview(self, database: Database) -> dict:
        return get_db_overview(database.url)

    def browse_table(self, database: Database, table: str, page: int = 1,
                     page_size: int = 100, filter_col: str = None, filter_val: str = None,
                     sort_col: str = None, sort_dir: str = "asc",
                     pk_col: str = None, last_pk_val: str = None,
                     direction: str = "next") -> dict:

        total = get_row_count(database.url, table)
        col_types = get_column_types(database.url, table)
        is_large = total > CACHE_ROW_THRESHOLD

        # ── Large table: keyset pagination ────────────────────────────────────
        if is_large:
            if not pk_col:
                columns, rows, bytea_cols = get_rows(
                    database.url, table, page, page_size,
                    filter_col, filter_val, sort_col, sort_dir
                )
                return {
                    "columns": columns,
                    "rows": rows,
                    "bytea_cols": bytea_cols,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": max(1, -(-total // page_size)),
                    "col_types": col_types,
                    "is_large": True,
                    "is_cached": False,
                    "has_more": False,
                    "first_pk": None,
                    "last_pk": None,
                }

            columns, rows, bytea_cols, has_more, first_pk, last_pk = get_rows_keyset(
                database.url, table,
                pk_col=pk_col,
                last_pk_val=last_pk_val,
                direction=direction,
                page_size=page_size,
                filter_col=filter_col,
                filter_val=filter_val,
                sort_col=sort_col,
                sort_dir=sort_dir,
            )
            return {
                "columns": columns,
                "rows": rows,
                "bytea_cols": bytea_cols,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": None,
                "col_types": col_types,
                "is_large": True,
                "is_cached": False,
                "has_more": has_more,
                "first_pk": first_pk,
                "last_pk": last_pk,
            }

        # ── Small table: populate cache if needed ─────────────────────────────
        if not is_cached(database.url, table):
            all_columns, all_rows = fetch_all_rows(database.url, table, col_types)
            print(f"[CACHE] fetch_all_rows returned {len(all_rows) if all_rows is not None else 'None'} rows")
            store_cache(database.url, table, all_rows or [], col_types)

        cached = get_cached(database.url, table)
        print(f"[CACHE] get_cached returned keys={list(cached.keys()) if cached else None}, rows={len(cached['rows']) if cached and cached.get('rows') is not None else 'None'}")
        all_rows = cached["rows"] if cached else []
        cached_at = cached.get("cached_at", "") if cached else ""

        # Apply filter
        if filter_col and filter_val:
            filtered = filter_rows(all_rows, filter_col, filter_val)
        else:
            filtered = all_rows

        # Apply sort
        if sort_col:
            filtered = sort_rows(filtered, sort_col, sort_dir, col_types)

        # Paginate
        total_filtered = len(filtered)
        page_rows = paginate_rows(filtered, page, page_size)
        bytea_cols = [c for c, t in col_types.items() if t == "bytea"]

        return {
            "columns": list(col_types.keys()),
            "rows": page_rows,
            "bytea_cols": bytea_cols,
            "total": total_filtered,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total_filtered // page_size)),
            "col_types": col_types,
            "is_large": False,
            "is_cached": True,
            "cached_at": cached_at,
            "has_more": None,
            "first_pk": None,
            "last_pk": None,
        }

    def refresh_table_cache(self, database: Database, table: str) -> None:
        """Force re-fetch of a table into cache."""
        col_types = get_column_types(database.url, table)
        all_columns, all_rows = fetch_all_rows(database.url, table, col_types)
        store_cache(database.url, table, all_rows or [], col_types)

    def get_table_columns(self, database: Database, table: str) -> list[dict]:
        return get_table_columns(database.url, table)

    def get_blob(self, database: Database, table: str, pk_col: str, pk_val: str, column: str) -> tuple[bytes, str, str]:
        return get_blob(database.url, table, pk_col, pk_val, column)

    # ── Row Write ─────────────────────────────────────────────────────────────

    def update_value(self, database: Database, table: str, row_id, column: str, new_value, pk_col: str = "id"):
        update_table_value(database.url, table, row_id, column, new_value, pk_col=pk_col)
        self._post_write_cache(database, table)

    def delete_row(self, database: Database, table: str, row_id, pk_col: str = "id"):
        result = delete_row(database.url, table, row_id, pk_col=pk_col)
        self._post_write_cache(database, table)
        return result

    def add_row(self, database: Database, table: str, form_data: dict):
        columns = get_table_columns(database.url, table)
        data = validate_row_data(columns, form_data)
        result = add_row(database.url, table, data)
        self._post_write_cache(database, table)
        return result

    def _post_write_cache(self, database: Database, table: str) -> None:
        """After any write, invalidate and re-fetch cache if table is small enough."""
        invalidate_row_count_cache(database.url, table)
        total = get_row_count(database.url, table)
        if total <= CACHE_ROW_THRESHOLD:
            col_types = get_column_types(database.url, table)
            all_columns, all_rows = fetch_all_rows(database.url, table, col_types)
            store_cache(database.url, table, all_rows or [], col_types)
        else:
            invalidate_table_cache(database.url, table)

    # ── Schema Operations ─────────────────────────────────────────────────────

    def create_table(self, database: Database, table: str, columns: list[dict],
                     pk_mode: str = "serial", pk_custom_name: str = "id", pk_custom_type: str = ""):
        create_table(database.url, table, columns,
                     pk_mode=pk_mode, pk_custom_name=pk_custom_name, pk_custom_type=pk_custom_type)
        invalidate_tables_list_cache(database.url)

    def drop_table(self, database: Database, table: str):
        drop_table(database.url, table)
        invalidate_table_cache(database.url, table)
        invalidate_table_columns_cache(database.url, table)
        invalidate_column_cache(database.url, table)
        invalidate_row_count_cache(database.url, table)
        invalidate_tables_list_cache(database.url)

    def add_column(self, database: Database, table: str, column: str, col_type: str, nullable: bool = True):
        add_column(database.url, table, column, col_type, nullable)
        invalidate_table_cache(database.url, table)
        invalidate_table_columns_cache(database.url, table)
        invalidate_column_cache(database.url, table)

    def drop_column(self, database: Database, table: str, column: str):
        drop_column(database.url, table, column)
        invalidate_table_cache(database.url, table)
        invalidate_table_columns_cache(database.url, table)
        invalidate_column_cache(database.url, table)

    def rename_column(self, database: Database, table: str, old_name: str, new_name: str):
        rename_column(database.url, table, old_name, new_name)
        invalidate_table_cache(database.url, table)
        invalidate_table_columns_cache(database.url, table)
        invalidate_column_cache(database.url, table)

    def change_column_type(self, database: Database, table: str, column: str, new_type: str):
        change_column_type(database.url, table, column, new_type)
        invalidate_table_cache(database.url, table)
        invalidate_table_columns_cache(database.url, table)
        invalidate_column_cache(database.url, table)

    def clear_table(self, database: Database, table: str) -> int:
        result = clear_table(database.url, table)
        invalidate_table_cache(database.url, table)
        invalidate_row_count_cache(database.url, table)
        return result

    # ── Export ────────────────────────────────────────────────────────────────

    def export_csv(self, database: Database, table: str) -> tuple[bytes, int]:
        return export_table_csv(database.url, table)

    def export_excel(self, database: Database, table: str) -> tuple[bytes, int]:
        return export_table_excel(database.url, table)

    def get_row_count(self, database: Database, table: str) -> int:
        return get_row_count(database.url, table)

    # ── Bulk Upload ───────────────────────────────────────────────────────────

    def parse_upload(self, file_bytes: bytes, filename: str):
        return parse_upload_file(file_bytes, filename)

    def bulk_insert(self, database: Database, table: str, headers: list[str],
                    rows: list[list], pk_col: str = "id") -> dict:
        result = bulk_insert(database.url, table, headers, rows, pk_col=pk_col)
        self._post_write_cache(database, table)
        return result

    # ── Raw SQL ───────────────────────────────────────────────────────────────

    def execute_sql(self, database: Database, sql: str) -> dict:
        return execute_sql(database.url, sql)