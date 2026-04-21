"""
Verify that every db module function raises ValueError before reaching the DB
when given a malicious identifier.  No DB connection required.
"""
import pytest
from db.read        import get_row_count, fetch_all_rows, get_rows, get_rows_keyset, get_blob
from db.edit        import update_value
from db.delete      import delete_row
from db.schema      import (create_table, drop_table, add_column, drop_column,
                             rename_column, change_column_type, clear_table)
from db.bulk_upload import bulk_insert
from db.export      import export_table_csv, export_table_excel

DB    = "postgresql://admin:admin123@127.0.0.1:5433/testdb"
BAD   = "users; DROP TABLE users; --"
BAD_C = "id; SELECT * FROM secrets; --"


# ── read.py ───────────────────────────────────────────────────────────────────

class TestReadBlocked:
    def test_get_row_count_bad_table(self):
        with pytest.raises(ValueError):
            get_row_count(DB, BAD)

    def test_get_row_count_bad_filter_col(self):
        with pytest.raises(ValueError):
            get_row_count(DB, "users", filter_col=BAD_C, filter_val="x")

    def test_get_rows_bad_table(self):
        with pytest.raises(ValueError):
            get_rows(DB, BAD)

    def test_get_rows_bad_sort_col(self):
        with pytest.raises(ValueError):
            get_rows(DB, "users", sort_col=BAD_C)

    def test_get_rows_bad_filter_col(self):
        with pytest.raises(ValueError):
            get_rows(DB, "users", filter_col=BAD_C, filter_val="x")

    def test_get_rows_keyset_bad_table(self):
        with pytest.raises(ValueError):
            get_rows_keyset(DB, BAD, pk_col="id")

    def test_get_rows_keyset_bad_pk_col(self):
        with pytest.raises(ValueError):
            get_rows_keyset(DB, "users", pk_col=BAD_C)

    def test_get_rows_keyset_bad_filter_col(self):
        with pytest.raises(ValueError):
            get_rows_keyset(DB, "users", pk_col="id", filter_col=BAD_C, filter_val="x")

    def test_get_rows_keyset_bad_sort_col(self):
        with pytest.raises(ValueError):
            get_rows_keyset(DB, "users", pk_col="id", sort_col=BAD_C)

    def test_get_blob_bad_table(self):
        with pytest.raises(ValueError):
            get_blob(DB, BAD, "id", "1", "photo")

    def test_get_blob_bad_pk_col(self):
        with pytest.raises(ValueError):
            get_blob(DB, "users", BAD_C, "1", "photo")

    def test_get_blob_bad_column(self):
        with pytest.raises(ValueError):
            get_blob(DB, "users", "id", "1", BAD_C)


# ── edit.py / delete.py ───────────────────────────────────────────────────────

class TestWriteBlocked:
    def test_update_bad_table(self):
        with pytest.raises(ValueError):
            update_value(DB, BAD, 1, "name", "x")

    def test_update_bad_column(self):
        with pytest.raises(ValueError):
            update_value(DB, "users", 1, BAD_C, "x")

    def test_delete_bad_table(self):
        with pytest.raises(ValueError):
            delete_row(DB, BAD, 1)


# ── schema.py ─────────────────────────────────────────────────────────────────

class TestSchemaBlocked:
    def test_create_table_bad_name(self):
        with pytest.raises(ValueError):
            create_table(DB, BAD, [])

    def test_create_table_bad_col_name(self):
        with pytest.raises(ValueError):
            create_table(DB, "safe_table", [{"name": BAD_C, "type": "TEXT", "nullable": True}])

    def test_drop_table_bad_name(self):
        with pytest.raises(ValueError):
            drop_table(DB, BAD)

    def test_add_column_bad_table(self):
        with pytest.raises(ValueError):
            add_column(DB, BAD, "col", "TEXT")

    def test_add_column_bad_col_name(self):
        with pytest.raises(ValueError):
            add_column(DB, "users", BAD_C, "TEXT")

    def test_drop_column_bad_table(self):
        with pytest.raises(ValueError):
            drop_column(DB, BAD, "col")

    def test_drop_column_bad_col(self):
        with pytest.raises(ValueError):
            drop_column(DB, "users", BAD_C)

    def test_rename_column_bad_old(self):
        with pytest.raises(ValueError):
            rename_column(DB, "users", BAD_C, "safe_name")

    def test_rename_column_bad_new(self):
        with pytest.raises(ValueError):
            rename_column(DB, "users", "col", BAD_C)

    def test_change_type_bad_table(self):
        with pytest.raises(ValueError):
            change_column_type(DB, BAD, "col", "TEXT")

    def test_change_type_bad_col(self):
        with pytest.raises(ValueError):
            change_column_type(DB, "users", BAD_C, "TEXT")

    def test_clear_table_bad_name(self):
        with pytest.raises(ValueError):
            clear_table(DB, BAD)


# ── bulk_upload.py ────────────────────────────────────────────────────────────

class TestBulkUploadBlocked:
    def test_bulk_insert_bad_table(self):
        with pytest.raises(ValueError):
            bulk_insert(DB, BAD, ["name"], [["alice"]])

    def test_bulk_insert_bad_header(self):
        with pytest.raises(ValueError):
            bulk_insert(DB, "users", [BAD_C], [["alice"]])


# ── export.py ─────────────────────────────────────────────────────────────────

class TestExportBlocked:
    def test_export_csv_bad_table(self):
        with pytest.raises(ValueError):
            export_table_csv(DB, BAD)

    def test_export_excel_bad_table(self):
        with pytest.raises(ValueError):
            export_table_excel(DB, BAD)
