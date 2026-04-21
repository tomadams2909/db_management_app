"""
Integration tests — require the LOCAL PostgreSQL instance at port 5433.
Run with: pytest -m integration

These tests create a real scratch table, run full CRUD through it, and tear it down.
They verify the whole stack: validate_identifier → SQL → real DB response.
"""
import pytest
from db.read        import fetch_all_rows, get_row_count, get_column_types, get_rows

from db.upload      import add_row
from db.edit        import update_value
from db.delete      import delete_row
from db.schema      import clear_table
from db.bulk_upload import bulk_insert
from db.export      import export_table_csv, export_table_excel
from db.utils       import invalidate_row_count_cache

pytestmark = pytest.mark.integration


# ── helpers ───────────────────────────────────────────────────────────────────

def fresh_count(db_url, table):
    """Row count bypassing cache."""
    invalidate_row_count_cache(db_url, table)
    return get_row_count(db_url, table)

def all_rows(db_url, table):
    col_types = get_column_types(db_url, table)
    _, rows = fetch_all_rows(db_url, table, col_types)
    return rows


# ── CRUD ──────────────────────────────────────────────────────────────────────

class TestCRUD:
    def test_table_starts_empty(self, db_url, test_table):
        assert fresh_count(db_url, test_table) == 0

    def test_add_row(self, db_url, test_table):
        add_row(db_url, test_table, {"name": "Alice", "score": 100})
        assert fresh_count(db_url, test_table) == 1

    def test_fetch_row_values(self, db_url, test_table):
        rows = all_rows(db_url, test_table)
        assert rows[0]["name"] == "Alice"
        assert rows[0]["score"] == 100

    def test_update_value(self, db_url, test_table):
        row_id = all_rows(db_url, test_table)[0]["id"]
        update_value(db_url, test_table, row_id, "name", "Alice Updated", pk_col="id")
        rows = all_rows(db_url, test_table)
        assert rows[0]["name"] == "Alice Updated"

    def test_update_returns_rowcount(self, db_url, test_table):
        row_id = all_rows(db_url, test_table)[0]["id"]
        affected = update_value(db_url, test_table, row_id, "score", 99, pk_col="id")
        assert affected == 1

    def test_delete_row(self, db_url, test_table):
        row_id = all_rows(db_url, test_table)[0]["id"]
        delete_row(db_url, test_table, row_id, pk_col="id")
        assert fresh_count(db_url, test_table) == 0


# ── Bulk insert ───────────────────────────────────────────────────────────────

class TestBulkInsert:
    def test_inserts_all_rows(self, db_url, test_table):
        clear_table(db_url, test_table)
        headers = ["name", "score"]
        rows    = [["Bob", "95"], ["Carol", "87"], ["Dave", "72"]]
        result  = bulk_insert(db_url, test_table, headers, rows)
        assert result["inserted"] == 3
        assert result["skipped"]  == 0
        assert fresh_count(db_url, test_table) == 3

    def test_skips_blank_rows(self, db_url, test_table):
        clear_table(db_url, test_table)
        headers = ["name", "score"]
        rows    = [["Eve", "88"], ["", ""], ["Frank", "91"]]
        result  = bulk_insert(db_url, test_table, headers, rows)
        assert result["inserted"] == 2
        assert result["skipped"]  == 1
        assert fresh_count(db_url, test_table) == 2

    def test_strips_pk_column(self, db_url, test_table):
        """Bulk insert must silently drop the PK column so the DB auto-assigns it."""
        clear_table(db_url, test_table)
        headers = ["id", "name", "score"]
        rows    = [["999", "Grace", "80"]]
        result  = bulk_insert(db_url, test_table, headers, rows, pk_col="id")
        assert result["inserted"] == 1
        inserted = all_rows(db_url, test_table)
        # DB assigned its own id — not the 999 we tried to force
        assert inserted[0]["id"] != 999
        assert inserted[0]["name"] == "Grace"

    def test_rollback_on_failure(self, db_url, test_table):
        """A bad row mid-upload must roll back all rows, not leave a partial insert."""
        clear_table(db_url, test_table)
        headers = ["name", "score"]
        # Third row has a non-integer score — will fail the INTEGER column constraint
        rows = [["Harry", "85"], ["Iris", "90"], ["Bad", "not_a_number"]]
        with pytest.raises(RuntimeError, match="Row 3 failed"):
            bulk_insert(db_url, test_table, headers, rows)
        # Transaction rolled back — table must still be empty
        assert fresh_count(db_url, test_table) == 0


# ── Pagination ────────────────────────────────────────────────────────────────

class TestPagination:
    def test_page_size_respected(self, db_url, test_table):
        clear_table(db_url, test_table)
        headers = ["name", "score"]
        rows    = [[f"User{i}", str(i)] for i in range(10)]
        bulk_insert(db_url, test_table, headers, rows)

        col_types = get_column_types(db_url, test_table)
        _, page_rows, _ = get_rows(db_url, test_table, page=1, page_size=3)
        assert len(page_rows) == 3

    def test_page_two_is_different(self, db_url, test_table):
        col_types = get_column_types(db_url, test_table)
        _, page1, _ = get_rows(db_url, test_table, page=1, page_size=5)
        _, page2, _ = get_rows(db_url, test_table, page=2, page_size=5)
        ids_p1 = {r["id"] for r in page1}
        ids_p2 = {r["id"] for r in page2}
        assert ids_p1.isdisjoint(ids_p2)

    def test_filter_narrows_results(self, db_url, test_table):
        col_types = get_column_types(db_url, test_table)
        _, rows, _ = get_rows(db_url, test_table, filter_col="name", filter_val="User1")
        names = [r["name"] for r in rows]
        assert all("User1" in n for n in names)


# ── Export ────────────────────────────────────────────────────────────────────

class TestExport:
    def test_csv_contains_headers(self, db_url, test_table):
        csv_bytes, _ = export_table_csv(db_url, test_table)
        assert b"name" in csv_bytes
        assert b"score" in csv_bytes

    def test_csv_row_count_matches(self, db_url, test_table):
        invalidate_row_count_cache(db_url, test_table)
        db_count = get_row_count(db_url, test_table)
        _, export_count = export_table_csv(db_url, test_table)
        assert export_count == db_count

    def test_excel_returns_bytes(self, db_url, test_table):
        xlsx_bytes, row_count = export_table_excel(db_url, test_table)
        # XLSX files start with PK (zip magic bytes)
        assert xlsx_bytes[:2] == b"PK"
        assert row_count > 0
