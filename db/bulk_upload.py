import io
import csv
from .utils import get_engine, get_table_columns, validate_row_data, validate_identifier
from sqlalchemy import text


def parse_upload_file(file_bytes: bytes, filename: str) -> tuple[list[str], list[list]]:
    """Parse CSV or Excel file. Returns (headers, rows). Unchanged — preview shows all columns."""
    if filename.lower().endswith(".csv"):
        content = file_bytes.decode("utf-8-sig")  # handle BOM
        reader = csv.reader(io.StringIO(content))
        rows = list(reader)
        if not rows:
            return [], []
        return rows[0], rows[1:]
    else:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            return [], []
        headers = [str(h) if h is not None else "" for h in all_rows[0]]
        data_rows = [[str(c) if c is not None else "" for c in row] for row in all_rows[1:]]
        return headers, data_rows


def bulk_insert(
    db_conn_string: str,
    table: str,
    headers: list[str],
    rows: list[list],
    pk_col: str = "id",
) -> int:
    """
    Insert rows into table. Returns inserted count.
    Strips the pk_col from headers/rows before inserting so the DB
    can auto-assign it — prevents duplicate key errors on upload.
    """
    if not rows:
        return 0

    # Strip PK column from headers and all rows if present
    if pk_col and pk_col in headers:
        pk_index = headers.index(pk_col)
        headers = [h for i, h in enumerate(headers) if i != pk_index]
        rows = [
            [v for i, v in enumerate(row) if i != pk_index]
            for row in rows
        ]

    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_headers = [validate_identifier(h) for h in headers]
    cols = ", ".join(f'"{h}"' for h in safe_headers)
    params = ", ".join(f":col_{i}" for i in range(len(safe_headers)))
    query = text(f'INSERT INTO "{safe_table}" ({cols}) VALUES ({params})')

    inserted = 0
    with engine.begin() as conn:
        for row in rows:
            if all(v == "" or v is None for v in row):
                continue  # skip blank rows
            row_dict = {f"col_{i}": (v if v != "" else None) for i, v in enumerate(row)}
            conn.execute(query, row_dict)
            inserted += 1
    return inserted