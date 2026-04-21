import io
import csv
from .utils import get_engine, get_column_types_cached, validate_identifier
from sqlalchemy import text

EXCEL_ROW_LIMIT = 1_048_576


def export_table_csv(db_conn_string: str, table: str) -> tuple[bytes, int]:
    """Returns (csv_bytes, row_count). BYTEA columns are excluded."""
    engine = get_engine(db_conn_string)

    col_types = get_column_types_cached(db_conn_string, table)
    select_cols = [c for c, t in col_types.items() if t != "bytea"]
    select_clause = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"

    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT {select_clause} FROM "{validate_identifier(table)}" ORDER BY 1'))
        columns = list(result.keys())
        rows = result.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8"), len(rows)


def export_table_excel(db_conn_string: str, table: str) -> tuple[bytes, int]:
    """Returns (xlsx_bytes, row_count). BYTEA columns are excluded."""
    import openpyxl

    engine = get_engine(db_conn_string)

    col_types = get_column_types_cached(db_conn_string, table)
    select_cols = [c for c, t in col_types.items() if t != "bytea"]
    select_clause = ", ".join(f'"{c}"' for c in select_cols) if select_cols else "1"

    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT {select_clause} FROM "{validate_identifier(table)}" ORDER BY 1'))
        columns = list(result.keys())
        rows = result.fetchall()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = table[:31]
    ws.append(columns)
    for row in rows:
        ws.append([str(v) if v is not None else "" for v in row])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read(), len(rows)