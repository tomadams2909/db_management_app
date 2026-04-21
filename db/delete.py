from sqlalchemy import text
from .utils import get_engine, validate_identifier

def delete_row(
    db_conn_string: str,
    table: str,
    row_id,
    pk_col: str = "id",
) -> int:
    engine = get_engine(db_conn_string)
    safe_table  = validate_identifier(table)
    safe_pk_col = validate_identifier(pk_col)

    query = text(f"""
        DELETE FROM "{safe_table}"
        WHERE "{safe_pk_col}" = :pk_val
    """)

    with engine.begin() as conn:
        result = conn.execute(query, {"pk_val": row_id})

    return result.rowcount
