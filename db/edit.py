from sqlalchemy import text
from .utils import get_engine, validate_identifier

def update_value(
    db_conn_string: str,
    table: str,
    row_id,
    column: str,
    new_value,
    pk_col: str = "id",
) -> int:
    engine = get_engine(db_conn_string)
    safe_table  = validate_identifier(table)
    safe_column = validate_identifier(column)
    safe_pk_col = validate_identifier(pk_col)

    query = text(f"""
        UPDATE "{safe_table}"
        SET "{safe_column}" = :value
        WHERE "{safe_pk_col}" = :pk_val
    """)

    with engine.begin() as conn:
        result = conn.execute(query, {"value": new_value, "pk_val": row_id})

    return result.rowcount
