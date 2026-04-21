from sqlalchemy import text
from .utils import get_engine, validate_identifier

def update_value(
    db_conn_string: str,
    table: str,
    row_id: int,
    column: str,
    new_value
) -> int:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_column = validate_identifier(column)

    query = text(f"""
        UPDATE "{safe_table}"
        SET "{safe_column}" = :value
        WHERE id = :id
    """)

    with engine.begin() as conn:
        result = conn.execute(
            query,
            {"value": new_value, "id": row_id}
        )

    return result.rowcount
