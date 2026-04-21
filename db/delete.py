from sqlalchemy import text
from .utils import get_engine, validate_identifier

def delete_row(
    db_conn_string: str,
    table: str,
    row_id: int
) -> int:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)

    query = text(f"""
        DELETE FROM "{safe_table}"
        WHERE id = :id
    """)

    with engine.begin() as conn:
        result = conn.execute(query, {"id": row_id})

    return result.rowcount
