from sqlalchemy import text
from .utils import get_engine

def add_row(
    db_conn_string: str,
    table: str,
    data: dict
) -> int:
    engine = get_engine(db_conn_string)

    cols = ", ".join(data.keys())
    params = ", ".join(f":{k}" for k in data.keys())

    query = text(f"""
        INSERT INTO {table} ({cols})
        VALUES ({params})
    """)

    with engine.begin() as conn:
        result = conn.execute(query, data)

    return result.rowcount