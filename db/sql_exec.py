from sqlalchemy import text
from .utils import get_engine


def execute_sql(db_conn_string: str, sql: str) -> dict:
    """
    Execute raw SQL. Returns dict with keys:
      - columns: list of column names (SELECT) or []
      - rows: list of dicts (SELECT) or []
      - rowcount: int (INSERT/UPDATE/DELETE) or None
      - is_select: bool
    """
    engine = get_engine(db_conn_string)
    sql = sql.strip()
    is_select = sql.upper().startswith("SELECT") or sql.upper().startswith("WITH")

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        if is_select:
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            return {"columns": columns, "rows": rows, "rowcount": None, "is_select": True}
        else:
            return {"columns": [], "rows": [], "rowcount": result.rowcount, "is_select": False}
