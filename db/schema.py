from sqlalchemy import text
from .utils import get_engine, invalidate_column_cache, invalidate_table_columns_cache, validate_identifier


def create_table(db_conn_string: str, table: str, columns: list[dict],
                 pk_mode: str = "serial",
                 pk_custom_name: str = "id",
                 pk_custom_type: str = "") -> None:
    """
    pk_mode:
      'serial'  -> id SERIAL PRIMARY KEY  (default)
      'uuid'    -> id UUID PRIMARY KEY DEFAULT gen_random_uuid()
      'custom'  -> <pk_custom_name> <pk_custom_type> PRIMARY KEY
      'none'    -> no primary key column injected
    columns: list of {name, type, nullable}
    """
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)

    col_defs = []
    if pk_mode == "serial":
        col_defs.append("id SERIAL PRIMARY KEY")
    elif pk_mode == "uuid":
        col_defs.append("id UUID PRIMARY KEY DEFAULT gen_random_uuid()")
    elif pk_mode == "custom" and pk_custom_name and pk_custom_type:
        safe_pk_name = validate_identifier(pk_custom_name)
        col_defs.append(f'"{safe_pk_name}" {pk_custom_type} PRIMARY KEY')
    # pk_mode == 'none' → skip

    for col in columns:
        safe_col_name = validate_identifier(col['name'])
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        col_defs.append(f'"{safe_col_name}" {col["type"]}{nullable}')

    ddl = f'CREATE TABLE "{safe_table}" ({", ".join(col_defs)})'
    with engine.begin() as conn:
        conn.execute(text(ddl))


def drop_table(db_conn_string: str, table: str) -> None:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE "{safe_table}"'))
    invalidate_column_cache(db_conn_string, table)
    invalidate_table_columns_cache(db_conn_string, table)


def add_column(db_conn_string: str, table: str, column: str, col_type: str, nullable: bool = True) -> None:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_column = validate_identifier(column)
    null_clause = "" if nullable else " NOT NULL DEFAULT ''"
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{safe_table}" ADD COLUMN "{safe_column}" {col_type}{null_clause}'))
    invalidate_column_cache(db_conn_string, table)
    invalidate_table_columns_cache(db_conn_string, table)


def drop_column(db_conn_string: str, table: str, column: str) -> None:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_column = validate_identifier(column)
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{safe_table}" DROP COLUMN "{safe_column}"'))
    invalidate_column_cache(db_conn_string, table)
    invalidate_table_columns_cache(db_conn_string, table)


def rename_column(db_conn_string: str, table: str, old_name: str, new_name: str) -> None:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_old = validate_identifier(old_name)
    safe_new = validate_identifier(new_name)
    with engine.begin() as conn:
        conn.execute(text(f'ALTER TABLE "{safe_table}" RENAME COLUMN "{safe_old}" TO "{safe_new}"'))
    invalidate_column_cache(db_conn_string, table)
    invalidate_table_columns_cache(db_conn_string, table)


def change_column_type(db_conn_string: str, table: str, column: str, new_type: str) -> None:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    safe_column = validate_identifier(column)
    with engine.begin() as conn:
        conn.execute(text(
            f'ALTER TABLE "{safe_table}" ALTER COLUMN "{safe_column}" TYPE {new_type} USING "{safe_column}"::{new_type}'
        ))
    invalidate_column_cache(db_conn_string, table)
    invalidate_table_columns_cache(db_conn_string, table)


def clear_table(db_conn_string: str, table: str) -> int:
    engine = get_engine(db_conn_string)
    safe_table = validate_identifier(table)
    with engine.begin() as conn:
        result = conn.execute(text(f'DELETE FROM "{safe_table}"'))
    return result.rowcount
