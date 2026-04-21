from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
from api import DatabaseAPI
from config.databases import Database
from config.settings import get_env
from db import EXCEL_ROW_LIMIT, get_blob, invalidate_table_cache
import io
import json
import base64
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__, static_folder="style", static_url_path="/static")
app.secret_key = get_env("FLASK_SECRET_KEY")

db_api = DatabaseAPI()
PAGE_SIZE = 100


def get_db(name: str) -> Database:
    return Database[name]

def db_names():
    return [db.name for db in Database]\

def get_tables_safe(database):
    try:
        return db_api.list_tables(database)
    except Exception:
        return []


# ── Browse: overview (no table selected) ─────────────────────────────────────

@app.route("/")
@app.route("/browse")
def browse():
    import json as _json
    dbs = db_names()
    selected_db_name = request.args.get("database", dbs[0])
    database = get_db(selected_db_name)

    overview, tables, error = {}, [], None
    try:
        overview = db_api.get_db_overview(database)
        tables   = [t["table"] for t in overview.get("tables", [])]
    except Exception as e:
        error = str(e)

    # Serialise chart data for JS
    chart_data = {}
    if overview:
        t_list = overview["tables"]
        chart_data = {
            "labels":      [t["table"] for t in t_list],
            "rows":        [t["rows"]       for t in t_list],
            "size_bytes":  [t["size_bytes"] for t in t_list],
            "size_pretty": [t["size_pretty"] for t in t_list],
            "col_counts":  [t["col_count"]  for t in t_list],
        }

    return render_template(
        "browse.html",
        db_names=dbs,
        selected_db=selected_db_name,
        tables=tables,
        overview=overview,
        chart_data=_json.dumps(chart_data),
        error=error,
    )


# ── Browse: table view ────────────────────────────────────────────────────────

@app.route("/browse/<db_name>/<table>")
def browse_table(db_name, table):
    import time
    t0 = time.time()

    database = get_db(db_name)
    page       = int(request.args.get("page", 1))
    filter_col = request.args.get("filter_col") or None
    filter_val = request.args.get("filter_val") or None
    sort_col   = request.args.get("sort_col") or None
    sort_dir   = request.args.get("sort_dir", "asc")
    last_pk_val = request.args.get("last_pk_val") or None
    direction   = request.args.get("direction", "next")

    # ── Run sidebar + data + columns in parallel ──────────────────────────────
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_tables = executor.submit(get_tables_safe, database)
        future_cols   = executor.submit(db_api.get_table_columns, database, table)

        table_cols = future_cols.result()
        t1 = time.time()
        print(f"[TIMING] get_table_columns: {t1-t0:.3f}s")
        session_key = f"pk_col_{db_name}_{table}"
        all_col_names = [c["column_name"] for c in table_cols]

        pk_col = session.get(session_key)
        if not pk_col:
            if "id" in all_col_names:
                pk_col = "id"
            elif "uid" in all_col_names:
                pk_col = "uid"
            else:
                pk_col = None
            if pk_col:
                session[session_key] = pk_col

        future_data = executor.submit(
            db_api.browse_table,
            database=database, table=table, page=page,
            page_size=PAGE_SIZE, filter_col=filter_col,
            filter_val=filter_val, sort_col=sort_col, sort_dir=sort_dir,
            pk_col=pk_col, last_pk_val=last_pk_val, direction=direction
        )
        tables = future_tables.result()
        t2 = time.time()
        print(f"[TIMING] get_tables (parallel): {t2-t0:.3f}s")

    data, error = {}, None
    try:
        data = future_data.result()
    except Exception as e:
        error = str(e)
    t3 = time.time()
    print(f"[TIMING] browse_table data: {t3-t2:.3f}s")
    print(f"[TIMING] is_cached={data.get('is_cached')} | is_large={data.get('is_large')}")
    print(f"[TIMING] total: {t3-t0:.3f}s")
    print(f"---")

    flash_msg  = request.args.get("msg")
    flash_type = request.args.get("msg_type", "ok")

    return render_template("table_view.html", db_names=db_names(), selected_db=db_name,
                           tables=tables, table=table, data=data,
                           filter_col=filter_col, filter_val=filter_val,
                           sort_col=sort_col, sort_dir=sort_dir, error=error,
                           table_cols=table_cols, flash_msg=flash_msg, flash_type=flash_type,
                           excel_limit=EXCEL_ROW_LIMIT,
                           pk_col=pk_col, all_col_names=all_col_names)


# ── PK column selector ────────────────────────────────────────────────────────

@app.route("/api/set-pk/<db_name>/<table>", methods=["POST"])
def set_pk(db_name, table):
    pk_col = request.json.get("pk_col")
    session_key = f"pk_col_{db_name}_{table}"
    if pk_col:
        session[session_key] = pk_col
    else:
        session.pop(session_key, None)
    return jsonify({"ok": True, "pk_col": pk_col})


# ── Cache refresh ─────────────────────────────────────────────────────────────

@app.route("/api/refresh-cache/<db_name>/<table>")
def refresh_cache(db_name, table):
    try:
        database = get_db(db_name)
        db_api.refresh_table_cache(database, table)
    except Exception as e:
        return redirect(url_for("browse_table", db_name=db_name, table=table,
                                msg=f"❌ Refresh failed: {e}", msg_type="err"))
    return redirect(url_for("browse_table", db_name=db_name, table=table,
                            msg="✅ Cache refreshed", msg_type="ok"))


# ── Row operations ────────────────────────────────────────────────────────────

@app.route("/update-value", methods=["POST"])
def update_value():
    try:
        database = get_db(request.form["database"])
        table    = request.form["table"]
        pk_col   = session.get(f"pk_col_{request.form['database']}_{table}", "id")
        db_api.update_value(database=database, table=table,
                            row_id=request.form["row_id"],
                            column=request.form["column"],
                            new_value=request.form["new_value"],
                            pk_col=pk_col)
        msg, msg_type = "✅ Cell updated successfully", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"

    return redirect(url_for("browse_table", db_name=request.form["redirect_db"],
                            table=request.form["redirect_table"],
                            page=request.form.get("redirect_page", 1),
                            filter_col=request.form.get("filter_col") or "",
                            filter_val=request.form.get("filter_val") or "",
                            sort_col=request.form.get("sort_col") or "",
                            sort_dir=request.form.get("sort_dir", "asc"),
                            msg=msg, msg_type=msg_type))


@app.route("/delete-row", methods=["POST"])
def delete_row_route():
    try:
        database = get_db(request.form["database"])
        table    = request.form["table"]
        pk_col   = session.get(f"pk_col_{request.form['database']}_{table}", "id")
        db_api.delete_row(database=database, table=table,
                          row_id=request.form["row_id"],
                          pk_col=pk_col)
        msg, msg_type = "✅ Row deleted", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"

    return redirect(url_for("browse_table", db_name=request.form["redirect_db"],
                            table=request.form["redirect_table"],
                            page=request.form.get("redirect_page", 1),
                            msg=msg, msg_type=msg_type))


@app.route("/add-row", methods=["POST"])
def add_row_route():
    try:
        database = get_db(request.form["database"])
        table = request.form["table"]
        cols = db_api.get_table_columns(database, table)
        form_data = {
            col["column_name"]: request.form.get(col["column_name"])
            for col in cols
            if not (col["column_name"] == "id" and col.get("column_default")
                    and "nextval" in col["column_default"])
        }
        db_api.add_row(database=database, table=table, form_data=form_data)
        msg, msg_type = "✅ Row added successfully", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"

    return redirect(url_for("browse_table", db_name=request.form["redirect_db"],
                            table=request.form["redirect_table"],
                            page=request.form.get("redirect_page", 1),
                            msg=msg, msg_type=msg_type))


# ── Blob download ─────────────────────────────────────────────────────────────

@app.route("/api/blob/<db_name>/<table>/<pk_col>/<pk_val>/<column>")
def download_blob(db_name, table, pk_col, pk_val, column):
    try:
        database = get_db(db_name)
        data, mimetype, extension = db_api.get_blob(
            database=database, table=table,
            pk_col=pk_col, pk_val=pk_val, column=column
        )
        if data is None:
            return "No data found", 404
        return send_file(
            io.BytesIO(data),
            mimetype=mimetype,
            as_attachment=True,
            download_name=f"{table}_{column}_{pk_val}.{extension}"
        )
    except Exception as e:
        return str(e), 500


# ── Export ────────────────────────────────────────────────────────────────────

@app.route("/export/<db_name>/<table>/<fmt>")
def export_table(db_name, table, fmt):
    database = get_db(db_name)
    try:
        if fmt == "csv":
            data, count = db_api.export_csv(database, table)
            return send_file(io.BytesIO(data), mimetype="text/csv",
                             as_attachment=True, download_name=f"{table}.csv")
        elif fmt == "excel":
            data, count = db_api.export_excel(database, table)
            return send_file(io.BytesIO(data),
                             mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             as_attachment=True, download_name=f"{table}.xlsx")
    except Exception as e:
        return redirect(url_for("browse_table", db_name=db_name, table=table,
                                msg=f"❌ Export failed: {e}", msg_type="err"))


# ── Upload ────────────────────────────────────────────────────────────────────

@app.route("/upload/<db_name>/<table>", methods=["POST"])
def upload_table(db_name, table):
    database = get_db(db_name)
    file = request.files.get("upload_file")
    if not file or file.filename == "":
        return redirect(url_for("browse_table", db_name=db_name, table=table,
                                msg="❌ No file selected", msg_type="err"))
    try:
        file_bytes = file.read()
        headers, rows = db_api.parse_upload(file_bytes, file.filename)
        preview_rows = rows[:5]
        rows_b64 = base64.b64encode(json.dumps(rows).encode()).decode()
        headers_b64 = base64.b64encode(json.dumps(headers).encode()).decode()
        return render_template("upload_preview.html", db_names=db_names(), selected_db=db_name,
                               table=table, headers=headers, preview_rows=preview_rows,
                               rows_b64=rows_b64, headers_b64=headers_b64, total=len(rows))
    except Exception as e:
        return redirect(url_for("browse_table", db_name=db_name, table=table,
                                msg=f"❌ Upload parse failed: {e}", msg_type="err"))


@app.route("/upload-confirm/<db_name>/<table>", methods=["POST"])
def upload_confirm(db_name, table):
    database = get_db(db_name)
    try:
        headers = json.loads(base64.b64decode(request.form["headers_b64"]).decode())
        rows    = json.loads(base64.b64decode(request.form["rows_b64"]).decode())
        pk_col  = session.get(f"pk_col_{db_name}_{table}", "id")
        result = db_api.bulk_insert(database=database, table=table,
                                     headers=headers, rows=rows, pk_col=pk_col)
        msg, msg_type = (
            f"✅ {result['inserted']} rows inserted"
            + (f", {result['skipped']} blank rows skipped" if result['skipped'] else ""),
            "ok",
        )
    except Exception as e:
        msg, msg_type = f"❌ Upload failed: {e}", "err"

    return redirect(url_for("browse_table", db_name=db_name, table=table,
                            msg=msg, msg_type=msg_type))


# ── Operations ────────────────────────────────────────────────────────────────

@app.route("/operations")
def operations():
    dbs = db_names()
    selected_db = request.args.get("database", dbs[0])
    return render_template("operations.html", db_names=dbs, selected_db=selected_db,
                           sql_result=None, sql_error=None, sql_query=None, open_card=None)


@app.route("/api/tables/<db_name>")
def api_tables(db_name):
    try:
        database = get_db(db_name)
        tables = db_api.list_tables(database)
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/columns/<db_name>/<table>")
def api_columns(db_name, table):
    try:
        database = get_db(db_name)
        cols = db_api.get_table_columns(database, table)
        return jsonify({"columns": [c["column_name"] for c in cols]})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/ops/create-table", methods=["POST"])
def ops_create_table():
    try:
        database = get_db(request.form["database"])
        table = request.form["table_name"].strip()
        col_names = request.form.getlist("col_name")
        col_types = request.form.getlist("col_type")
        col_nulls = request.form.getlist("col_nullable")
        pk_mode   = request.form.get("pk_mode", "serial")
        pk_custom_name = request.form.get("pk_custom_name", "id").strip() or "id"
        pk_custom_type = request.form.get("pk_custom_type", "").strip()
        columns = [{"name": n, "type": t, "nullable": str(i) in col_nulls}
                   for i, (n, t) in enumerate(zip(col_names, col_types)) if n.strip()]
        db_api.create_table(database=database, table=table, columns=columns,
                            pk_mode=pk_mode, pk_custom_name=pk_custom_name,
                            pk_custom_type=pk_custom_type)
        msg, msg_type = f"✅ Table '{table}' created successfully", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/drop-table", methods=["POST"])
def ops_drop_table():
    try:
        database = get_db(request.form["database"])
        table = request.form["table"]
        if request.form.get("confirm_name", "").strip() != table:
            raise ValueError("Table name confirmation did not match")
        db_api.drop_table(database=database, table=table)
        msg, msg_type = f"✅ Table '{table}' dropped", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/add-column", methods=["POST"])
def ops_add_column():
    try:
        database = get_db(request.form["database"])
        col_type = request.form.get("column_type_other") or request.form.get("column_type")
        db_api.add_column(database=database, table=request.form["table"],
                          column=request.form["column_name"], col_type=col_type,
                          nullable=request.form.get("nullable") == "yes")
        msg, msg_type = f"✅ Column '{request.form['column_name']}' added", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/drop-column", methods=["POST"])
def ops_drop_column():
    try:
        database = get_db(request.form["database"])
        db_api.drop_column(database=database, table=request.form["table"],
                           column=request.form["column"])
        msg, msg_type = f"✅ Column '{request.form['column']}' dropped", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/rename-column", methods=["POST"])
def ops_rename_column():
    try:
        database = get_db(request.form["database"])
        db_api.rename_column(database=database, table=request.form["table"],
                             old_name=request.form["old_column"],
                             new_name=request.form["new_column_name"])
        msg, msg_type = f"✅ Column renamed to '{request.form['new_column_name']}'", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/change-column-type", methods=["POST"])
def ops_change_column_type():
    try:
        database = get_db(request.form["database"])
        new_type = request.form.get("new_type_other") or request.form.get("new_type")
        db_api.change_column_type(database=database, table=request.form["table"],
                                  column=request.form["column"], new_type=new_type)
        msg, msg_type = f"✅ Column type changed to '{new_type}'", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/clear-table", methods=["POST"])
def ops_clear_table():
    try:
        database = get_db(request.form["database"])
        table = request.form["table"]
        if request.form.get("confirm_name", "").strip() != table:
            raise ValueError("Table name confirmation did not match")
        count = db_api.clear_table(database=database, table=table)
        msg, msg_type = f"✅ Cleared {count} rows from '{table}' (schema preserved)", "ok"
    except Exception as e:
        msg, msg_type = f"❌ {e}", "err"
    return redirect(url_for("operations", database=request.form.get("database"),
                            msg=msg, msg_type=msg_type))


@app.route("/ops/execute-sql", methods=["POST"])
def ops_execute_sql():
    db_name = request.form.get("database")
    sql     = request.form.get("sql", "").strip()
    result  = None
    error   = None
    if sql:
        try:
            database = get_db(db_name)
            result = db_api.execute_sql(database=database, sql=sql)
        except Exception as e:
            error = str(e)
    dbs = db_names()
    return render_template("operations.html", db_names=dbs, selected_db=db_name,
                           sql_result=result, sql_error=error, sql_query=sql,
                           open_card="execute-sql")


if __name__ == "__main__":
    app.run(debug=True)