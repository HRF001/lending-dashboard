import os

import psycopg2
from psycopg2.extras import execute_values


TABLE_NAME = "clean_lending_activity"


def get_local_db_config():
    return {
        "host": os.getenv("LOCAL_PGHOST", "localhost"),
        "port": int(os.getenv("LOCAL_PGPORT", "5433")),
        "dbname": os.getenv("LOCAL_PGDATABASE", "lending_db"),
        "user": os.getenv("LOCAL_PGUSER", "postgres"),
        "password": os.getenv("LOCAL_PGPASSWORD", "1"),
    }


def get_render_db_config():
    return {
        "host": os.getenv("RENDER_PGHOST"),
        "port": int(os.getenv("RENDER_PGPORT", "5432")),
        "dbname": os.getenv("RENDER_PGDATABASE"),
        "user": os.getenv("RENDER_PGUSER"),
        "password": os.getenv("RENDER_PGPASSWORD"),
    }


def validate_render_db_config(config):
    missing = [key for key, value in config.items() if value in (None, "")]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Missing render database settings: {joined}")


def fetch_source_rows():
    source_conn = psycopg2.connect(**get_local_db_config())
    try:
        with source_conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {TABLE_NAME}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return columns, rows
    finally:
        source_conn.close()


def sync_to_render(columns, rows):
    render_config = get_render_db_config()
    validate_render_db_config(render_config)

    target_conn = psycopg2.connect(**render_config)
    try:
        with target_conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")

            if rows:
                column_list = ", ".join(columns)
                insert_sql = f"INSERT INTO {TABLE_NAME} ({column_list}) VALUES %s"
                execute_values(cur, insert_sql, rows, page_size=1000)

        target_conn.commit()
    except Exception:
        target_conn.rollback()
        raise
    finally:
        target_conn.close()


def main():
    columns, rows = fetch_source_rows()
    sync_to_render(columns, rows)
    print(f"Synchronized {len(rows)} rows from local database to render database.")


if __name__ == "__main__":
    main()
