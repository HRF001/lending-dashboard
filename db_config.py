import os


def get_db_config():
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5433")),
        "dbname": os.getenv("PGDATABASE", "lending_db"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "1"),
    }
