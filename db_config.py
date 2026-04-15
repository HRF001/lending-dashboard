import os


def _getenv_stripped(key: str, default=None):
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip()


def get_db_config():
    pg_host = _getenv_stripped("PGHOST")
    pg_port = _getenv_stripped("PGPORT")
    pg_database = _getenv_stripped("PGDATABASE")
    pg_user = _getenv_stripped("PGUSER")
    pg_password = _getenv_stripped("PGPASSWORD")

    # If any production-style PG env var is set, require the full set so
    # Render never falls back to local defaults by accident.
    if any([pg_host, pg_port, pg_database, pg_user, pg_password]):
        missing = [
            key for key, value in {
                "PGHOST": pg_host,
                "PGPORT": pg_port,
                "PGDATABASE": pg_database,
                "PGUSER": pg_user,
                "PGPASSWORD": pg_password,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"Missing required database environment variables: {', '.join(missing)}"
            )

    return {
        "host": pg_host or "localhost",
        "port": int(pg_port or "5433"),
        "dbname": pg_database or "lending_db",
        "user": pg_user or "postgres",
        "password": pg_password or "1",
    }
