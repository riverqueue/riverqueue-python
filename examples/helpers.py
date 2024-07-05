import os


def dev_database_url(is_async: bool = False) -> str:
    database_url = os.getenv("DATABASE_URL", "postgres://localhost/river_dev")

    # sqlalchemy removed support for postgres:// for reasons beyond comprehension
    database_url = database_url.replace("postgres://", "postgresql://")

    # mix in an async driver for async
    if is_async:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")

    return database_url
