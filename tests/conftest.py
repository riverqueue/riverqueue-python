import os

import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio


@pytest.fixture(scope="session")
def engine() -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(test_database_url())


# @pytest_asyncio.fixture(scope="session")
@pytest.fixture(scope="session")
def engine_async() -> sqlalchemy.ext.asyncio.AsyncEngine:
    return sqlalchemy.ext.asyncio.create_async_engine(
        test_database_url(is_async=True),
        # For the life of me I can't get async SQLAlchemy working with
        # pytest-async. Even when using an explicit `engine.connect()`,
        # SQLAlchemy seems to reuse the same connection between test cases,
        # thereby resulting in a "another operation is in progress" error.
        # This statement disables pooling which isn't ideal, but I've spent
        # too many hours trying to figure this out so I'm calling it.
        poolclass=sqlalchemy.pool.NullPool,
    )


def test_database_url(is_async: bool = False) -> str:
    database_url = os.getenv("TEST_DATABASE_URL", "postgres://localhost/river_test")

    # sqlalchemy removed support for postgres:// for reasons beyond comprehension
    database_url = database_url.replace("postgres://", "postgresql://")

    # mix in an async driver for async
    if is_async:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")

    return database_url
