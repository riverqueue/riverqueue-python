import os
from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import sqlalchemy
import sqlalchemy.ext.asyncio
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def engine() -> Iterator[sqlalchemy.Engine]:
    if os.getenv("RIVER_USE_DOCKER"):
        yield from engine_with_docker()
    else:
        yield sqlalchemy.create_engine(test_database_url())


@pytest_asyncio.fixture(scope="session")
async def engine_async() -> AsyncIterator[sqlalchemy.ext.asyncio.AsyncEngine]:
    if os.getenv("RIVER_USE_DOCKER"):
        async for engine in engine_with_docker_async():
            yield engine
    else:
        yield sqlalchemy.ext.asyncio.create_async_engine(
            test_database_url(is_async=True),
            # For the life of me I can't get async SQLAlchemy working with
            # pytest-async. Even when using an explicit `engine.connect()`,
            # SQLAlchemy seems to reuse the same connection between test cases,
            # thereby resulting in a "another operation is in progress" error.
            # This statement disables pooling which isn't ideal, but I've spent
            # too many hours trying to figure this out so I'm calling it.
            poolclass=sqlalchemy.pool.NullPool,
        )


def engine_with_docker() -> Iterator[sqlalchemy.Engine]:
    with PostgresContainer("postgres:16") as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        with engine.connect() as conn:
            for statement in migrate_up_statements():
                conn.execute(sqlalchemy.text(statement))
            conn.commit()
        yield engine


async def engine_with_docker_async() -> (
    AsyncIterator[sqlalchemy.ext.asyncio.AsyncEngine]
):
    with PostgresContainer("postgres:16") as postgres:
        print("docker_database_url", postgres.get_connection_url())
        engine = sqlalchemy.ext.asyncio.create_async_engine(
            postgres.get_connection_url(driver="asyncpg")
        )
        async with engine.connect() as conn:
            for statement in migrate_up_statements():
                await conn.execute(sqlalchemy.text(statement))
            await conn.commit()
        yield engine


def migrate_up_statements() -> list[str]:
    with open(os.path.join(os.path.dirname(__file__), "0001-create-tables.sql")) as f:
        statements = f.read()
    return [statement.strip() for statement in statements.split(";") if statement != ""]


def test_database_url(is_async: bool = False) -> str:
    database_url = os.getenv("TEST_DATABASE_URL", "postgres://localhost/river_test")

    # sqlalchemy removed support for postgres:// for reasons beyond comprehension
    database_url = database_url.replace("postgres://", "postgresql://")

    # mix in an async driver for async
    if is_async:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")

    return database_url
