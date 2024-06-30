import os
from typing import Iterator

import pytest
from testcontainers.postgres import PostgresContainer
import sqlalchemy
from sqlalchemy import Engine, text


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    if os.getenv("RIVER_USE_DOCKER"):
        yield from engine_with_docker()
    else:
        yield from engine_with_database_url()


def engine_with_database_url() -> Iterator[Engine]:
    database_url = os.getenv("TEST_DATABASE_URL", "postgres://localhost/river_test")

    # sqlalchemy removed support for postgres:// for reasons beyond comprehension
    database_url = database_url.replace("postgres://", "postgresql://")

    yield sqlalchemy.create_engine(database_url)


def engine_with_docker() -> Iterator[Engine]:
    with PostgresContainer("postgres:16") as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())
        with engine.connect() as conn:
            with open(
                os.path.join(os.path.dirname(__file__), "0001-create-tables.sql")
            ) as f:
                statements = f.read()
            for statement in statements.split(";"):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        yield engine
