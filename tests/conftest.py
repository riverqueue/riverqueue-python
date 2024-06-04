import os

import pytest
from testcontainers.postgres import PostgresContainer
import sqlalchemy
from sqlalchemy import text


@pytest.fixture(scope="session")
def engine():
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
