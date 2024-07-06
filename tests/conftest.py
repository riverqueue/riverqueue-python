from dataclasses import dataclass
import json
import os
from typing import Iterator

import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from riverqueue.driver.riversqlalchemy.dbsqlc import river_job


def engine_opts() -> dict:
    """
    Use to pass verbose logging options to an SQLAlchemy when `RIVER_DEBUG=true`
    in the environment.
    """

    if os.getenv("RIVER_DEBUG") == "true":
        return dict(echo=True, echo_pool="debug")

    return dict()


@pytest.fixture(scope="session")
def engine() -> sqlalchemy.Engine:
    return sqlalchemy.create_engine(test_database_url(), **engine_opts())


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
        **engine_opts(),
    )


def test_database_url(is_async: bool = False) -> str:
    """
    Produces a test URL based on `TEST_DATABASE_URL` or River's default
    convention and modifies it so that it's protocol includes an appropriate
    driver to make SQLAlchemy happy.
    """

    database_url = os.getenv("TEST_DATABASE_URL", "postgres://localhost/river_test")

    # sqlalchemy removed support for postgres:// for reasons beyond comprehension
    database_url = database_url.replace("postgres://", "postgresql://")

    # mix in an async driver for async
    if is_async:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://")

    return database_url


@dataclass
class SimpleArgs:
    test_name: str

    kind: str = "simple"

    def to_json(self) -> str:
        return json.dumps({"test_name": self.test_name})


@pytest.fixture
def simple_args(request: pytest.FixtureRequest):
    """
    Returns an instance of SimpleArgs encapsulating the running test's name. This
    can be useful in cases where a test is accidentally leaving leftovers in the
    database.
    """

    return SimpleArgs(test_name=request.node.name)


@pytest.fixture(autouse=True)
def check_leftover_jobs(engine) -> Iterator[None]:
    """
    Autorunning fixture that checks for leftover jobs after each test case. I
    previously had a huge amount of trouble tracking down tests that were
    inserting rows despite being in a test transaction and ended up adding this
    check, along with naming inserted jobs after their test case. If it turns
    these measures haven't been needed in a long time, we can probably remove
    them.
    """

    yield

    with engine.begin() as conn_tx:
        jobs = river_job.Querier(conn_tx).job_get_all()
        assert (
            list(jobs) == []
        ), "test case should not have persisted any jobs after run"
