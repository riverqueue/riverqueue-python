import pytest
import pytest_asyncio
import sqlalchemy
import sqlalchemy.ext.asyncio
from datetime import datetime, timezone
from typing import AsyncIterator, Iterator
from unittest.mock import patch

from riverqueue import (
    MAX_ATTEMPTS_DEFAULT,
    PRIORITY_DEFAULT,
    QUEUE_DEFAULT,
    AsyncClient,
    Client,
    InsertManyParams,
    InsertOpts,
    JobState,
    UniqueOpts,
)
from riverqueue.driver import riversqlalchemy
from riverqueue.driver.driver_protocol import JobGetByKindAndUniquePropertiesParam


@pytest.fixture
def test_tx(engine: sqlalchemy.Engine) -> Iterator[sqlalchemy.Connection]:
    with engine.connect() as conn_tx:
        # Force SQLAlchemy to open a transaction.
        #
        # SQLAlchemy seems to be designed to operate as surprisingly as
        # possible. Invoking `begin()` doesn't actually start a transaction.
        # Instead, it only does so lazily when a command is first issued. This
        # can be a big problem for our internal code, because when it wants to
        # start a transaction of its own to do say, a uniqueness check, unless
        # another SQL command has already executed it'll accidentally start a
        # top-level transaction instead of one in a test transaction that'll be
        # rolled back, and cause our tests to commit test jobs. So to work
        # around that, we make sure to fire an initial command, thereby forcing
        # a transaction to begin. Absolutely terrible design.
        conn_tx.execute(sqlalchemy.text("SELECT 1"))

        yield conn_tx

        conn_tx.rollback()


@pytest.fixture
def driver(test_tx: sqlalchemy.Connection) -> riversqlalchemy.Driver:
    return riversqlalchemy.Driver(test_tx)


@pytest.fixture
def client(driver: riversqlalchemy.Driver) -> Client:
    return Client(driver)


@pytest_asyncio.fixture
async def test_tx_async(
    engine_async: sqlalchemy.ext.asyncio.AsyncEngine,
) -> AsyncIterator[sqlalchemy.ext.asyncio.AsyncConnection]:
    async with engine_async.connect() as conn_tx:
        # Force SQLAlchemy to open a transaction.
        #
        # See explanatory comment in `test_tx()` above.
        await conn_tx.execute(sqlalchemy.text("SELECT 1"))

        yield conn_tx
        await conn_tx.rollback()


@pytest.fixture
def driver_async(
    test_tx_async: sqlalchemy.ext.asyncio.AsyncConnection,
) -> riversqlalchemy.AsyncDriver:
    return riversqlalchemy.AsyncDriver(test_tx_async)


@pytest_asyncio.fixture
async def client_async(
    driver_async: riversqlalchemy.AsyncDriver,
) -> AsyncClient:
    return AsyncClient(driver_async)


def test_insert_job_from_row(client, simple_args):
    insert_res = client.insert(simple_args)
    job = insert_res.job
    assert job
    assert isinstance(job.args, dict)
    assert job.attempt == 0
    assert job.attempted_by is None
    assert job.created_at.tzinfo == timezone.utc
    assert job.errors is None
    assert job.kind == "simple"
    assert job.max_attempts == MAX_ATTEMPTS_DEFAULT
    assert isinstance(job.metadata, dict)
    assert job.priority == PRIORITY_DEFAULT
    assert job.queue == QUEUE_DEFAULT
    assert job.scheduled_at.tzinfo == timezone.utc
    assert job.state == JobState.AVAILABLE
    assert job.tags == []


def test_insert_with_only_args_sync(client, simple_args):
    insert_res = client.insert(simple_args)
    assert insert_res.job


@pytest.mark.asyncio
async def test_insert_with_only_args_async(client_async, simple_args):
    insert_res = await client_async.insert(simple_args)
    assert insert_res.job


def test_insert_tx_sync(client, driver, engine, simple_args, test_tx):
    insert_res = client.insert_tx(test_tx, simple_args)
    assert insert_res.job

    job = driver.unwrap_executor(test_tx).job_get_by_kind_and_unique_properties(
        JobGetByKindAndUniquePropertiesParam(kind=simple_args.kind)
    )
    assert job == insert_res.job

    with engine.begin() as conn_tx2:
        job = driver.unwrap_executor(conn_tx2).job_get_by_kind_and_unique_properties(
            JobGetByKindAndUniquePropertiesParam(kind=simple_args.kind)
        )
        assert job is None

        conn_tx2.rollback()


@pytest.mark.asyncio
async def test_insert_tx_async(
    client_async, driver_async, engine_async, simple_args, test_tx_async
):
    insert_res = await client_async.insert_tx(test_tx_async, simple_args)
    assert insert_res.job

    job = await driver_async.unwrap_executor(
        test_tx_async
    ).job_get_by_kind_and_unique_properties(
        JobGetByKindAndUniquePropertiesParam(kind=simple_args.kind)
    )
    assert job == insert_res.job

    async with engine_async.begin() as conn_tx2:
        job = await driver_async.unwrap_executor(
            conn_tx2
        ).job_get_by_kind_and_unique_properties(
            JobGetByKindAndUniquePropertiesParam(kind=simple_args.kind)
        )
        assert job is None

        await conn_tx2.rollback()


def test_insert_with_opts_sync(client, simple_args):
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    insert_res = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job


@pytest.mark.asyncio
async def test_insert_with_opts_async(client_async, simple_args):
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    insert_res = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job


def test_insert_with_unique_opts_by_args_sync(client, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    insert_res = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_args_async(client_async, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    insert_res = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period_sync(mock_datetime, client, simple_args):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    insert_res = client.insert(simple_args, insert_opts=insert_opts)
    insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@patch("datetime.datetime")
@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_period_async(
    mock_datetime, client_async, simple_args
):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    insert_res = await client_async.insert(simple_args, insert_opts=insert_opts)
    insert_res2 = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_queue_sync(client, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
    insert_res = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_queue_async(client_async, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
    insert_res = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_state_sync(client, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))
    insert_res = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_state_async(client_async, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))
    insert_res = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(simple_args, insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_many_with_only_args_sync(client, simple_args):
    num_inserted = client.insert_many([simple_args])
    assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_with_only_args_async(client_async, simple_args):
    num_inserted = await client_async.insert_many([simple_args])
    assert num_inserted == 1


def test_insert_many_with_insert_opts_sync(client, simple_args):
    num_inserted = client.insert_many(
        [
            InsertManyParams(
                args=simple_args,
                insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
            )
        ]
    )
    assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_with_insert_opts_async(client_async, simple_args):
    num_inserted = await client_async.insert_many(
        [
            InsertManyParams(
                args=simple_args,
                insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
            )
        ]
    )
    assert num_inserted == 1


def test_insert_many_tx_sync(client, simple_args, test_tx):
    num_inserted = client.insert_many_tx(test_tx, [simple_args])
    assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_tx_async(client_async, simple_args, test_tx_async):
    num_inserted = await client_async.insert_many_tx(test_tx_async, [simple_args])
    assert num_inserted == 1
