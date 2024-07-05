import pytest
import pytest_asyncio
import sqlalchemy
import sqlalchemy.ext.asyncio
from datetime import datetime, timezone
from typing import AsyncIterator, Iterator
from unittest.mock import patch

from riverqueue import Client, InsertOpts, UniqueOpts
from riverqueue.client import AsyncClient, InsertManyParams
from riverqueue.driver import riversqlalchemy
from riverqueue.driver.driver_protocol import GetParams

# from tests.conftest import engine_async
from tests.simple_args import SimpleArgs


@pytest.fixture
def driver(engine: sqlalchemy.Engine) -> Iterator[riversqlalchemy.Driver]:
    with engine.connect() as conn_tx:
        conn_tx.execute(sqlalchemy.text("SET search_path TO public"))
        yield riversqlalchemy.Driver(conn_tx)
        conn_tx.rollback()


@pytest_asyncio.fixture
async def driver_async(
    engine_async: sqlalchemy.ext.asyncio.AsyncEngine,
) -> AsyncIterator[riversqlalchemy.AsyncDriver]:
    async with engine_async.connect() as conn_tx:
        await conn_tx.execute(sqlalchemy.text("SET search_path TO public"))
        yield riversqlalchemy.AsyncDriver(conn_tx)
        await conn_tx.rollback()


@pytest.fixture
def client(driver: riversqlalchemy.Driver) -> Client:
    return Client(driver)


@pytest_asyncio.fixture
async def client_async(
    driver_async: riversqlalchemy.AsyncDriver,
) -> AsyncClient:
    return AsyncClient(driver_async)


def test_insert_with_only_args_sync(client, driver):
    insert_res = client.insert(SimpleArgs())
    assert insert_res.job


@pytest.mark.asyncio
async def test_insert_with_only_args_async(client_async):
    insert_res = await client_async.insert(SimpleArgs())
    assert insert_res.job


def test_insert_tx_sync(client, driver, engine):
    with engine.begin() as conn_tx:
        args = SimpleArgs()
        insert_res = client.insert_tx(conn_tx, args)
        assert insert_res.job

        job = driver.unwrap_executor(conn_tx).job_get_by_kind_and_unique_properties(
            GetParams(kind=args.kind)
        )
        assert job == insert_res.job

        with engine.begin() as conn_tx2:
            job = driver.unwrap_executor(
                conn_tx2
            ).job_get_by_kind_and_unique_properties(GetParams(kind=args.kind))
            assert job is None

        conn_tx.rollback()


@pytest.mark.asyncio
async def test_insert_tx_async(client_async, driver_async, engine_async):
    async with engine_async.begin() as conn_tx:
        args = SimpleArgs()
        insert_res = await client_async.insert_tx(conn_tx, args)
        assert insert_res.job

        job = await driver_async.unwrap_executor(
            conn_tx
        ).job_get_by_kind_and_unique_properties(GetParams(kind=args.kind))
        assert job == insert_res.job

        async with engine_async.begin() as conn_tx2:
            job = await driver_async.unwrap_executor(
                conn_tx2
            ).job_get_by_kind_and_unique_properties(GetParams(kind=args.kind))
            assert job is None

        await conn_tx.rollback()


def test_insert_with_opts_sync(client):
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job


@pytest.mark.asyncio
async def test_insert_with_opts_async(client_async):
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    insert_res = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job


def test_insert_with_unique_opts_by_args_sync(client):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_args_sync_async(client_async):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    insert_res = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period_sync(mock_datetime, client):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@patch("datetime.datetime")
@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_period_async(mock_datetime, client_async):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    insert_res = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    insert_res2 = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_queue_sync(client):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_queue_async(client_async):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
    insert_res = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_state_sync(client):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@pytest.mark.asyncio
async def test_insert_with_unique_opts_by_state_async(client_async):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))
    insert_res = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = await client_async.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_many_with_only_args_sync(client, driver):
    num_inserted = client.insert_many([SimpleArgs()])
    assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_with_only_args_async(client_async):
    num_inserted = await client_async.insert_many([SimpleArgs()])
    assert num_inserted == 1


def test_insert_many_with_insert_opts_sync(client, driver):
    num_inserted = client.insert_many(
        [
            InsertManyParams(
                args=SimpleArgs(),
                insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
            )
        ]
    )
    assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_with_insert_opts_async(client_async):
    num_inserted = await client_async.insert_many(
        [
            InsertManyParams(
                args=SimpleArgs(),
                insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
            )
        ]
    )
    assert num_inserted == 1


def test_insert_many_tx_sync(client, engine):
    with engine.begin() as conn_tx:
        num_inserted = client.insert_many_tx(conn_tx, [SimpleArgs()])
        assert num_inserted == 1


@pytest.mark.asyncio
async def test_insert_many_tx_async(client_async, engine_async):
    async with engine_async.begin() as conn_tx:
        num_inserted = await client_async.insert_many_tx(conn_tx, [SimpleArgs()])
        assert num_inserted == 1
