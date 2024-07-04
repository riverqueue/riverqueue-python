from datetime import datetime, timezone
from typing import Iterator
from unittest.mock import patch

import pytest

from riverqueue import Client, InsertOpts, UniqueOpts
from riverqueue.driver import riversqlalchemy
from riverqueue.driver.driver_protocol import GetParams
from sqlalchemy import Engine, text

from tests.simple_args import SimpleArgs


@pytest.fixture
def driver(engine: Engine) -> Iterator[riversqlalchemy.Driver]:
    with engine.begin() as tx:
        tx.execute(text("SET search_path TO public"))
        yield riversqlalchemy.Driver(tx)
        tx.rollback()


@pytest.fixture
def client(driver: riversqlalchemy.Driver) -> Client:
    return Client(driver)


def test_insert_with_only_args(client, driver):
    insert_res = client.insert(SimpleArgs())
    assert insert_res.job


def test_insert_tx(client, driver, engine):
    with engine.begin() as tx:
        args = SimpleArgs()
        insert_res = client.insert_tx(tx, args)
        assert insert_res.job

        job = driver.unwrap_executor(tx).job_get_by_kind_and_unique_properties(
            GetParams(kind=args.kind)
        )
        assert job == insert_res.job

        with engine.begin() as tx2:
            job = driver.unwrap_executor(tx2).job_get_by_kind_and_unique_properties(
                GetParams(kind=args.kind)
            )
            assert job is None

        tx.rollback()


def test_insert_with_opts(client, driver):
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job


def test_insert_with_unique_opts_by_args(client, driver):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_queue(client, driver):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job


def test_insert_with_unique_opts_by_state(client, driver):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))
    insert_res = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job
    insert_res2 = client.insert(SimpleArgs(), insert_opts=insert_opts)
    assert insert_res.job == insert_res2.job
