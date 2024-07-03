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


@patch("datetime.datetime")
def test_insert_with_only_args(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    args = SimpleArgs()
    result = client.insert(args)
    assert result.job


@patch("datetime.datetime")
def test_insert_tx(mock_datetime, client, driver, engine):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    with engine.begin() as tx:
        args = SimpleArgs()
        result = client.insert_tx(tx, args)
        assert result.job

        job = driver.unwrap_executor(tx).job_get_by_kind_and_unique_properties(
            GetParams(kind=args.kind)
        )
        assert job == result.job

        with engine.begin() as tx2:
            job = driver.unwrap_executor(tx2).job_get_by_kind_and_unique_properties(
                GetParams(kind=args.kind)
            )
            assert job is None

        tx.rollback()


@patch("datetime.datetime")
def test_insert_with_opts(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    args = SimpleArgs()
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
    result = client.insert(args, insert_opts=insert_opts)
    assert result.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_args(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_args=args)
    insert_opts = InsertOpts(unique_opts=unique_opts)
    result = client.insert(args, insert_opts=insert_opts)
    assert result.job
    result2 = client.insert(args, insert_opts=insert_opts)
    assert result.job == result2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_period=900)
    insert_opts = InsertOpts(unique_opts=unique_opts)
    result = client.insert(args, insert_opts=insert_opts)
    result2 = client.insert(args, insert_opts=insert_opts)
    assert result.job == result2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_queue(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_queue=True)
    insert_opts = InsertOpts(unique_opts=unique_opts)
    result = client.insert(args, insert_opts=insert_opts)
    assert result.job
    result2 = client.insert(args, insert_opts=insert_opts)
    assert result.job == result2.job


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_state(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_state=["available", "running"])
    insert_opts = InsertOpts(unique_opts=unique_opts)
    result = client.insert(args, insert_opts=insert_opts)
    assert result.job
    result2 = client.insert(args, insert_opts=insert_opts)
    assert result.job == result2.job
