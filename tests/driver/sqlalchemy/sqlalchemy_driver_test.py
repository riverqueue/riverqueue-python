from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from riverqueue.client import Client
from riverqueue.models import InsertOpts, UniqueOpts
from riverqueue.drivers.sqlalchemy.sqlalchemy_driver import SqlAlchemyDriver
from client_test import SimpleArgs
from sqlalchemy import text


@pytest.fixture
def driver(engine):
    with engine.begin() as conn:
        conn.execute(text("SET search_path TO public"))
        yield SqlAlchemyDriver(conn)


@pytest.fixture
def client(driver):
    return Client(driver)


@patch("datetime.datetime")
def test_insert_with_only_args(mock_datetime, client, driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    args = SimpleArgs()
    result = client.insert(args)
    assert result.job


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
