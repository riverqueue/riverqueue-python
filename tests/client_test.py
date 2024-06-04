import json
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from riverqueue.client import Client
from riverqueue.models import InsertOpts, UniqueOpts


@pytest.fixture
def mock_driver():
    return MagicMock()


@pytest.fixture
def client(mock_driver):
    return Client(mock_driver)


@dataclass
class SimpleArgs:
    kind: str = "simple"

    @staticmethod
    def to_json() -> str:
        return json.dumps({"job_num": 1})


@patch("datetime.datetime")
def test_insert_with_proto_args(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(SimpleArgs())

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"


@patch("datetime.datetime")
def test_insert_with_only_args(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(SimpleArgs())

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"


@patch("datetime.datetime")
def test_insert_with_opts(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the InsertOpts were correctly passed to make_insert_params
    call_args = mock_driver.job_insert.call_args[0][0]
    assert call_args.queue == "high_priority"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_args(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_args=True)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_driver.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_period=900)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_driver.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_queue(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_queue=True)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_driver.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_state(mock_datetime, client, mock_driver):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_state=["available", "running"])
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_driver.job_get_by_kind_and_unique_properties.return_value = None
    mock_driver.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_driver.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_driver.job_insert.call_args[0][0]
    assert call_args.kind == "simple"
