from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

from riverqueue import Client, InsertOpts, UniqueOpts
from riverqueue.driver import DriverProtocol, ExecutorProtocol
import sqlalchemy

from tests.simple_args import SimpleArgs


@pytest.fixture
def mock_driver() -> DriverProtocol:
    return MagicMock(spec=DriverProtocol)


@pytest.fixture
def mock_exec(mock_driver) -> ExecutorProtocol:
    def mock_context_manager(val) -> Mock:
        context_manager_mock = MagicMock()
        context_manager_mock.__enter__.return_value = val
        context_manager_mock.__exit__.return_value = Mock()
        return context_manager_mock

    # def mock_context_manager(val) -> Mock:
    #     return Mock(__enter__=val, __exit__=Mock())

    mock_exec = MagicMock(spec=ExecutorProtocol)
    mock_driver.executor.return_value = mock_context_manager(mock_exec)

    return mock_exec


@pytest.fixture
def client(mock_driver) -> Client:
    return Client(mock_driver)


@patch("datetime.datetime")
def test_insert_with_only_args(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(SimpleArgs())

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"


@patch("datetime.datetime")
def test_insert_tx(mock_datetime, mock_driver, client):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_exec = MagicMock(spec=ExecutorProtocol)
    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    mock_tx = MagicMock(spec=sqlalchemy.Transaction)

    def mock_unwrap_executor(tx: sqlalchemy.Transaction):
        assert tx == mock_tx
        return mock_exec

    mock_driver.unwrap_executor.side_effect = mock_unwrap_executor

    result = client.insert_tx(mock_tx, SimpleArgs())

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"


@patch("datetime.datetime")
def test_insert_with_opts(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    insert_opts = InsertOpts(queue="high_priority", unique_opts=None)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the InsertOpts were correctly passed to make_insert_params
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.queue == "high_priority"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_args(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_args=True)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_period=900)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_queue(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_queue=True)
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_state(mock_datetime, client, mock_exec):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    args = SimpleArgs()
    unique_opts = UniqueOpts(by_state=["available", "running"])
    insert_opts = InsertOpts(unique_opts=unique_opts)

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    result = client.insert(args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert result.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"
