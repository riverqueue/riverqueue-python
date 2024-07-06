from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

from riverqueue import Client, InsertOpts, UniqueOpts
from riverqueue.driver import DriverProtocol, ExecutorProtocol
import sqlalchemy


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


def test_insert_with_only_args(client, mock_exec, simple_args):
    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(simple_args)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"


def test_insert_tx(mock_driver, client, simple_args):
    mock_exec = MagicMock(spec=ExecutorProtocol)
    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    mock_tx = MagicMock(spec=sqlalchemy.Transaction)

    def mock_unwrap_executor(tx: sqlalchemy.Transaction):
        assert tx == mock_tx
        return mock_exec

    mock_driver.unwrap_executor.side_effect = mock_unwrap_executor

    insert_res = client.insert_tx(mock_tx, simple_args)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"


def test_insert_with_insert_opts_from_args(client, mock_exec, simple_args):
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(
        simple_args,
        insert_opts=InsertOpts(
            max_attempts=23, priority=2, queue="job_custom_queue", tags=["job_custom"]
        ),
    )

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    insert_args = mock_exec.job_insert.call_args[0][0]
    assert insert_args.max_attempts == 23
    assert insert_args.priority == 2
    assert insert_args.queue == "job_custom_queue"
    assert insert_args.tags == ["job_custom"]


def test_insert_with_insert_opts_from_job(client, mock_exec):
    @dataclass
    class MyArgs:
        kind = "my_args"

        @staticmethod
        def insert_opts() -> InsertOpts:
            return InsertOpts(
                max_attempts=23,
                priority=2,
                queue="job_custom_queue",
                tags=["job_custom"],
            )

        @staticmethod
        def to_json() -> str:
            return "{}"

    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(
        MyArgs(),
    )

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    insert_args = mock_exec.job_insert.call_args[0][0]
    assert insert_args.max_attempts == 23
    assert insert_args.priority == 2
    assert insert_args.queue == "job_custom_queue"
    assert insert_args.tags == ["job_custom"]


def test_insert_with_insert_opts_precedence(client, mock_exec, simple_args):
    @dataclass
    class MyArgs:
        kind = "my_args"

        @staticmethod
        def insert_opts() -> InsertOpts:
            return InsertOpts(
                max_attempts=23,
                priority=2,
                queue="job_custom_queue",
                tags=["job_custom"],
            )

        @staticmethod
        def to_json() -> str:
            return "{}"

    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(
        simple_args,
        insert_opts=InsertOpts(
            max_attempts=17, priority=3, queue="my_queue", tags=["custom"]
        ),
    )

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    insert_args = mock_exec.job_insert.call_args[0][0]
    assert insert_args.max_attempts == 17
    assert insert_args.priority == 3
    assert insert_args.queue == "my_queue"
    assert insert_args.tags == ["custom"]


def test_insert_with_unique_opts_by_args(client, mock_exec, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(
    mock_datetime, client, mock_exec, simple_args
):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


def test_insert_with_unique_opts_by_queue(client, mock_exec, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


def test_insert_with_unique_opts_by_state(client, mock_exec, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_state=["available", "running"]))

    mock_exec.job_get_by_kind_and_unique_properties.return_value = None
    mock_exec.job_insert.return_value = "job_row"

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert.call_args[0][0]
    assert call_args.kind == "simple"


def test_insert_kind_error(client):
    @dataclass
    class MyArgs:
        pass

    with pytest.raises(AttributeError) as ex:
        client.insert(MyArgs())
    assert "'MyArgs' object has no attribute 'kind'" == str(ex.value)


def test_insert_to_json_attribute_error(client):
    @dataclass
    class MyArgs:
        kind = "my"

    with pytest.raises(AttributeError) as ex:
        client.insert(MyArgs())
    assert "'MyArgs' object has no attribute 'to_json'" == str(ex.value)


def test_insert_to_json_none_error(client):
    @dataclass
    class MyArgs:
        kind = "my"

        @staticmethod
        def to_json() -> None:
            return None

    with pytest.raises(AssertionError) as ex:
        client.insert(MyArgs())
    assert "args should return non-nil from `to_json`" == str(ex.value)


def test_tag_validation(client, simple_args):
    client.insert(
        simple_args, insert_opts=InsertOpts(tags=["foo", "bar", "baz", "foo-bar-baz"])
    )

    with pytest.raises(AssertionError) as ex:
        client.insert(simple_args, insert_opts=InsertOpts(tags=["commas,bad"]))
    assert (
        r"tags should be less than 255 characters in length and match regex \A[\w][\w\-]+[\w]\Z"
        == str(ex.value)
    )

    with pytest.raises(AssertionError) as ex:
        client.insert(simple_args, insert_opts=InsertOpts(tags=["a" * 256]))
    assert (
        r"tags should be less than 255 characters in length and match regex \A[\w][\w\-]+[\w]\Z"
        == str(ex.value)
    )


def test_check_advisory_lock_prefix_bounds():
    Client(mock_driver, advisory_lock_prefix=123)

    with pytest.raises(OverflowError) as ex:
        Client(mock_driver, advisory_lock_prefix=-1)
    assert "can't convert negative int to unsigned" == str(ex.value)

    # 2^32-1 is 0xffffffff (1s for 32 bits) which fits
    Client(mock_driver, advisory_lock_prefix=2**32 - 1)

    # 2^32 is 0x100000000, which does not
    with pytest.raises(OverflowError) as ex:
        Client(mock_driver, advisory_lock_prefix=2**32)
    assert "int too big to convert" == str(ex.value)
