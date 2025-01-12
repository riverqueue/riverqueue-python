from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from riverqueue import Client, InsertOpts, JobState, UniqueOpts
from riverqueue.client import unique_bitmask_from_states
from riverqueue.driver import DriverProtocol, ExecutorProtocol
import sqlalchemy


@pytest.fixture
def mock_driver() -> DriverProtocol:
    return MagicMock(spec=DriverProtocol)


@pytest.fixture
def mock_exec(mock_driver) -> ExecutorProtocol:
    # Don't try to mock a context manager. It will cause endless pain around the
    # edges like swallowing raised exceptions.
    class TrivialContextManager:
        def __init__(self, with_val):
            self.with_val = with_val

        def __enter__(self):
            return self.with_val

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_exec = MagicMock(spec=ExecutorProtocol)
    mock_driver.executor.return_value = TrivialContextManager(mock_exec)

    return mock_exec


@pytest.fixture
def client(mock_driver) -> Client:
    return Client(mock_driver)


def test_insert_with_only_args(client, mock_exec, simple_args):
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(simple_args)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"


def test_insert_tx(mock_driver, client, simple_args):
    mock_exec = MagicMock(spec=ExecutorProtocol)
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    mock_tx = MagicMock(spec=sqlalchemy.Transaction)

    def mock_unwrap_executor(tx: sqlalchemy.Transaction):
        assert tx == mock_tx
        return mock_exec

    mock_driver.unwrap_executor.side_effect = mock_unwrap_executor

    insert_res = client.insert_tx(mock_tx, simple_args)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"


def test_insert_with_insert_opts_from_args(client, mock_exec, simple_args):
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(
        simple_args,
        insert_opts=InsertOpts(
            max_attempts=23, priority=2, queue="job_custom_queue", tags=["job_custom"]
        ),
    )

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_args = call_args[0]
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

    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(
        MyArgs(),
    )

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_args = call_args[0]
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

    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(
        simple_args,
        insert_opts=InsertOpts(
            max_attempts=17, priority=3, queue="my_queue", tags=["custom"]
        ),
    )

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_args = call_args[0]
    assert insert_args.max_attempts == 17
    assert insert_args.priority == 3
    assert insert_args.queue == "my_queue"
    assert insert_args.tags == ["custom"]


def test_insert_with_unique_opts_by_args(client, mock_exec, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_params = call_args[0]
    assert insert_params.kind == "simple"


@patch("datetime.datetime")
def test_insert_with_unique_opts_by_period(
    mock_datetime, client, mock_exec, simple_args
):
    mock_datetime.now.return_value = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_params = call_args[0]
    assert insert_params.kind == "simple"


def test_insert_with_unique_opts_by_queue(client, mock_exec, simple_args):
    insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))

    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_params = call_args[0]
    assert insert_params.kind == "simple"
    # default unique states should all be set except for cancelled and discarded:
    assert insert_params.unique_states == bytes([0b11110101])


def test_insert_with_unique_opts_by_state(client, mock_exec, simple_args):
    # Turn on all unique states:
    insert_opts = InsertOpts(
        unique_opts=UniqueOpts(
            by_state=[
                "available",
                "cancelled",
                "completed",
                "discarded",
                "pending",
                "retryable",
                "running",
                "scheduled",
            ]
        )
    )
    mock_exec.job_insert_many.return_value = [("job_row", False)]

    insert_res = client.insert(simple_args, insert_opts=insert_opts)

    mock_exec.job_insert_many.assert_called_once()
    assert insert_res.job == "job_row"

    # Check that the UniqueOpts were correctly processed
    call_args = mock_exec.job_insert_many.call_args[0][0]
    assert len(call_args) == 1
    insert_params = call_args[0]
    assert insert_params.kind == "simple"
    assert insert_params.unique_states == bytes([0b11111111])


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


def test_tag_validation(client, mock_exec, simple_args):
    mock_exec.job_insert_many.return_value = [("job_row", False)]
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


@pytest.mark.parametrize(
    "description, input_states, postgres_bitstring",
    [
        # Postgres bitstrings are little-endian, so the MSB (AVAILABLE) is on the right.
        ("No states selected", [], bytes([0b00000000])),
        ("Single state - available", [JobState.AVAILABLE], bytes([0b00000001])),
        ("Single state - SCHEDULED", [JobState.SCHEDULED], bytes([0b10000000])),
        ("Single state - RUNNING", [JobState.RUNNING], bytes([0b01000000])),
        (
            "AVAILABLE and SCHEDULED",
            [JobState.AVAILABLE, JobState.SCHEDULED],
            bytes([0b10000001]),
        ),
        (
            "COMPLETED, PENDING, RETRYABLE",
            [JobState.COMPLETED, JobState.PENDING, JobState.RETRYABLE],
            bytes([0b00110100]),
        ),
        (
            "Default states",
            [
                JobState.AVAILABLE,
                JobState.COMPLETED,
                JobState.PENDING,
                JobState.RETRYABLE,
                JobState.RUNNING,
                JobState.SCHEDULED,
            ],
            bytes([0b11110101]),
        ),
        (
            "All states selected",
            [
                JobState.AVAILABLE,
                JobState.CANCELLED,
                JobState.COMPLETED,
                JobState.DISCARDED,
                JobState.PENDING,
                JobState.RETRYABLE,
                JobState.RUNNING,
                JobState.SCHEDULED,
            ],
            bytes([0b11111111]),
        ),
        (
            "AVAILABLE, COMPLETED, RETRYABLE, SCHEDULED",
            [
                JobState.AVAILABLE,
                JobState.COMPLETED,
                JobState.RETRYABLE,
                JobState.SCHEDULED,
            ],
            bytes([0b10100101]),
        ),
        (
            "Overlapping states",
            [JobState.AVAILABLE, JobState.AVAILABLE],
            bytes([0b00000001]),
        ),
        ("None input treated as empty", None, bytes([0b00000000])),
    ],
)
def test_unique_bitmask_from_states(description, input_states, postgres_bitstring):
    if input_states is None:
        input_states = []

    result = unique_bitmask_from_states(input_states)
    assert (
        result == postgres_bitstring
    ), f"{description} For states {input_states}, expected {postgres_bitstring}, got {result}"
