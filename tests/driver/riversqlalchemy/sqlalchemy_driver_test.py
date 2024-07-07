import json
import pytest
import pytest_asyncio
from riverqueue.job import AttemptError
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
from riverqueue.driver.riversqlalchemy import dbsqlc


class TestAsyncClient:
    #
    # fixtures
    #

    @pytest_asyncio.fixture
    @staticmethod
    async def test_tx(
        engine_async: sqlalchemy.ext.asyncio.AsyncEngine,
    ) -> AsyncIterator[sqlalchemy.ext.asyncio.AsyncConnection]:
        async with engine_async.connect() as conn_tx:
            # Force SQLAlchemy to open a transaction.
            #
            # See explanatory comment in `test_tx()` above.
            await conn_tx.execute(sqlalchemy.text("SELECT 1"))

            yield conn_tx
            await conn_tx.rollback()

    @pytest_asyncio.fixture
    @staticmethod
    async def client(
        test_tx: sqlalchemy.ext.asyncio.AsyncConnection,
    ) -> AsyncClient:
        return AsyncClient(riversqlalchemy.AsyncDriver(test_tx))

    #
    # tests
    #

    @pytest.mark.asyncio
    async def test_insert_job_from_row(self, client, simple_args, test_tx):
        insert_res = await client.insert(simple_args)
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

        now = datetime.now(timezone.utc)

        job_row = await dbsqlc.river_job.AsyncQuerier(test_tx).job_insert_full(
            dbsqlc.river_job.JobInsertFullParams(
                args=json.dumps(dict(foo="args")),
                attempt=0,
                attempted_at=None,
                created_at=datetime.now(),
                errors=[
                    AttemptError(
                        at=now,
                        attempt=1,
                        error="message",
                        trace="trace",
                    ).to_json(),
                ],
                finalized_at=datetime.now(),
                kind="custom_kind",
                max_attempts=MAX_ATTEMPTS_DEFAULT,
                metadata=json.dumps(dict(foo="metadata")),
                priority=PRIORITY_DEFAULT,
                queue=QUEUE_DEFAULT,
                scheduled_at=datetime.now(),
                state=JobState.COMPLETED,
                tags=[],
            )
        )

        job = riversqlalchemy.sql_alchemy_driver.job_from_row(job_row)
        assert job
        assert job.args == dict(foo="args")
        assert job.errors == [
            AttemptError(
                at=now,
                attempt=1,
                error="message",
                trace="trace",
            )
        ]
        assert job.finalized_at.tzinfo == timezone.utc
        assert job.metadata == dict(foo="metadata")

    #
    # tests below this line should match what are in the sync client tests below
    #

    @pytest.mark.asyncio
    async def test_insert_with_only_args(self, client, simple_args):
        insert_res = await client.insert(simple_args)
        assert insert_res.job

    @pytest.mark.asyncio
    async def test_insert_tx(self, client, engine_async, simple_args, test_tx):
        insert_res = await client.insert_tx(test_tx, simple_args)
        assert insert_res.job

        job = await dbsqlc.river_job.AsyncQuerier(test_tx).job_get_by_id(
            id=insert_res.job.id
        )
        assert job

        async with engine_async.begin() as test_tx2:
            job = await dbsqlc.river_job.AsyncQuerier(test_tx2).job_get_by_id(
                id=insert_res.job.id
            )
            assert job is None

            await test_tx2.rollback()

    @pytest.mark.asyncio
    async def test_insert_with_opts(self, client, simple_args):
        insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
        insert_res = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job

    @pytest.mark.asyncio
    async def test_insert_with_unique_opts_by_args(self, client, simple_args):
        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
        insert_res = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    @patch("datetime.datetime")
    @pytest.mark.asyncio
    async def test_insert_with_unique_opts_by_period(
        self, mock_datetime, client, simple_args
    ):
        mock_datetime.now.return_value = datetime(
            2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc
        )

        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
        insert_res = await client.insert(simple_args, insert_opts=insert_opts)
        insert_res2 = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    @pytest.mark.asyncio
    async def test_insert_with_unique_opts_by_queue(self, client, simple_args):
        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
        insert_res = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    @pytest.mark.asyncio
    async def test_insert_with_unique_opts_by_state(self, client, simple_args):
        insert_opts = InsertOpts(
            unique_opts=UniqueOpts(by_state=[JobState.AVAILABLE, JobState.RUNNING])
        )
        insert_res = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = await client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    @pytest.mark.asyncio
    async def test_insert_many_with_only_args(self, client, simple_args):
        num_inserted = await client.insert_many([simple_args])
        assert num_inserted == 1

    @pytest.mark.asyncio
    async def test_insert_many_with_insert_opts(self, client, simple_args):
        num_inserted = await client.insert_many(
            [
                InsertManyParams(
                    args=simple_args,
                    insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
                )
            ]
        )
        assert num_inserted == 1

    @pytest.mark.asyncio
    async def test_insert_many_tx(self, client, simple_args, test_tx):
        num_inserted = await client.insert_many_tx(test_tx, [simple_args])
        assert num_inserted == 1


class TestSyncClient:
    #
    # fixtures
    #

    @pytest.fixture
    @staticmethod
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
    @staticmethod
    def client(test_tx: sqlalchemy.Connection) -> Client:
        return Client(riversqlalchemy.Driver(test_tx))

    #
    # tests; should match with tests for the async client above
    #

    def test_insert_with_only_args(self, client, simple_args):
        insert_res = client.insert(simple_args)
        assert insert_res.job

    def test_insert_tx(self, client, engine, simple_args, test_tx):
        insert_res = client.insert_tx(test_tx, simple_args)
        assert insert_res.job

        job = dbsqlc.river_job.Querier(test_tx).job_get_by_id(id=insert_res.job.id)
        assert job

        with engine.begin() as test_tx2:
            job = dbsqlc.river_job.Querier(test_tx2).job_get_by_id(id=insert_res.job.id)
            assert job is None

            test_tx2.rollback()

    def test_insert_with_opts(self, client, simple_args):
        insert_opts = InsertOpts(queue="high_priority", unique_opts=None)
        insert_res = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job

    def test_insert_with_unique_opts_by_args(self, client, simple_args):
        print("self", self)
        print("client", client)
        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_args=True))
        insert_res = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    @patch("datetime.datetime")
    def test_insert_with_unique_opts_by_period(
        self, mock_datetime, client, simple_args
    ):
        mock_datetime.now.return_value = datetime(
            2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc
        )

        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_period=900))
        insert_res = client.insert(simple_args, insert_opts=insert_opts)
        insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    def test_insert_with_unique_opts_by_queue(self, client, simple_args):
        insert_opts = InsertOpts(unique_opts=UniqueOpts(by_queue=True))
        insert_res = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    def test_insert_with_unique_opts_by_state(self, client, simple_args):
        insert_opts = InsertOpts(
            unique_opts=UniqueOpts(by_state=[JobState.AVAILABLE, JobState.RUNNING])
        )
        insert_res = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job
        insert_res2 = client.insert(simple_args, insert_opts=insert_opts)
        assert insert_res.job == insert_res2.job

    def test_insert_many_with_only_args(self, client, simple_args):
        num_inserted = client.insert_many([simple_args])
        assert num_inserted == 1

    def test_insert_many_with_insert_opts(self, client, simple_args):
        num_inserted = client.insert_many(
            [
                InsertManyParams(
                    args=simple_args,
                    insert_opts=InsertOpts(queue="high_priority", unique_opts=None),
                )
            ]
        )
        assert num_inserted == 1

    def test_insert_many_tx(self, client, simple_args, test_tx):
        num_inserted = client.insert_many_tx(test_tx, [simple_args])
        assert num_inserted == 1
