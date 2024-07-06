from contextlib import (
    asynccontextmanager,
    contextmanager,
)
from datetime import datetime, timezone
from riverqueue.driver.driver_protocol import AsyncDriverProtocol, AsyncExecutorProtocol
from sqlalchemy import Engine
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Iterator,
    Optional,
    cast,
)

from ...driver import DriverProtocol, ExecutorProtocol, GetParams, JobInsertParams
from ...model import Job, JobState
from .dbsqlc import models, river_job, pg_misc


class AsyncExecutor(AsyncExecutorProtocol):
    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.pg_misc_querier = pg_misc.AsyncQuerier(conn)
        self.job_querier = river_job.AsyncQuerier(conn)

    async def advisory_lock(self, key: int) -> None:
        await self.pg_misc_querier.pg_advisory_xact_lock(key=key)

    async def job_insert(self, insert_params: JobInsertParams) -> Job:
        return _job_from_row(
            cast(  # drop Optional[] because insert always returns a row
                models.RiverJob,
                await self.job_querier.job_insert_fast(
                    cast(river_job.JobInsertFastParams, insert_params)
                ),
            )
        )

    async def job_insert_many(self, all_params: list[JobInsertParams]) -> int:
        await self.job_querier.job_insert_fast_many(
            _build_insert_many_params(all_params)
        )
        return len(all_params)

    async def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        row = await self.job_querier.job_get_by_kind_and_unique_properties(
            cast(river_job.JobGetByKindAndUniquePropertiesParams, get_params)
        )
        return _job_from_row(row) if row else None

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator:
        if self.conn.in_transaction():
            async with self.conn.begin_nested():
                yield
        else:
            async with self.conn.begin():
                yield


class AsyncDriver(AsyncDriverProtocol):
    def __init__(self, conn: AsyncConnection | AsyncEngine):
        assert isinstance(conn, AsyncConnection) or isinstance(conn, AsyncEngine)

        self.conn = conn

    @asynccontextmanager
    async def executor(self) -> AsyncIterator[AsyncExecutorProtocol]:
        if isinstance(self.conn, AsyncEngine):
            async with self.conn.begin() as tx:
                yield AsyncExecutor(tx)
        else:
            yield AsyncExecutor(self.conn)

    def unwrap_executor(self, tx) -> AsyncExecutorProtocol:
        return AsyncExecutor(tx)


class Executor(ExecutorProtocol):
    def __init__(self, conn: Connection):
        self.conn = conn
        self.pg_misc_querier = pg_misc.Querier(conn)
        self.job_querier = river_job.Querier(conn)

    def advisory_lock(self, key: int) -> None:
        self.pg_misc_querier.pg_advisory_xact_lock(key=key)

    def job_insert(self, insert_params: JobInsertParams) -> Job:
        return _job_from_row(
            cast(  # drop Optional[] because insert always returns a row
                models.RiverJob,
                self.job_querier.job_insert_fast(
                    cast(river_job.JobInsertFastParams, insert_params)
                ),
            ),
        )

    def job_insert_many(self, all_params: list[JobInsertParams]) -> int:
        self.job_querier.job_insert_fast_many(_build_insert_many_params(all_params))
        return len(all_params)

    def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        row = self.job_querier.job_get_by_kind_and_unique_properties(
            cast(river_job.JobGetByKindAndUniquePropertiesParams, get_params)
        )
        return _job_from_row(row) if row else None

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self.conn.in_transaction():
            with self.conn.begin_nested():
                yield
        else:
            with self.conn.begin():
                yield


class Driver(DriverProtocol):
    def __init__(self, conn: Connection | Engine):
        assert isinstance(conn, Connection) or isinstance(conn, Engine)

        self.conn = conn

    @contextmanager
    def executor(self) -> Iterator[ExecutorProtocol]:
        if isinstance(self.conn, Engine):
            with self.conn.begin() as tx:
                yield Executor(tx)
        else:
            yield Executor(self.conn)

    def unwrap_executor(self, tx) -> ExecutorProtocol:
        return Executor(tx)


def _build_insert_many_params(
    all_params: list[JobInsertParams],
) -> river_job.JobInsertFastManyParams:
    insert_many_params = river_job.JobInsertFastManyParams(
        args=[],
        kind=[],
        max_attempts=[],
        metadata=[],
        priority=[],
        queue=[],
        scheduled_at=[],
        state=[],
        tags=[],
    )

    for insert_params in all_params:
        insert_many_params.args.append(insert_params.args)
        insert_many_params.kind.append(insert_params.kind)
        insert_many_params.max_attempts.append(insert_params.max_attempts)
        insert_many_params.metadata.append(insert_params.metadata or "{}")
        insert_many_params.priority.append(insert_params.priority)
        insert_many_params.queue.append(insert_params.queue)
        insert_many_params.scheduled_at.append(
            insert_params.scheduled_at or datetime.now(timezone.utc)
        )
        insert_many_params.state.append(cast(models.RiverJobState, insert_params.state))
        insert_many_params.tags.append(",".join(insert_params.tags))

    return insert_many_params


def _job_from_row(row: models.RiverJob) -> Job:
    """
    Converts an internal sqlc generated row to the top level type, issuing a few
    minor transformations along the way. Timestamps are changed from local
    timezone to UTC.
    """

    return Job(
        id=row.id,
        args=row.args,
        attempt=row.attempt,
        attempted_at=row.attempted_at.astimezone(timezone.utc)
        if row.attempted_at
        else None,
        attempted_by=row.attempted_by,
        created_at=row.created_at.astimezone(timezone.utc),
        errors=row.errors,
        finalized_at=row.finalized_at.astimezone(timezone.utc)
        if row.finalized_at
        else None,
        kind=row.kind,
        max_attempts=row.max_attempts,
        metadata=row.metadata,
        priority=row.priority,
        queue=row.queue,
        state=cast(JobState, row.state),
        scheduled_at=row.scheduled_at.astimezone(timezone.utc),
        tags=row.tags,
    )
