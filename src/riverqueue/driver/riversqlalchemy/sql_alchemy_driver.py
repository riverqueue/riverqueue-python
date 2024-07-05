from contextlib import (
    asynccontextmanager,
    contextmanager,
)
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
from ...model import Job
from .dbsqlc import river_job, pg_misc


class AsyncExecutor(AsyncExecutorProtocol):
    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.pg_misc_querier = pg_misc.AsyncQuerier(conn)
        self.job_querier = river_job.AsyncQuerier(conn)

    async def advisory_lock(self, key: int) -> None:
        await self.pg_misc_querier.pg_advisory_xact_lock(key=key)

    async def job_insert(self, insert_params: JobInsertParams) -> Job:
        return cast(
            Job,
            await self.job_querier.job_insert_fast(
                cast(river_job.JobInsertFastParams, insert_params)
            ),
        )

    async def job_insert_many(self, all_params) -> int:
        raise NotImplementedError("sqlc doesn't implement copy in python yet")

    async def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        return cast(
            Optional[Job],
            await self.job_querier.job_get_by_kind_and_unique_properties(
                cast(river_job.JobGetByKindAndUniquePropertiesParams, get_params)
            ),
        )

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
        return cast(
            Job,
            self.job_querier.job_insert_fast(
                cast(river_job.JobInsertFastParams, insert_params)
            ),
        )

    def job_insert_many(self, all_params) -> int:
        raise NotImplementedError("sqlc doesn't implement copy in python yet")

    def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        return cast(
            Optional[Job],
            self.job_querier.job_get_by_kind_and_unique_properties(
                cast(river_job.JobGetByKindAndUniquePropertiesParams, get_params)
            ),
        )

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
