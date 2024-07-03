from contextlib import contextmanager
from sqlalchemy import Engine
from sqlalchemy.engine import Connection
from typing import Iterator, Optional, List, Generator, cast

from ...driver import DriverProtocol, ExecutorProtocol, GetParams, JobInsertParams
from ...model import Job
from . import river_job, pg_misc


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

    def job_insert_many(self, all_params) -> List[Job]:
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
    def transaction(self) -> Generator:
        if self.conn.in_transaction():
            with self.conn.begin_nested():
                yield
        else:
            with self.conn.begin():
                yield


class Driver(DriverProtocol):
    def __init__(self, conn: Connection | Engine):
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
