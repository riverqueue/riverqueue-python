from contextlib import contextmanager
from typing import Optional, List, Generator

from ...client import JobInsertParams
from ...driver import Driver
from ...models import GetParams, Job
from . import river_job, pg_misc


class SqlAlchemyDriver(Driver):
    def __init__(self, session):
        self.session = session
        self.pg_misc_querier = pg_misc.Querier(session)
        self.job_querier = river_job.Querier(session)

    def advisory_lock(self, key: int) -> None:
        self.pg_misc_querier.pg_advisory_xact_lock(key=key)

    def job_insert(self, insert_params: JobInsertParams) -> Optional[Job]:
        return self.job_querier.job_insert_fast(insert_params)

    def job_insert_many(self, all_params) -> List[Job]:
        raise NotImplementedError("sqlc doesn't implement copy in python yet")

    def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        return self.job_querier.job_get_by_kind_and_unique_properties(get_params)

    @contextmanager
    def transaction(self) -> Generator:
        session = self.session
        with session.begin_nested():
            yield
