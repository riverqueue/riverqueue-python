from typing import List, ContextManager, Optional, Protocol

from .models import JobInsertParams, Job


class Driver(Protocol):
    def advisory_lock(self, lock: int) -> None:
        pass

    def job_insert(self, insert_params: JobInsertParams) -> Optional[Job]:
        pass

    def job_insert_many(self, all_params) -> List[Job]:
        pass

    def job_get_by_kind_and_unique_properties(self, get_params) -> Optional[Job]:
        pass

    def transaction(self) -> ContextManager:
        pass
