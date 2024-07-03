from contextlib import _GeneratorContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, List, Optional, Protocol

from ..model import Job


@dataclass()
class GetParams:
    kind: str
    by_args: Optional[bool] = None
    args: Optional[Any] = None
    by_created_at: Optional[bool] = None
    created_at: Optional[List[datetime]] = None
    created_at_begin: Optional[datetime] = None
    created_at_end: Optional[datetime] = None
    by_queue: Optional[bool] = None
    queue: Optional[str] = None
    by_state: Optional[bool] = None
    state: Optional[List[str]] = None


@dataclass
class JobInsertParams:
    kind: str
    args: Optional[Any] = None
    metadata: Optional[Any] = None
    max_attempts: Optional[int] = field(default=25)
    priority: Optional[int] = field(default=1)
    queue: Optional[str] = field(default="default")
    scheduled_at: Optional[datetime] = None
    state: Optional[str] = field(default="available")
    tags: Optional[List[str]] = field(default_factory=list)
    finalized_at: Optional[datetime] = None


class ExecutorProtocol(Protocol):
    def advisory_lock(self, lock: int) -> None:
        pass

    def job_insert(self, insert_params: JobInsertParams) -> Job:
        pass

    def job_insert_many(self, all_params) -> List[Job]:
        pass

    def job_get_by_kind_and_unique_properties(
        self, get_params: GetParams
    ) -> Optional[Job]:
        pass

    def transaction(self) -> _GeneratorContextManager:
        pass


class DriverProtocol(Protocol):
    def executor(self) -> Iterator[ExecutorProtocol]:
        pass

    def unwrap_executor(self, tx) -> ExecutorProtocol:
        pass
