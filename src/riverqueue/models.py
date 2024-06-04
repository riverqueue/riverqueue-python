from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Protocol, List, Any


class Args(Protocol):
    kind: str

    def to_json(self) -> str:
        pass


@dataclass
class Job:
    pass


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


@dataclass
class InsertOpts:
    scheduled_at: Optional[datetime] = None
    unique_opts: Optional["UniqueOpts"] = None
    max_attempts: Optional[int] = None
    priority: Optional[int] = None
    queue: Optional[str] = None
    tags: Optional[List[Any]] = None


@dataclass
class InsertManyParams:
    args: Args
    insert_opts: Optional["InsertOpts"] = None


@dataclass
class UniqueOpts:
    by_args: Optional[Any] = None
    by_period: Optional[Any] = None
    by_queue: Optional[Any] = None
    by_state: Optional[Any] = None


@dataclass
class InsertResult:
    job: Optional["Job"] = field(default=None)
    unique_skipped_as_duplicated: bool = field(default=False)


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
