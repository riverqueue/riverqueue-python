from dataclasses import dataclass, field
import datetime
from enum import Enum
from typing import Any, Optional


class JobState(str, Enum):
    AVAILABLE = "available"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    DISCARDED = "discarded"
    PENDING = "pending"
    RETRYABLE = "retryable"
    RUNNING = "running"
    SCHEDULED = "scheduled"


@dataclass
class InsertResult:
    job: "Job"
    unique_skipped_as_duplicated: bool = field(default=False)


@dataclass
class Job:
    id: int
    args: dict[str, Any]
    attempt: int
    attempted_at: Optional[datetime.datetime]
    attempted_by: Optional[list[str]]
    created_at: datetime.datetime
    errors: Optional[list[Any]]
    finalized_at: Optional[datetime.datetime]
    kind: str
    max_attempts: int
    metadata: dict[str, Any]
    priority: int
    queue: str
    state: JobState
    scheduled_at: datetime.datetime
    tags: list[str]
