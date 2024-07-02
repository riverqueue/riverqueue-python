from dataclasses import dataclass, field
from typing import Optional


@dataclass
class InsertResult:
    job: Optional["Job"] = field(default=None)
    unique_skipped_as_duplicated: bool = field(default=False)


@dataclass
class Job:
    pass
