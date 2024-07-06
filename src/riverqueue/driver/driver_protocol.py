from contextlib import (
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, List, Optional, Protocol

from ..job import Job


@dataclass()
class JobGetByKindAndUniquePropertiesParam:
    """
    Parameters for looking up a job by kind and unique properties.
    """

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
    """
    Insert parameters for a job. This is sent to underlying drivers and is meant
    for internal use only. Its interface is subject to change.
    """

    kind: str
    args: Any = None
    created_at: Optional[datetime] = None
    finalized_at: Optional[datetime] = None
    metadata: Optional[Any] = None
    max_attempts: int = field(default=25)
    priority: int = field(default=1)
    queue: str = field(default="default")
    scheduled_at: Optional[datetime] = None
    state: str = field(default="available")
    tags: list[str] = field(default_factory=list)


class AsyncExecutorProtocol(Protocol):
    """
    Protocol for an asyncio executor. An executor wraps a connection pool or
    transaction and performs the operations required for a client to insert a
    job.
    """

    async def advisory_lock(self, lock: int) -> None:
        pass

    async def job_insert(self, insert_params: JobInsertParams) -> Job:
        pass

    async def job_insert_many(self, all_params) -> int:
        pass

    async def job_get_by_kind_and_unique_properties(
        self, get_params: JobGetByKindAndUniquePropertiesParam
    ) -> Optional[Job]:
        pass

    # Even after spending two hours on it, I'm unable to find a return type for
    # this function that MyPy will accept. The only two workable options I found
    # were either (1) removing the return value completely (the implementations
    # still have one however), or (2) remove the `async` keyword, remove the
    # `@asynccontextmanager` annotation, and use this return type:
    #
    #     -> _AsyncGeneratorContextManager
    #
    # I went with (1) because that seems preferable.
    @asynccontextmanager
    async def transaction(self):
        """
        Used as a context manager in a `with` block, open a transaction or
        subtransaction for the given context. Commits automatically on exit, or
        rolls back on error.
        """

        pass


class AsyncDriverProtocol(Protocol):
    """
    Protocol for an asyncio client driver. A driver acts as a layer of
    abstraction that wraps another class for a client to work.
    """

    # Even after spending two hours on it, I'm unable to find a return type for
    # this function that MyPy will accept. The only two workable options I found
    # were either (1) removing the return value completely (the implementations
    # still have one however), or (2) remove the `async` keyword, remove the
    # `@asynccontextmanager` annotation, and use this return type:
    #
    #     -> _AsyncGeneratorContextManager[AsyncExecutorProtocol]
    #
    # I went with (1) because that seems preferable.
    @asynccontextmanager
    async def executor(self):
        """
        Used as a context manager in a `with` block, return an executor from the
        underlying engine that's good for the given context.
        """

        pass

    def unwrap_executor(self, tx) -> AsyncExecutorProtocol:
        """
        Produces an executor from a transaction.
        """

        pass


class ExecutorProtocol(Protocol):
    """
    Protocol for a non-asyncio executor. An executor wraps a connection pool or
    transaction and performs the operations required for a client to insert a
    job.
    """

    def advisory_lock(self, lock: int) -> None:
        pass

    def job_insert(self, insert_params: JobInsertParams) -> Job:
        pass

    def job_insert_many(self, all_params) -> int:
        pass

    def job_get_by_kind_and_unique_properties(
        self, get_params: JobGetByKindAndUniquePropertiesParam
    ) -> Optional[Job]:
        pass

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """
        Used as a context manager in a `with` block, open a transaction or
        subtransaction for the given context. Commits automatically on exit, or
        rolls back on error.
        """

        pass


class DriverProtocol(Protocol):
    """
    Protocol for a non-asyncio client driver. A driver acts as a layer of
    abstraction that wraps another class for a client to work.
    """

    @contextmanager
    def executor(self) -> Iterator[ExecutorProtocol]:
        """
        Used as a context manager in a `with` block, return an executor from the
        underlying engine that's good for the given context.
        """

        pass

    def unwrap_executor(self, tx) -> ExecutorProtocol:
        """
        Produces an executor from a transaction.
        """

        pass
