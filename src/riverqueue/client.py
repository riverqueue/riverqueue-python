from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import re
from typing import (
    Any,
    Awaitable,
    Literal,
    Optional,
    Protocol,
    Tuple,
    List,
    Callable,
    runtime_checkable,
)

from .driver import GetParams, JobInsertParams, DriverProtocol, ExecutorProtocol
from .driver.driver_protocol import AsyncDriverProtocol, AsyncExecutorProtocol
from .model import InsertResult
from .fnv import fnv1_hash

JOB_STATE_AVAILABLE = "available"
JOB_STATE_CANCELLED = "cancelled"
JOB_STATE_COMPLETED = "completed"
JOB_STATE_DISCARDED = "discarded"
JOB_STATE_RETRYABLE = "retryable"
JOB_STATE_RUNNING = "running"
JOB_STATE_SCHEDULED = "scheduled"

MAX_ATTEMPTS_DEFAULT = 25
PRIORITY_DEFAULT = 1
QUEUE_DEFAULT = "default"
UNIQUE_STATES_DEFAULT = [
    JOB_STATE_AVAILABLE,
    JOB_STATE_COMPLETED,
    JOB_STATE_RUNNING,
    JOB_STATE_RETRYABLE,
    JOB_STATE_SCHEDULED,
]


@dataclass
class InsertOpts:
    max_attempts: Optional[int] = None
    priority: Optional[int] = None
    queue: Optional[str] = None
    scheduled_at: Optional[datetime] = None
    tags: Optional[List[Any]] = None
    unique_opts: Optional["UniqueOpts"] = None


class JobArgs(Protocol):
    """
    Protocol that should be implemented by all job args.
    """

    kind: str

    def to_json(self) -> str:
        pass


@runtime_checkable
class JobArgsWithInsertOpts(Protocol):
    """
    Protocol that's optionally implemented by a JobArgs implementation so that
    every inserted instance of them provides the same custom `InsertOpts`.
    `InsertOpts` passed to insert functions will take precedence of one returned
    by `JobArgsWithInsertOpts`.
    """

    def insert_opts(self) -> InsertOpts:
        pass


@dataclass
class InsertManyParams:
    args: JobArgs
    insert_opts: Optional[InsertOpts] = None


@dataclass
class UniqueOpts:
    by_args: Optional[Literal[True]] = None
    by_period: Optional[int] = None
    by_queue: Optional[Literal[True]] = None
    by_state: Optional[list[str]] = None


class AsyncClient:
    def __init__(
        self, driver: AsyncDriverProtocol, advisory_lock_prefix: Optional[int] = None
    ):
        self.driver = driver
        self.advisory_lock_prefix = _check_advisory_lock_prefix_bounds(
            advisory_lock_prefix
        )

    async def insert(
        self, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        async with self.driver.executor() as exec:
            if not insert_opts:
                insert_opts = InsertOpts()
            insert_params, unique_opts = _make_insert_params(args, insert_opts)

            async def insert():
                return InsertResult(await exec.job_insert(insert_params))

            return await self.__check_unique_job(
                exec, insert_params, unique_opts, insert
            )

    async def insert_tx(
        self, tx, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        exec = self.driver.unwrap_executor(tx)
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = _make_insert_params(args, insert_opts)

        async def insert():
            return InsertResult(await exec.job_insert(insert_params))

        return await self.__check_unique_job(exec, insert_params, unique_opts, insert)

    async def insert_many(self, args: List[JobArgs | InsertManyParams]) -> int:
        async with self.driver.executor() as exec:
            return await exec.job_insert_many(_make_insert_params_many(args))

    async def insert_many_tx(self, tx, args: List[JobArgs | InsertManyParams]) -> int:
        exec = self.driver.unwrap_executor(tx)
        return await exec.job_insert_many(_make_insert_params_many(args))

    async def __check_unique_job(
        self,
        exec: AsyncExecutorProtocol,
        insert_params: JobInsertParams,
        unique_opts: Optional[UniqueOpts],
        insert_func: Callable[[], Awaitable[InsertResult]],
    ) -> InsertResult:
        get_params, lock_key = _build_unique_get_params_and_lock_key(
            self.advisory_lock_prefix, insert_params, unique_opts
        )

        if not get_params:
            return await insert_func()

        async with exec.transaction():
            await exec.advisory_lock(lock_key)

            existing_job = await exec.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return await insert_func()


class Client:
    def __init__(
        self, driver: DriverProtocol, advisory_lock_prefix: Optional[int] = None
    ):
        self.driver = driver
        self.advisory_lock_prefix = _check_advisory_lock_prefix_bounds(
            advisory_lock_prefix
        )

    def insert(
        self, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        with self.driver.executor() as exec:
            if not insert_opts:
                insert_opts = InsertOpts()
            insert_params, unique_opts = _make_insert_params(args, insert_opts)

            def insert():
                return InsertResult(exec.job_insert(insert_params))

            return self.__check_unique_job(exec, insert_params, unique_opts, insert)

    def insert_tx(
        self, tx, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        exec = self.driver.unwrap_executor(tx)
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = _make_insert_params(args, insert_opts)

        def insert():
            return InsertResult(exec.job_insert(insert_params))

        return self.__check_unique_job(exec, insert_params, unique_opts, insert)

    def insert_many(self, args: List[JobArgs | InsertManyParams]) -> int:
        with self.driver.executor() as exec:
            return exec.job_insert_many(_make_insert_params_many(args))

    def insert_many_tx(self, tx, args: List[JobArgs | InsertManyParams]) -> int:
        exec = self.driver.unwrap_executor(tx)
        return exec.job_insert_many(_make_insert_params_many(args))

    def __check_unique_job(
        self,
        exec: ExecutorProtocol,
        insert_params: JobInsertParams,
        unique_opts: Optional[UniqueOpts],
        insert_func: Callable[[], InsertResult],
    ) -> InsertResult:
        get_params, lock_key = _build_unique_get_params_and_lock_key(
            self.advisory_lock_prefix, insert_params, unique_opts
        )

        if not get_params:
            return insert_func()

        with exec.transaction():
            exec.advisory_lock(lock_key)

            existing_job = exec.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return insert_func()


def _build_unique_get_params_and_lock_key(
    advisory_lock_prefix: Optional[int],
    insert_params: JobInsertParams,
    unique_opts: Optional[UniqueOpts],
) -> tuple[Optional[GetParams], int]:
    if unique_opts is None:
        return (None, 0)

    any_unique_opts = False
    get_params = GetParams(kind=insert_params.kind)

    lock_str = f"unique_keykind={insert_params.kind}"

    if unique_opts.by_args:
        any_unique_opts = True
        get_params.by_args = True
        get_params.args = insert_params.args
        lock_str += f"&args={insert_params.args}"

    if unique_opts.by_period:
        lower_period_bound = _truncate_time(
            datetime.now(timezone.utc), unique_opts.by_period
        )

        any_unique_opts = True
        get_params.by_created_at = True
        get_params.created_at = [
            lower_period_bound,
            lower_period_bound + timedelta(seconds=unique_opts.by_period),
        ]
        lock_str += f"&period={lower_period_bound.strftime('%FT%TZ')}"

    if unique_opts.by_queue:
        any_unique_opts = True
        get_params.by_queue = True
        get_params.queue = insert_params.queue
        lock_str += f"&queue={insert_params.queue}"

    if unique_opts.by_state:
        any_unique_opts = True
        get_params.by_state = True
        get_params.state = unique_opts.by_state
        lock_str += f"&state={','.join(unique_opts.by_state)}"
    else:
        get_params.state = UNIQUE_STATES_DEFAULT
        lock_str += f"&state={','.join(UNIQUE_STATES_DEFAULT)}"

    if not any_unique_opts:
        return (None, 0)

    if advisory_lock_prefix is None:
        lock_key = fnv1_hash(lock_str.encode("utf-8"), 64)
    else:
        prefix = advisory_lock_prefix
        lock_key = (prefix << 32) | fnv1_hash(lock_str.encode("utf-8"), 32)

    return (get_params, _uint64_to_int64(lock_key))


def _check_advisory_lock_prefix_bounds(
    advisory_lock_prefix: Optional[int],
) -> Optional[int]:
    if advisory_lock_prefix:
        # We only reserve 4 bytes for the prefix, so make sure the given one
        # properly fits. This will error in case that's not the case.
        advisory_lock_prefix.to_bytes(4)
    return advisory_lock_prefix


def _make_insert_params(
    args: JobArgs,
    insert_opts: InsertOpts,
    is_insert_many: bool = False,
) -> Tuple[JobInsertParams, Optional[UniqueOpts]]:
    args.kind  # fail fast in case args don't respond to kind

    args_json = args.to_json()
    assert args_json is not None, "args should return non-nil from `to_json`"

    args_insert_opts = InsertOpts()
    if isinstance(args, JobArgsWithInsertOpts):
        args_insert_opts = args.insert_opts()

    scheduled_at = insert_opts.scheduled_at or args_insert_opts.scheduled_at
    unique_opts = insert_opts.unique_opts or args_insert_opts.unique_opts

    if is_insert_many and unique_opts:
        raise ValueError("unique opts can't be used with `insert_many`")

    insert_params = JobInsertParams(
        args=args_json,
        kind=args.kind,
        max_attempts=insert_opts.max_attempts
        or args_insert_opts.max_attempts
        or MAX_ATTEMPTS_DEFAULT,
        priority=insert_opts.priority or args_insert_opts.priority or PRIORITY_DEFAULT,
        queue=insert_opts.queue or args_insert_opts.queue or QUEUE_DEFAULT,
        scheduled_at=scheduled_at and scheduled_at.astimezone(timezone.utc),
        state="scheduled" if scheduled_at else "available",
        tags=_validate_tags(insert_opts.tags or args_insert_opts.tags or []),
    )

    return insert_params, unique_opts


def _make_insert_params_many(
    args: List[JobArgs | InsertManyParams],
) -> List[JobInsertParams]:
    return [
        _make_insert_params(
            arg.args, arg.insert_opts or InsertOpts(), is_insert_many=True
        )[0]
        if isinstance(arg, InsertManyParams)
        else _make_insert_params(arg, InsertOpts(), is_insert_many=True)[0]
        for arg in args
    ]


def _truncate_time(time, interval_seconds) -> datetime:
    return datetime.fromtimestamp(
        (time.timestamp() // interval_seconds) * interval_seconds, tz=timezone.utc
    )


def _uint64_to_int64(uint64):
    # Packs a uint64 then unpacks to int64 to fit within Postgres bigint
    return (uint64 + (1 << 63)) % (1 << 64) - (1 << 63)


tag_re = re.compile("\A[\w][\w\-]+[\w]\Z")


def _validate_tags(tags: list[str]) -> list[str]:
    for tag in tags:
        assert (
            len(tag) <= 255 and tag_re.match(tag)
        ), f"tags should be less than 255 characters in length and match regex {tag_re.pattern}"
    return tags
