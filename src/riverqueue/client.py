from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Protocol, Tuple, List, Callable

from .driver import GetParams, JobInsertParams, DriverProtocol, ExecutorProtocol
from .model import InsertResult
from .fnv import fnv1_hash

MAX_ATTEMPTS_DEFAULT = 25
PRIORITY_DEFAULT = 1
QUEUE_DEFAULT = "default"

DEFAULT_UNIQUE_STATES = ["available", "completed", "running", "retryable", "scheduled"]


class Args(Protocol):
    kind: str

    def to_json(self) -> str:
        pass


@dataclass
class InsertManyParams:
    args: Args
    insert_opts: Optional["InsertOpts"] = None


@dataclass
class InsertOpts:
    scheduled_at: Optional[datetime] = None
    unique_opts: Optional["UniqueOpts"] = None
    max_attempts: Optional[int] = None
    priority: Optional[int] = None
    queue: Optional[str] = None
    tags: Optional[List[Any]] = None


@dataclass
class UniqueOpts:
    by_args: Optional[Any] = None
    by_period: Optional[Any] = None
    by_queue: Optional[Any] = None
    by_state: Optional[Any] = None


class Client:
    def __init__(self, driver: DriverProtocol, advisory_lock_prefix=None):
        self.driver = driver
        self.advisory_lock_prefix = advisory_lock_prefix
        self.time_now_utc = lambda: datetime.now(timezone.utc)  # for test time stubbing

    def insert(
        self, args: Args, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        for exec in self.driver.executor():
            if not insert_opts:
                insert_opts = InsertOpts()
            insert_params, unique_opts = self.__make_insert_params(args, insert_opts)

            def insert():
                return InsertResult(exec.job_insert(insert_params))

            return self.__check_unique_job(exec, insert_params, unique_opts, insert)

        return InsertResult()  # for MyPy's benefit

    def insert_tx(
        self, tx, args: Args, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        exec = self.driver.unwrap_executor(tx)
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = self.__make_insert_params(args, insert_opts)

        def insert():
            return InsertResult(exec.job_insert(insert_params))

        return self.__check_unique_job(exec, insert_params, unique_opts, insert)

    def insert_many(self, args: List[Args]) -> List[InsertResult]:
        for exec in self.driver.executor():
            return [
                InsertResult(x)
                for x in exec.job_insert_many(self.__make_insert_params_many(args))
            ]

        return []  # for MyPy's benefit

    def insert_many_tx(self, tx, args: List[Args]) -> List[InsertResult]:
        exec = self.driver.unwrap_executor(tx)
        return [
            InsertResult(x)
            for x in exec.job_insert_many(self.__make_insert_params_many(args))
        ]

    def __make_insert_params_many(self, args: List[Args]) -> List[JobInsertParams]:
        return [
            self.__make_insert_params(
                arg.args, arg.insert_opts or InsertOpts(), is_insert_many=True
            )[0]
            if isinstance(arg, InsertManyParams)
            else self.__make_insert_params(arg, InsertOpts(), is_insert_many=True)[0]
            for arg in args
        ]

    def __check_unique_job(
        self,
        exec: ExecutorProtocol,
        insert_params: JobInsertParams,
        unique_opts: Optional[UniqueOpts],
        insert_func: Callable[[], InsertResult],
    ) -> InsertResult:
        if unique_opts is None:
            return insert_func()

        any_unique_opts = False
        get_params = GetParams(kind=insert_params.kind)

        lock_str = f"unique_keykind={insert_params.kind}"

        if unique_opts.by_args:
            any_unique_opts = True
            get_params.by_args = True
            get_params.args = insert_params.args
            lock_str += f"&args={insert_params.args}"

        if unique_opts.by_period:
            lower_period_bound = self.__truncate_time(
                self.time_now_utc(), unique_opts.by_period
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
            get_params.state = DEFAULT_UNIQUE_STATES
            lock_str += f"&state={','.join(DEFAULT_UNIQUE_STATES)}"

        if not any_unique_opts:
            return insert_func()

        with exec.transaction():
            if self.advisory_lock_prefix is None:
                lock_key = fnv1_hash(lock_str.encode("utf-8"), 64)
            else:
                prefix = self.advisory_lock_prefix
                lock_key = (prefix << 32) | fnv1_hash(lock_str.encode("utf-8"), 32)

            lock_key = self.__uint64_to_int64(lock_key)
            exec.advisory_lock(lock_key)

            existing_job = exec.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return insert_func()

        return InsertResult()  # for MyPy's benefit

    @staticmethod
    def __make_insert_params(
        args: Args,
        insert_opts: InsertOpts,
        is_insert_many: bool = False,
    ) -> Tuple[JobInsertParams, Optional[UniqueOpts]]:
        if not hasattr(args, "kind"):
            raise Exception("args should respond to `kind`")

        args_json = args.to_json()
        if args_json is None:
            raise Exception("args should return non-nil from `to_json`")

        args_insert_opts = getattr(args, "insert_opts", InsertOpts())

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
            priority=insert_opts.priority
            or args_insert_opts.priority
            or PRIORITY_DEFAULT,
            queue=insert_opts.queue or args_insert_opts.queue or QUEUE_DEFAULT,
            scheduled_at=scheduled_at and scheduled_at.astimezone(timezone.utc),
            state="scheduled" if scheduled_at else "available",
            tags=insert_opts.tags or args_insert_opts.tags,
        )

        return insert_params, unique_opts

    @staticmethod
    def __truncate_time(time, interval_seconds) -> datetime:
        return datetime.fromtimestamp(
            (time.timestamp() // interval_seconds) * interval_seconds, tz=timezone.utc
        )

    @staticmethod
    def __uint64_to_int64(uint64):
        # Packs a uint64 then unpacks to int64 to fit within Postgres bigint
        return (uint64 + (1 << 63)) % (1 << 64) - (1 << 63)
