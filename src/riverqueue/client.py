from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Callable

from .driver import Driver
from .fnv import fnv1a_64, fnv1a_32
from .models import (
    InsertOpts,
    Args,
    InsertResult,
    InsertManyParams,
    JobInsertParams,
    UniqueOpts,
    GetParams,
)

MAX_ATTEMPTS_DEFAULT = 25
PRIORITY_DEFAULT = 1
QUEUE_DEFAULT = "default"

DEFAULT_UNIQUE_STATES = ["available", "completed", "running", "retryable", "scheduled"]


class Client:
    def __init__(self, driver: Driver, advisory_lock_prefix=None):
        self.driver = driver
        self.advisory_lock_prefix = advisory_lock_prefix
        self.time_now_utc = lambda: datetime.now(timezone.utc)  # for test time stubbing

    def insert(
        self, args: Args, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = self.make_insert_params(args, insert_opts)

        def insert():
            return InsertResult(self.driver.job_insert(insert_params))

        return self.check_unique_job(insert_params, unique_opts, insert)

    def insert_many(self, args: List[Args]) -> List[InsertResult]:
        all_params = [
            self.make_insert_params(
                arg.args, arg.insert_opts or InsertOpts(), is_insert_many=True
            )[0]
            if isinstance(arg, InsertManyParams)
            else self.make_insert_params(arg, InsertOpts(), is_insert_many=True)[0]
            for arg in args
        ]
        return [InsertResult(x) for x in self.driver.job_insert_many(all_params)]

    def check_unique_job(
        self,
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
            lower_period_bound = self.truncate_time(
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

        with self.driver.transaction():
            if self.advisory_lock_prefix is None:
                lock_key = fnv1a_64(lock_str.encode("utf-8"))
            else:
                prefix = self.advisory_lock_prefix
                lock_key = (prefix << 32) | fnv1a_32(lock_str.encode("utf-8"))

            lock_key = self.uint64_to_int64(lock_key)
            self.driver.advisory_lock(lock_key)

            existing_job = self.driver.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return insert_func()

    @staticmethod
    def make_insert_params(
        args: Args, insert_opts: InsertOpts, is_insert_many: bool = False
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
    def truncate_time(time, interval_seconds) -> datetime:
        return datetime.fromtimestamp(
            (time.timestamp() // interval_seconds) * interval_seconds, tz=timezone.utc
        )

    @staticmethod
    def uint64_to_int64(uint64):
        # Packs a uint64 then unpacks to int64 to fit within Postgres bigint
        return (uint64 + (1 << 63)) % (1 << 64) - (1 << 63)
