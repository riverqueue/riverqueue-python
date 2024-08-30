from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from hashlib import sha256
import re
from typing import (
    Optional,
    Protocol,
    Tuple,
    List,
    cast,
    runtime_checkable,
)

from riverqueue.insert_opts import InsertOpts, UniqueOpts

from .driver import (
    JobGetByKindAndUniquePropertiesParam,
    JobInsertParams,
    DriverProtocol,
    ExecutorProtocol,
)
from .driver.driver_protocol import AsyncDriverProtocol, AsyncExecutorProtocol
from .job import Job, JobState
from .fnv import fnv1_hash


MAX_ATTEMPTS_DEFAULT: int = 25
"""
Default number of maximum attempts for a job.
"""

PRIORITY_DEFAULT: int = 1
"""
Default priority for a job.
"""

QUEUE_DEFAULT: str = "default"
"""
Default queue for a job.
"""

UNIQUE_STATES_DEFAULT: list[str] = [
    JobState.AVAILABLE,
    JobState.COMPLETED,
    JobState.RUNNING,
    JobState.RETRYABLE,
    JobState.SCHEDULED,
]
"""
Default job states included during a unique job insertion.
"""


@dataclass
class InsertResult:
    job: "Job"
    """
    Inserted job row, or an existing job row if insert was skipped due to a
    previously existing unique job.
    """

    unique_skipped_as_duplicated: bool = field(default=False)
    """
    True if for a unique job, the insertion was skipped due to an equivalent job
    matching unique property already being present.
    """


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
    """
    A single job to insert that's part of an `insert_many()` batch insert.
    Unlike sending raw job args, supports an `InsertOpts` to pair with the job.
    """

    args: JobArgs
    """
    Job args to insert.
    """

    insert_opts: Optional[InsertOpts] = None
    """
    Insertion options to use with the insert.
    """


class AsyncClient:
    """
    Provides a client for River that inserts jobs. Unlike the Go version of the
    River client, this one can insert jobs only. Jobs can only be worked from Go
    code, so job arg kinds and JSON encoding details must be shared between Ruby
    and Go code.

    Used in conjunction with a River driver like:

        ```
        import riverqueue
        from riverqueue.driver import riversqlalchemy

        engine = sqlalchemy.ext.asyncio.create_async_engine("postgresql+asyncpg://...")
        client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))
        ```

    This variant is for use with Python's asyncio (asynchronous I/O).
    """

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
        """
        Inserts a new job for work given a job args implementation and insertion
        options (which may be omitted).

        With job args only:

            ```
            insert_res = await client.insert(
                SortArgs(strings=["whale", "tiger", "bear"]),
            )
            insert_res.job # inserted job row
            ```

        With insert opts:

            ```
            insert_res = await client.insert(
                SortArgs(strings=["whale", "tiger", "bear"]),
                insert_opts=riverqueue.InsertOpts(
                    max_attempts=17,
                    priority=3,
                    queue: "my_queue",
                    tags: ["custom"]
                ),
            )
            insert_res.job # inserted job row
            ```

        Job arg implementations are expected to respond to:

            * `kind` is a unique string that identifies them the job in the
              database, and which a Go worker will recognize.

            * `to_json()` defines how the job will serialize to JSON, which of
              course will have to be parseable as an object in Go.

        They may also respond to `insert_opts()` which is expected to return an
        `InsertOpts` that contains options that will apply to all jobs of this
        kind. Insertion options provided as an argument to `insert()` override
        those returned by job args.

        For example:

            ```
            @dataclass
            class SortArgs:
                strings: list[str]

                kind: str = "sort"

                def to_json(self) -> str:
                    return json.dumps({"strings": self.strings})
            ```

        We recommend using `@dataclass` for job args since they should ideally
        be minimal sets of primitive properties with little other embellishment,
        and `@dataclass` provides a succinct way of accomplishing this.

        Returns an instance of `InsertResult`.
        """

        async with self.driver.executor() as exec:
            if not insert_opts:
                insert_opts = InsertOpts()
            insert_params, unique_opts = _make_driver_insert_params(args, insert_opts)

            return await self.__insert_job_with_unique(exec, insert_params, unique_opts)

    async def insert_tx(
        self, tx, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        """
        Inserts a new job for work given a job args implementation and insertion
        options (which may be omitted).

        This variant inserts a job in an open transaction. For example:

            ```
            with engine.begin() as session:
                insert_res = await client.insert_tx(
                    session,
                    SortArgs(strings=["whale", "tiger", "bear"]),
                )
            ```

        With insert opts:

            ```
            with engine.begin() as session:
                insert_res = await client.insert_tx(
                    session,
                    SortArgs(strings=["whale", "tiger", "bear"]),
                    insert_opts=riverqueue.InsertOpts(
                        max_attempts=17,
                        priority=3,
                        queue: "my_queue",
                        tags: ["custom"]
                    ),
                )
                insert_res.job # inserted job row
            ```
        """

        exec = self.driver.unwrap_executor(tx)
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = _make_driver_insert_params(args, insert_opts)

        return await self.__insert_job_with_unique(exec, insert_params, unique_opts)

    async def insert_many(self, args: List[JobArgs | InsertManyParams]) -> int:
        """
        Inserts many new jobs as part of a single batch operation for improved
        efficiency.

        Takes an array of job args or `InsertManyParams` which encapsulate job
        args and a paired `InsertOpts`.

        With job args:

            ```
            num_inserted = await client.insert_many([
                SimpleArgs(job_num: 1),
                SimpleArgs(job_num: 2)
            ])
            ```

        With `InsertManyParams`:

            ```
            num_inserted = await client.insert_many([
                InsertManyParams(args=SimpleArgs.new(job_num: 1), insert_opts=riverqueue.InsertOpts.new(max_attempts=5)),
                InsertManyParams(args=SimpleArgs.new(job_num: 2), insert_opts=riverqueue.InsertOpts.new(queue="high_priority"))
            ])
            ```

        Unique job insertion isn't supported with bulk insertion because it'd
        run the risk of major lock contention.

        Returns the number of jobs inserted.
        """

        async with self.driver.executor() as exec:
            return await exec.job_insert_many(_make_driver_insert_params_many(args))

    async def insert_many_tx(self, tx, args: List[JobArgs | InsertManyParams]) -> int:
        """
        Inserts many new jobs as part of a single batch operation for improved
        efficiency.

        This variant inserts a job in an open transaction. For example:

            ```
            with engine.begin() as session:
                num_inserted = await client.insert_many_tx(session, [
                    SimpleArgs(job_num: 1),
                    SimpleArgs(job_num: 2)
                ])
            ```

        With `InsertManyParams`:

            ```
            with engine.begin() as session:
                num_inserted = await client.insert_many_tx(session, [
                    InsertManyParams(args=SimpleArgs.new(job_num: 1), insert_opts=riverqueue.InsertOpts.new(max_attempts=5)),
                    InsertManyParams(args=SimpleArgs.new(job_num: 2), insert_opts=riverqueue.InsertOpts.new(queue="high_priority"))
                ])
            ```

        Unique job insertion isn't supported with bulk insertion because it'd
        run the risk of major lock contention.

        Returns the number of jobs inserted.
        """

        exec = self.driver.unwrap_executor(tx)
        return await exec.job_insert_many(_make_driver_insert_params_many(args))

    async def __insert_job_with_unique(
        self,
        exec: AsyncExecutorProtocol,
        insert_params: JobInsertParams,
        unique_opts: Optional[UniqueOpts],
    ) -> InsertResult:
        """
        Inserts a job, accounting for unique jobs whose insertion may be skipped
        if an equivalent job is already present.
        """

        get_params, unique_key = _build_unique_get_params_and_unique_key(
            insert_params, unique_opts
        )

        if not get_params or not unique_opts:
            return InsertResult(await exec.job_insert(insert_params))

        # fast path
        if (
            not unique_opts.by_state
            or unique_opts.by_state.sort == UNIQUE_STATES_DEFAULT
        ):
            job, unique_skipped_as_duplicate = await exec.job_insert_unique(
                insert_params, sha256(unique_key.encode("utf-8")).digest()
            )
            return InsertResult(
                job=job, unique_skipped_as_duplicated=unique_skipped_as_duplicate
            )

        async with exec.transaction():
            lock_key = "unique_key"
            lock_key += "kind=#{insert_params.kind}"
            lock_key += unique_key

            await exec.advisory_lock(
                _hash_lock_key(self.advisory_lock_prefix, lock_key)
            )

            existing_job = await exec.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return InsertResult(await exec.job_insert(insert_params))


class Client:
    """
    Provides a client for River that inserts jobs. Unlike the Go version of the
    River client, this one can insert jobs only. Jobs can only be worked from Go
    code, so job arg kinds and JSON encoding details must be shared between Ruby
    and Go code.

    Used in conjunction with a River driver like:

        ```
        import riverqueue
        from riverqueue.driver import riversqlalchemy

        engine = sqlalchemy.create_engine("postgresql://...")
        client = riverqueue.Client(riversqlalchemy.Driver(engine))
        ```
    """

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
        """
        Inserts a new job for work given a job args implementation and insertion
        options (which may be omitted).

        With job args only:

            ```
            insert_res = client.insert(
                SortArgs(strings=["whale", "tiger", "bear"]),
            )
            insert_res.job # inserted job row
            ```

        With insert opts:

            ```
            insert_res = client.insert(
                SortArgs(strings=["whale", "tiger", "bear"]),
                insert_opts=riverqueue.InsertOpts(
                    max_attempts=17,
                    priority=3,
                    queue: "my_queue",
                    tags: ["custom"]
                ),
            )
            insert_res.job # inserted job row
            ```

        Job arg implementations are expected to respond to:

            * `kind` is a unique string that identifies them the job in the
              database, and which a Go worker will recognize.

            * `to_json()` defines how the job will serialize to JSON, which of
              course will have to be parseable as an object in Go.

        They may also respond to `insert_opts()` which is expected to return an
        `InsertOpts` that contains options that will apply to all jobs of this
        kind. Insertion options provided as an argument to `insert()` override
        those returned by job args.

        For example:

            ```
            @dataclass
            class SortArgs:
                strings: list[str]

                kind: str = "sort"

                def to_json(self) -> str:
                    return json.dumps({"strings": self.strings})
            ```

        We recommend using `@dataclass` for job args since they should ideally
        be minimal sets of primitive properties with little other embellishment,
        and `@dataclass` provides a succinct way of accomplishing this.

        Returns an instance of `InsertResult`.
        """

        with self.driver.executor() as exec:
            if not insert_opts:
                insert_opts = InsertOpts()
            insert_params, unique_opts = _make_driver_insert_params(args, insert_opts)

            return self.__insert_job_with_unique(exec, insert_params, unique_opts)

    def insert_tx(
        self, tx, args: JobArgs, insert_opts: Optional[InsertOpts] = None
    ) -> InsertResult:
        """
        Inserts a new job for work given a job args implementation and insertion
        options (which may be omitted).

        This variant inserts a job in an open transaction. For example:

            ```
            with engine.begin() as session:
                insert_res = client.insert_tx(
                    session,
                    SortArgs(strings=["whale", "tiger", "bear"]),
                )
            ```

        With insert opts:

            ```
            with engine.begin() as session:
                insert_res = client.insert_tx(
                    session,
                    SortArgs(strings=["whale", "tiger", "bear"]),
                    insert_opts=riverqueue.InsertOpts(
                        max_attempts=17,
                        priority=3,
                        queue: "my_queue",
                        tags: ["custom"]
                    ),
                )
                insert_res.job # inserted job row
            ```
        """

        exec = self.driver.unwrap_executor(tx)
        if not insert_opts:
            insert_opts = InsertOpts()
        insert_params, unique_opts = _make_driver_insert_params(args, insert_opts)

        return self.__insert_job_with_unique(exec, insert_params, unique_opts)

    def insert_many(self, args: List[JobArgs | InsertManyParams]) -> int:
        """
        Inserts many new jobs as part of a single batch operation for improved
        efficiency.

        Takes an array of job args or `InsertManyParams` which encapsulate job
        args and a paired `InsertOpts`.

        With job args:

            ```
            num_inserted = client.insert_many([
                SimpleArgs(job_num: 1),
                SimpleArgs(job_num: 2)
            ])
            ```

        With `InsertManyParams`:

            ```
            num_inserted = client.insert_many([
                InsertManyParams(args=SimpleArgs.new(job_num: 1), insert_opts=riverqueue.InsertOpts.new(max_attempts=5)),
                InsertManyParams(args=SimpleArgs.new(job_num: 2), insert_opts=riverqueue.InsertOpts.new(queue="high_priority"))
            ])
            ```

        Unique job insertion isn't supported with bulk insertion because it'd
        run the risk of major lock contention.

        Returns the number of jobs inserted.
        """

        with self.driver.executor() as exec:
            return exec.job_insert_many(_make_driver_insert_params_many(args))

    def insert_many_tx(self, tx, args: List[JobArgs | InsertManyParams]) -> int:
        """
        Inserts many new jobs as part of a single batch operation for improved
        efficiency.

        This variant inserts a job in an open transaction. For example:

            ```
            with engine.begin() as session:
                num_inserted = client.insert_many_tx(session, [
                    SimpleArgs(job_num: 1),
                    SimpleArgs(job_num: 2)
                ])
            ```

        With `InsertManyParams`:

            ```
            with engine.begin() as session:
                num_inserted = client.insert_many_tx(session, [
                    InsertManyParams(args=SimpleArgs.new(job_num: 1), insert_opts=riverqueue.InsertOpts.new(max_attempts=5)),
                    InsertManyParams(args=SimpleArgs.new(job_num: 2), insert_opts=riverqueue.InsertOpts.new(queue="high_priority"))
                ])
            ```

        Unique job insertion isn't supported with bulk insertion because it'd
        run the risk of major lock contention.

        Returns the number of jobs inserted.
        """

        exec = self.driver.unwrap_executor(tx)
        return exec.job_insert_many(_make_driver_insert_params_many(args))

    def __insert_job_with_unique(
        self,
        exec: ExecutorProtocol,
        insert_params: JobInsertParams,
        unique_opts: Optional[UniqueOpts],
    ) -> InsertResult:
        """
        Inserts a job, accounting for unique jobs whose insertion may be skipped
        if an equivalent job is already present.
        """

        get_params, unique_key = _build_unique_get_params_and_unique_key(
            insert_params, unique_opts
        )

        if not get_params or not unique_opts:
            return InsertResult(exec.job_insert(insert_params))

        # fast path
        if (
            not unique_opts.by_state
            or unique_opts.by_state.sort == UNIQUE_STATES_DEFAULT
        ):
            job, unique_skipped_as_duplicate = exec.job_insert_unique(
                insert_params, sha256(unique_key.encode("utf-8")).digest()
            )
            return InsertResult(
                job=job, unique_skipped_as_duplicated=unique_skipped_as_duplicate
            )

        with exec.transaction():
            lock_key = "unique_key"
            lock_key += "kind=#{insert_params.kind}"
            lock_key += unique_key

            exec.advisory_lock(_hash_lock_key(self.advisory_lock_prefix, lock_key))

            existing_job = exec.job_get_by_kind_and_unique_properties(get_params)
            if existing_job:
                return InsertResult(existing_job, unique_skipped_as_duplicated=True)

            return InsertResult(exec.job_insert(insert_params))


def _build_unique_get_params_and_unique_key(
    insert_params: JobInsertParams,
    unique_opts: Optional[UniqueOpts],
) -> tuple[Optional[JobGetByKindAndUniquePropertiesParam], str]:
    """
    Builds driver get params and an advisory lock key from insert params and
    unique options for use during a unique job insertion.
    """

    if unique_opts is None:
        return (None, "")

    any_unique_opts = False
    get_params = JobGetByKindAndUniquePropertiesParam(kind=insert_params.kind)

    unique_key = ""

    if unique_opts.by_args:
        any_unique_opts = True
        get_params.by_args = True
        get_params.args = insert_params.args
        unique_key += f"&args={insert_params.args}"

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
        unique_key += f"&period={lower_period_bound.strftime('%FT%TZ')}"

    if unique_opts.by_queue:
        any_unique_opts = True
        get_params.by_queue = True
        get_params.queue = insert_params.queue
        unique_key += f"&queue={insert_params.queue}"

    if unique_opts.by_state:
        any_unique_opts = True
        get_params.by_state = True
        get_params.state = cast(list[str], unique_opts.by_state)
        unique_key += f"&state={','.join(unique_opts.by_state)}"
    else:
        get_params.state = UNIQUE_STATES_DEFAULT
        unique_key += f"&state={','.join(UNIQUE_STATES_DEFAULT)}"

    if not any_unique_opts:
        return (None, "")

    return (get_params, unique_key)


def _check_advisory_lock_prefix_bounds(
    advisory_lock_prefix: Optional[int],
) -> Optional[int]:
    """
    Checks that an advisory lock prefix fits in 4 bytes, which is the maximum
    space reserved for one.
    """

    if advisory_lock_prefix:
        # We only reserve 4 bytes for the prefix, so make sure the given one
        # properly fits. This will error in case that's not the case.
        advisory_lock_prefix.to_bytes(4)
    return advisory_lock_prefix


def _hash_lock_key(advisory_lock_prefix: Optional[int], lock_key: str) -> int:
    """
    Generates an FNV-1 hash from the given lock key string suitable for use with
    a PG advisory lock while checking for the existence of a unique job.
    """

    if advisory_lock_prefix is None:
        lock_key_hash = fnv1_hash(lock_key.encode("utf-8"), 64)
    else:
        prefix = advisory_lock_prefix
        lock_key_hash = (prefix << 32) | fnv1_hash(lock_key.encode("utf-8"), 32)

    return _uint64_to_int64(lock_key_hash)


def _make_driver_insert_params(
    args: JobArgs,
    insert_opts: InsertOpts,
    is_insert_many: bool = False,
) -> Tuple[JobInsertParams, Optional[UniqueOpts]]:
    """
    Converts user-land job args and insert options to insert params for an
    underlying driver.
    """

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


def _make_driver_insert_params_many(
    args: List[JobArgs | InsertManyParams],
) -> List[JobInsertParams]:
    return [
        _make_driver_insert_params(
            arg.args, arg.insert_opts or InsertOpts(), is_insert_many=True
        )[0]
        if isinstance(arg, InsertManyParams)
        else _make_driver_insert_params(arg, InsertOpts(), is_insert_many=True)[0]
        for arg in args
    ]


def _truncate_time(time, interval_seconds) -> datetime:
    return datetime.fromtimestamp(
        (time.timestamp() // interval_seconds) * interval_seconds, tz=timezone.utc
    )


def _uint64_to_int64(uint64):
    # Packs a uint64 then unpacks to int64 to fit within Postgres bigint
    return (uint64 + (1 << 63)) % (1 << 64) - (1 << 63)


tag_re = re.compile(r"\A[\w][\w\-]+[\w]\Z")


def _validate_tags(tags: list[str]) -> list[str]:
    for tag in tags:
        assert (
            len(tag) <= 255 and tag_re.match(tag)
        ), f"tags should be less than 255 characters in length and match regex {tag_re.pattern}"
    return tags
