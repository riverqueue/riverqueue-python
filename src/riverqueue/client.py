from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import re
from typing import (
    Optional,
    Protocol,
    List,
    runtime_checkable,
)
import json

from riverqueue.insert_opts import InsertOpts, UniqueOpts

from .driver import (
    JobInsertParams,
    DriverProtocol,
)
from .driver.driver_protocol import AsyncDriverProtocol, ExecutorProtocol
from .job import Job, JobState

JOB_STATE_BIT_POSITIONS = {
    JobState.AVAILABLE: 7,
    JobState.CANCELLED: 6,
    JobState.COMPLETED: 5,
    JobState.DISCARDED: 4,
    JobState.PENDING: 3,
    JobState.RETRYABLE: 2,
    JobState.RUNNING: 1,
    JobState.SCHEDULED: 0,
}
"""
Maps job states to bit positions in a unique bitmask.
"""

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

UNIQUE_STATES_DEFAULT: list[JobState] = [
    JobState.AVAILABLE,
    JobState.COMPLETED,
    JobState.PENDING,
    JobState.RUNNING,
    JobState.RETRYABLE,
    JobState.SCHEDULED,
]
"""
Default job states included during a unique job insertion.
"""

UNIQUE_STATES_REQUIRED: list[JobState] = [
    JobState.AVAILABLE,
    JobState.PENDING,
    JobState.RUNNING,
    JobState.SCHEDULED,
]
"""
Job states required when customizing the state list for unique job insertion.
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

    def __init__(self, driver: AsyncDriverProtocol):
        self.driver = driver

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

        if not insert_opts:
            insert_opts = InsertOpts()

        return (await self.insert_many([InsertManyParams(args, insert_opts)]))[0]

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

        if not insert_opts:
            insert_opts = InsertOpts()

        return (await self.insert_many_tx(tx, [InsertManyParams(args, insert_opts)]))[0]

    async def insert_many(
        self, args: List[JobArgs | InsertManyParams]
    ) -> list[InsertResult]:
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
            res = await exec.job_insert_many(_make_driver_insert_params_many(args))
            return _to_insert_results(res)

    async def insert_many_tx(
        self, tx, args: List[JobArgs | InsertManyParams]
    ) -> list[InsertResult]:
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
        res = await exec.job_insert_many(_make_driver_insert_params_many(args))
        return _to_insert_results(res)


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

    def __init__(self, driver: DriverProtocol):
        self.driver = driver

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

        if not insert_opts:
            insert_opts = InsertOpts()

        return self.insert_many([InsertManyParams(args, insert_opts)])[0]

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

        if not insert_opts:
            insert_opts = InsertOpts()

        return self.insert_many_tx(tx, [InsertManyParams(args, insert_opts)])[0]

    def insert_many(self, args: List[JobArgs | InsertManyParams]) -> list[InsertResult]:
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
            return self._insert_many_exec(exec, args)

    def insert_many_tx(
        self, tx, args: List[JobArgs | InsertManyParams]
    ) -> list[InsertResult]:
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

        return self._insert_many_exec(self.driver.unwrap_executor(tx), args)

    def _insert_many_exec(
        self, exec: ExecutorProtocol, args: List[JobArgs | InsertManyParams]
    ) -> list[InsertResult]:
        res = exec.job_insert_many(_make_driver_insert_params_many(args))
        return _to_insert_results(res)


def _build_unique_key_and_bitmask(
    insert_params: JobInsertParams,
    unique_opts: UniqueOpts,
) -> tuple[Optional[bytes], Optional[bytes]]:
    """
    Builds driver get params and a unique key from insert params and unique
    options for use during a job insertion.
    """
    any_unique_opts = False

    unique_key = ""

    if not unique_opts.exclude_kind:
        unique_key += f"&kind={insert_params.kind}"

    if unique_opts.by_args:
        any_unique_opts = True

        # Re-parse the args JSON for sorting and potentially filtering:
        args_dict = json.loads(insert_params.args)

        args_to_include = args_dict
        if unique_opts.by_args is not True:
            # Filter to include only the specified keys:
            args_to_include = {
                key: args_dict[key] for key in unique_opts.by_args if key in args_dict
            }

        # Serialize with sorted keys and append to unique key:
        sorted_args = json.dumps(args_to_include, sort_keys=True)
        unique_key += f"&args={sorted_args}"

    if unique_opts.by_period:
        lower_period_bound = _truncate_time(
            datetime.now(timezone.utc), unique_opts.by_period
        )

        any_unique_opts = True
        unique_key += f"&period={lower_period_bound.strftime('%FT%TZ')}"

    if unique_opts.by_queue:
        any_unique_opts = True
        unique_key += f"&queue={insert_params.queue}"

    if unique_opts.by_state:
        any_unique_opts = True
        unique_key += f"&state={','.join(unique_opts.by_state)}"
    else:
        unique_key += f"&state={','.join(UNIQUE_STATES_DEFAULT)}"

    if not any_unique_opts:
        return (None, None)

    unique_key_hash = sha256(unique_key.encode("utf-8")).digest()
    unique_states = _validate_unique_states(
        unique_opts.by_state or UNIQUE_STATES_DEFAULT
    )

    return unique_key_hash, unique_bitmask_from_states(unique_states)


def _make_driver_insert_params(
    args: JobArgs, insert_opts: InsertOpts
) -> JobInsertParams:
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

    unique_opts = insert_opts.unique_opts or args_insert_opts.unique_opts
    if unique_opts:
        unique_key, unique_states = _build_unique_key_and_bitmask(
            insert_params, unique_opts
        )
        insert_params.unique_key = unique_key
        insert_params.unique_states = unique_states

    return insert_params


def _make_driver_insert_params_many(
    args: List[JobArgs | InsertManyParams],
) -> List[JobInsertParams]:
    return [
        _make_driver_insert_params(arg.args, arg.insert_opts or InsertOpts())
        if isinstance(arg, InsertManyParams)
        else _make_driver_insert_params(arg, InsertOpts())
        for arg in args
    ]


def _truncate_time(time, interval_seconds) -> datetime:
    return datetime.fromtimestamp(
        (time.timestamp() // interval_seconds) * interval_seconds, tz=timezone.utc
    )


def _to_insert_results(results: list[tuple[Job, bool]]) -> list[InsertResult]:
    return [
        InsertResult(job, unique_skipped_as_duplicated)
        for job, unique_skipped_as_duplicated in results
    ]


def unique_bitmask_from_states(states: list[JobState]) -> bytes:
    val = 0

    for state in states:
        bit_index = JOB_STATE_BIT_POSITIONS[state]

        bit_position = 7 - (bit_index % 8)
        val |= 1 << bit_position

    return val.to_bytes(1, "big")  # Returns bytes like b'\xf5'


def unique_bitmask_to_states(mask: str) -> list[JobState]:
    states = []

    # This logic differs a bit from the above because we're working with a string
    # of Postgres' bit(8) representation where the bit numbering is reversed
    # (MSB on the right).
    for state, bit_index in JOB_STATE_BIT_POSITIONS.items():
        if mask[bit_index] == "1":
            states.append(state)

    return sorted(states)


tag_re = re.compile(r"\A[\w][\w\-]+[\w]\Z")


def _validate_tags(tags: list[str]) -> list[str]:
    for tag in tags:
        assert (
            len(tag) <= 255 and tag_re.match(tag)
        ), f"tags should be less than 255 characters in length and match regex {tag_re.pattern}"
    return tags


def _validate_unique_states(states: list[JobState]) -> list[JobState]:
    for required_state in UNIQUE_STATES_REQUIRED:
        if required_state not in states:
            raise ValueError(
                f"by_state should include required state '{required_state}'"
            )

    return states
