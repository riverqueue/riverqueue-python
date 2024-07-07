from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional

from riverqueue.job import JobState


@dataclass
class InsertOpts:
    """
    Options for job insertion, and which can be provided by implementing
    `insert_opts()` on job args, or specified as a parameter on `insert()` or
    `insert_many()`.
    """

    max_attempts: Optional[int] = None
    """
    The maximum number of total attempts (including both the original run and
    all retries) before a job is abandoned and set as discarded.
    """

    priority: Optional[int] = None
    """
    The priority of the job, with 1 being the highest priority and 4 being the
    lowest. When fetching available jobs to work, the highest priority jobs
    will always be fetched before any lower priority jobs are fetched. Note
    that if your workers are swamped with more high-priority jobs then they
    can handle, lower priority jobs may not be fetched.

    Defaults to `PRIORITY_DEFAULT`.
    """

    queue: Optional[str] = None
    """
    The name of the job queue in which to insert the job.

    Defaults to `QUEUE_DEFAULT`.
    """

    scheduled_at: Optional[datetime] = None
    """
    A time in future at which to schedule the job (i.e. in cases where it
    shouldn't be run immediately). The job is guaranteed not to run before
    this time, but may run slightly after depending on the number of other
    scheduled jobs and how busy the queue is.

    Use of this option generally only makes sense when passing options into
    Insert rather than when a job args is returning `insert_opts()`, however,
    it will work in both cases.
    """

    tags: Optional[list[Any]] = None
    """
    An arbitrary list of keywords to add to the job. They have no functional
    behavior and are meant entirely as a user-specified construct to help
    group and categorize jobs.

    If tags are specified from both a job args override and from options on
    Insert, the latter takes precedence. Tags are not merged.
    """

    unique_opts: Optional["UniqueOpts"] = None
    """
    Options relating to job uniqueness. No unique options means that the job is
    never treated as unique.
    """


@dataclass
class UniqueOpts:
    """
    Parameters for uniqueness for a job.

    If all properties are nil, no uniqueness at is enforced. As each property is
    initialized, it's added as a dimension on the uniqueness matrix, and with
    any property on, the job's kind always counts toward uniqueness.

    So for example, if only `by_queue()` is on, then for the given job kind,
    only a single instance is allowed in any given queue, regardless of other
    properties on the job. If both `by_args()` and `by_queue()` are on, then for
    the given job kind, a single instance is allowed for each combination of
    args and queues. If either args or queue is changed on a new job, it's
    allowed to be inserted as a new job.

    Uniquenes is checked at insert time by taking a Postgres advisory lock,
    doing a look up for an equivalent row, and inserting only if none was found.
    There's no database-level mechanism that guarantees jobs stay unique, so if
    an equivalent row is inserted out of band (or batch inserted, where a unique
    check doesn't occur), it's conceivable that duplicates could coexist.
    """

    by_args: Optional[Literal[True]] = None
    """
    Indicates that uniqueness should be enforced for any specific instance of
    encoded args for a job.

    Default is false, meaning that as long as any other unique property is
    enabled, uniqueness will be enforced for a kind regardless of input args.
    """

    by_period: Optional[int] = None
    """
    Defines uniqueness within a given period. On an insert time is rounded
    down to the nearest multiple of the given period, and a job is only
    inserted if there isn't an existing job that will run between then and the
    next multiple of the period.

    The period should be specified in seconds. So a job that's unique every 15
    minute period would have a value of 900.

    Default is no unique period, meaning that as long as any other unique
    property is enabled, uniqueness will be enforced across all jobs of the
    kind in the database, regardless of when they were scheduled.
    """

    by_queue: Optional[Literal[True]] = None
    """
    Indicates that uniqueness should be enforced within each queue.

    Default is false, meaning that as long as any other unique property is
    enabled, uniqueness will be enforced for a kind across all queues.
    """

    by_state: Optional[list[JobState]] = None
    """
    Indicates that uniqueness should be enforced across any of the states in
    the given set. For example, if the given states were `(scheduled,
    running)` then a new job could be inserted even if one of the same kind
    was already being worked by the queue (new jobs are inserted as
    `available`).

    Unlike other unique options, ByState gets a default when it's not set for
    user convenience. The default is equivalent to:

        ```
        by_state=[JobState::AVAILABLE, JobState::COMPLETED, JobState::RUNNING, JobState::RETRYABLE, JobState::SCHEDULED]
        ```

    With this setting, any jobs of the same kind that have been completed or
    discarded, but not yet cleaned out by the system, won't count towards the
    uniqueness of a new insert.
    """
