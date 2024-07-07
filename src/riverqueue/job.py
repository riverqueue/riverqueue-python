from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Optional


class JobState(str, Enum):
    """
    The state of a job. Jobs start their lifecycle as either `AVAILABLE` or
    `SCHEDULED`, and if all goes well, transition to `COMPLETED` after they're
    worked.
    """

    AVAILABLE = "available"
    """
	The state for jobs that are immediately eligible to be worked.
    """

    CANCELLED = "cancelled"
    """
	The state for jobs that have been manually cancelled by user request.

	Cancelled jobs are reaped by the job cleaner service after a configured
	amount of time (default 24 hours).
    """

    COMPLETED = "completed"
    """
	The state for jobs that have successfully run to completion.

	Completed jobs are reaped by the job cleaner service after a configured
	amount of time (default 24 hours).
    """

    DISCARDED = "discarded"
    """
	The state for jobs that have errored enough times that they're no longer
	eligible to be retried. Manual user invention is required for them to be
	tried again.

	Discarded jobs are reaped by the job cleaner service after a configured
	amount of time (default 7 days).
    """

    PENDING = "pending"
    """
	A state for jobs to be parked while waiting for some external action before
	they can be worked. Jobs in pending will never be worked or deleted unless
	moved out of this state by the user.
    """

    RETRYABLE = "retryable"
    """
	The state for jobs that have errored, but will be retried.

	The job scheduler service changes them to `AVAILABLE` when they're ready to
	be worked (their `scheduled_at` timestamp comes due).

	Jobs that will be retried very soon in the future may be changed to
	`AVAILABLE` immediately instead of `RETRYABLE` so that they don't have to
	wait for the job scheduler to run.
    """

    RUNNING = "running"
    """
	Jobs which are actively running.

	If River can't update state of a running job (in the case of a program
	crash, underlying hardware failure, or job that doesn't return from its Work
	function), that job will be left as `RUNNING`, and will require a pass by
	the job rescuer service to be set back to `AVAILABLE` and be eligible for
	another run attempt.
    """

    SCHEDULED = "scheduled"
    """
	The state for jobs that are scheduled for the future.

	The job scheduler service changes them to `AVAILABLE` when they're ready to
	be worked (their `scheduled_at` timestamp comes due).
    """


@dataclass
class Job:
    """
    Contains the properties of a job that are persisted to the database.
    """

    id: int
    """
    ID of the job. Generated as part of a Postgres sequence and generally
    ascending in nature, but there may be gaps in it as transactions roll
    back.
    """

    args: dict[str, Any]
    """
    The job's args as a dictionary decoded from JSON.
    """

    attempt: int
    """
    The attempt number of the job. Jobs are inserted at 0, the number is
    incremented to 1 the first time work its worked, and may increment further
    if it's either snoozed or errors.
    """

    attempted_at: Optional[datetime]
    """
    The time that the job was last worked. Starts out as `nil` on a new insert.
    """

    attempted_by: Optional[list[str]]
    """
    The set of worker IDs that have worked this job. A worker ID differs between
    different programs, but is shared by all executors within any given one.
    (i.e. Different Go processes have different IDs, but IDs are shared within
    any given process.) A process generates a new ID based on host and current
    time when it starts up.
    """

    created_at: datetime
    """
    When the job record was created.
    """

    errors: Optional[list["AttemptError"]]
    """
    A set of errors that occurred when the job was worked, one for each attempt.
    Ordered from earliest error to the latest error.
    """

    finalized_at: Optional[datetime]
    """
    The time at which the job was "finalized", meaning it was either completed
    successfully or errored for the last time such that it'll no longer be
    retried.
    """

    kind: str
    """
    Kind uniquely identifies the type of job and instructs which worker should
    work it. It is set at insertion time via `#kind` on job args.
    """

    max_attempts: int
    """
    The maximum number of attempts that the job will be tried before it errors
    for the last time and will no longer be worked.
    """

    metadata: dict[str, Any]
    """
    Arbitrary metadata associated with the job.
    """

    priority: int
    """
    The priority of the job, with 1 being the highest priority and 4 being the
    lowest. When fetching available jobs to work, the highest priority jobs will
    always be fetched before any lower priority jobs are fetched. Note that if
    your workers are swamped with more high-priority jobs then they can handle,
    lower priority jobs may not be fetched.
    """

    queue: str
    """
    The name of the queue where the job will be worked. Queues can be configured
    independently and be used to isolate jobs.
    """

    scheduled_at: datetime
    """
    When the job is scheduled to become available to be worked. Jobs default to
    running immediately, but may be scheduled for the future when they're
    inserted. They may also be scheduled for later because they were snoozed or
    because they errored and have additional retry attempts remaining.
    """

    state: JobState
    """
    The state of job like `available` or `completed`. Jobs are `available` when
    they're first inserted.
    """

    tags: list[str]
    """
    Tags are an arbitrary list of keywords to add to the job. They have no
    functional behavior and are meant entirely as a user-specified construct to
    help group and categorize jobs.
    """


@dataclass
class AttemptError:
    """
    A failed job work attempt containing information about the error or panic
    that occurred.
    """

    at: datetime
    """
    The time at which the error occurred.
    """

    attempt: int
    """
    The attempt number on which the error occurred (maps to `attempt()` on a job
    row).
    """

    error: str
    """
    Contains the stringified error of an error returned from a job or a panic
    value in case of a panic.
    """

    trace: str
    """
    Contains a stack trace from a job that panicked. The trace is produced by
    invoking `debug.Trace()` in Go.
    """

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "AttemptError":
        return AttemptError(
            at=datetime.fromisoformat(data["at"]),
            attempt=data["attempt"],
            error=data["error"],
            trace=data["trace"],
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "at": self.at.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "attempt": self.attempt,
                "error": self.error,
                "trace": self.trace,
            }
        )
