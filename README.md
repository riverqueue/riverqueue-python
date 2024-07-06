# River client for Python

An insert-only Python client for [River](https://github.com/riverqueue/river) packaged in the [`riverqueue` package on PyPI](https://pypi.org/project/riverqueue/). Allows jobs to be inserted in Python and run by a Go worker, but doesn't support working jobs in Python.

## Basic usage

Your project should bundle the [`riverqueue` package](https://pypi.org/project/riverqueue/) in its dependencies. How to go about this will depend on your toolchain, but for example in [Rye](https://github.com/astral-sh/rye), it'd look like:

```shell
rye add riverqueue
```

Initialize a client with:

```python
import riverqueue
from riverqueue.driver import riversqlalchemy

engine = sqlalchemy.create_engine("postgresql://...")
client = riverqueue.Client(riversqlalchemy.Driver(engine))
```

Define a job and insert it:

```python
@dataclass
class SortArgs:
    strings: list[str]

    kind: str = "sort"

    def to_json(self) -> str:
        return json.dumps({"strings": self.strings})

insert_res = client.insert(
    SortArgs(strings=["whale", "tiger", "bear"]),
)
insert_res.job # inserted job row
```

Job args should comply with the `riverqueue.JobArgs` [protocol](https://peps.python.org/pep-0544/):

```python
class JobArgs(Protocol):
    kind: str

    def to_json(self) -> str:
        pass
```

* `kind` is a unique string that identifies them the job in the database, and which a Go worker will recognize.
* `to_json()` defines how the job will serialize to JSON, which of course will have to be parseable as an object in Go.

They may also respond to `insert_opts()` with an instance of `InsertOpts` to define insertion options that'll be used for all jobs of the kind.

We recommend using [`dataclasses`](https://docs.python.org/3/library/dataclasses.html) for job args since they should ideally be minimal sets of primitive properties with little other embellishment, and `dataclasses` provide a succinct way of accomplishing this.

## Insertion options

Inserts take an `insert_opts` parameter to customize features of the inserted job:

```python
insert_res = client.insert(
    SortArgs(strings=["whale", "tiger", "bear"]),
    insert_opts=riverqueue.InsertOpts(
        max_attempts=17,
        priority=3,
        queue: "my_queue",
        tags: ["custom"]
    ),
)
```

## Inserting unique jobs

[Unique jobs](https://riverqueue.com/docs/unique-jobs) are supported through `InsertOpts.unique_opts()`, and can be made unique by args, period, queue, and state. If a job matching unique properties is found on insert, the insert is skipped and the existing job returned.

```python
insert_res = client.insert(
    SortArgs(strings=["whale", "tiger", "bear"]),
    insert_opts=riverqueue.InsertOpts(
        unique_opts=riverqueue.UniqueOpts(
            by_args: True,
            by_period=15*60,
            by_queue: True,
            by_state: [riverqueue.JobState.AVAILABLE]
        )
    ),
)

# contains either a newly inserted job, or an existing one if insertion was skipped
insert_res.job

# true if insertion was skipped
insert_res.unique_skipped_as_duplicated
```

### Custom advisory lock prefix

Unique job insertion takes a Postgres advisory lock to make sure that its uniqueness check still works even if two conflicting insert operations are occurring in parallel. Postgres advisory locks share a global 64-bit namespace, which is a large enough space that it's unlikely for two advisory locks to ever conflict, but to _guarantee_ that River's advisory locks never interfere with an application's, River can be configured with a 32-bit advisory lock prefix which it will use for all its locks:

```python
client = riverqueue.Client(riversqlalchemy.Driver(engine), advisory_lock_prefix: 123456)
```

Doing so has the downside of leaving only 32 bits for River's locks (64 bits total - 32-bit prefix), making them somewhat more likely to conflict with each other.

## Inserting jobs in bulk

Use `#insert_many` to bulk insert jobs as a single operation for improved efficiency:

```python
num_inserted = client.insert_many([
    SimpleArgs(job_num: 1),
    SimpleArgs(job_num: 2)
])
```

Or with `InsertManyParams`, which may include insertion options:

```python
num_inserted = client.insert_many([
    InsertManyParams(args=SimpleArgs.new(job_num: 1), insert_opts=riverqueue.InsertOpts.new(max_attempts=5)),
    InsertManyParams(args=SimpleArgs.new(job_num: 2), insert_opts=riverqueue.InsertOpts.new(queue="high_priority"))
])
```

## Inserting in a transaction

To insert jobs in a transaction, open one in your driver, and pass it as the first argument to `insert_tx()` or `insert_many_tx()`:

```python
with engine.begin() as session:
    insert_res = client.insert_tx(
        session,
        SortArgs(strings=["whale", "tiger", "bear"]),
    )
```

## Asynchronous I/O (asyncio)

The package supports River's [`asyncio` (asynchronous I/O)](https://docs.python.org/3/library/asyncio.html) through an alternate `AsyncClient` and `riversqlalchemy.AsyncDriver`. You'll need to make sure to use SQLAlchemy's alternative async engine and an asynchronous Postgres driver like [`asyncpg`](https://github.com/MagicStack/asyncpg), but otherwise usage looks very similar to use without async:

```python
engine = sqlalchemy.ext.asyncio.create_async_engine("postgresql+asyncpg://...")
client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))

insert_res = await client.insert(
    SortArgs(strings=["whale", "tiger", "bear"]),
)
```

With a transaction:

```python
async with engine.begin() as session:
    insert_res = await client.insert_tx(
        session,
        SortArgs(strings=["whale", "tiger", "bear"]),
    )
```

## MyPy and type checking

The package exports a `py.typed` file to indicate that it's typed, so you should be able to use [MyPy](https://mypy-lang.org/) to include it in static analysis.

## Drivers

### SQLAlchemy

Our read is that [SQLAlchemy](https://www.sqlalchemy.org/) is the dominant ORM in the Python ecosystem, so it's the only driver available for River. Under the hood of SQLAlchemy, projects will also need a Postgres driver like [`psycopg2`](https://pypi.org/project/psycopg2/) or [`asyncpg`](https://github.com/MagicStack/asyncpg) (for async).

River's driver system should enable integration with other ORMs, so let us know if there's a good reason you need one, and we'll consider it.

## Development

See [development](./docs/development.md).
