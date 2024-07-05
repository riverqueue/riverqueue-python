# riverqueue-python development

## Install dependencies

The project uses [Rye](https://github.com/astral-sh/rye) for its development toolchain and to run development targets. Install it with:

```shell
$ curl -sSf https://rye.astral.sh/get | bash
```

Or via Homebrew:

```shell
$ brew install rye
```

Then use it to set up a virtual environment:

```shell
$ rye sync
```

## Run tests

Create a test database and migrate with River's CLI:

```shell
$ go install github.com/riverqueue/river/cmd/river
$ createdb river_test
$ river migrate-up --database-url "postgres://localhost/river_test"
```

Run all tests:

```shell
$ rye test
```

Run a specific test (or without `-k` option for all tests in a single file):

```shell
rye test -- tests/driver/sqlalchemy/sqlalchemy_driver_test.py -k test_insert_with_unique_opts_by_queue
```

## Run lint

```shell
$ rye lint
```

## Run type check (Mypy)

```shell
$ make typecheck
```

## Format code

```shell
$ rye fmt
```

Rye uses [Ruff](https://github.com/astral-sh/ruff) under the hood for code formatting.
