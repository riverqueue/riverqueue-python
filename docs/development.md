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

## Generate sqlc code

```shell
$ make generate
```

## Publish package

1. Pull existing `master` and tags, choose a version, and create a branch:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=v0.x.0
    git checkout -b $USER-$VERSION
    ```

2. Update `CHANGELOG.md` and `pyproject.toml` to the new version number, and open a pull request. Get it reviewed and merged.

3. Pull down the merged pull request, build the project (goes to `dist/`), publish it to PyPI, cut a tag for the new version, and push it to GitHub:

    ```shell
    git pull origin master

    rye build
    rye publish

    git tag $VERSION
    git push --tags

    # or else PyPI will keep uploading old versions forever
    rm dist/*
    ```
4. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/riverqueue-python/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.
