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

```shell
$ rye test
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