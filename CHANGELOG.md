# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2024-07-04

### Changed

- Rename `Args` to `JobArgs` and add `JobArgsWithInsertOpts` protocol. [PR #20](https://github.com/riverqueue/river/pull/20).

## [0.1.2] - 2024-07-04

### Changed

- Add usage instructions README, add job state constants, and change return value of `insert_many()` and `insert_many_tx()` to an integer instead of a list of jobs. [PR #19](https://github.com/riverqueue/river/pull/19).

## [0.1.1] - 2024-07-04

### Fixed

- Fix `pyproject.toml` description and add various URLs like to homepage, docs, and GitHub repositories. [PR #18](https://github.com/riverqueue/river/pull/18).

## [0.1.0] - 2024-07-04

### Added

- Initial release, supporting insertion through [SQLAlchemy](https://www.sqlalchemy.org/) and its underlying Postgres drivers like [`psycopg2`](https://pypi.org/project/psycopg2/) or [`asyncpg`](https://github.com/MagicStack/asyncpg) (for async).