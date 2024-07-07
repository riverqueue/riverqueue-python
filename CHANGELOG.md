# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.2] - 2024-07-06

### Changed

- `UniqueOpts.by_state` now has the stronger type of `list[JobState]` (the enum) instead of `list[str]`. [PR #32](https://github.com/riverqueue/riverqueue-python/pull/32).

## [0.6.1] - 2024-07-06

### Fixed

- `riverqueue.AttemptError` can now round trip to and from JSON properly, including its `at` timestamp. [PR #31](https://github.com/riverqueue/riverqueue-python/pull/31).

## [0.6.0] - 2024-07-06

### Added

- Add doc strings for most of the public API. [PR #27](https://github.com/riverqueue/riverqueue-python/pull/27).
- Add `riverqueue.AttemptError` data class to represent errors on a job row. [PR #27](https://github.com/riverqueue/riverqueue-python/pull/27).

## [0.5.0] - 2024-07-06

### Changed

- Use real enum for `JobState` instead of many constant. This is a breaking change, but the job state constants have existed for only a short time. [PR #25](https://github.com/riverqueue/riverqueue-python/pull/25).
- `riverqueue.Job`'s properties are now fully defined and typed. [PR #26](https://github.com/riverqueue/riverqueue-python/pull/26).

## [0.4.0] - 2024-07-05

### Changed

- Tags are now limited to 255 characters in length, and should match the regex `\A[\w][\w\-]+[\w]\z` (importantly, they can't contain commas). [PR #23](https://github.com/riverqueue/riverqueue-python/pull/23).

## [0.3.0] - 2024-07-04

### Added

- Implement `insert_many` and `insert_many_tx`. [PR #22](https://github.com/riverqueue/riverqueue-python/pull/22).

## [0.2.0] - 2024-07-04

### Changed

- Rename `Args` to `JobArgs` and add `JobArgsWithInsertOpts` protocol. [PR #20](https://github.com/riverqueue/riverqueue-python/pull/20).

## [0.1.2] - 2024-07-04

### Changed

- Add usage instructions README, add job state constants, and change return value of `insert_many()` and `insert_many_tx()` to an integer instead of a list of jobs. [PR #19](https://github.com/riverqueue/riverqueue-python/pull/19).

## [0.1.1] - 2024-07-04

### Fixed

- Fix `pyproject.toml` description and add various URLs like to homepage, docs, and GitHub repositories. [PR #18](https://github.com/riverqueue/riverqueue-python/pull/18).

## [0.1.0] - 2024-07-04

### Added

- Initial release, supporting insertion through [SQLAlchemy](https://www.sqlalchemy.org/) and its underlying Postgres drivers like [`psycopg2`](https://pypi.org/project/psycopg2/) or [`asyncpg`](https://github.com/MagicStack/asyncpg) (for async).