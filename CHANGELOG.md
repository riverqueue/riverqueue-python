# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **Breaking change:** The return type of `Client#insert_many` and `Client#insert_many_tx` has been changed. Rather than returning just the number of rows inserted, it returns an array of all the `InsertResult` values for each inserted row. Unique conflicts which are skipped as duplicates are indicated in the same fashion as single inserts (the `unique_skipped_as_duplicated` attribute), and in such cases the conflicting row will be returned instead. [PR #38](https://github.com/riverqueue/riverqueue-python/pull/38).
- **Breaking change:** Unique jobs no longer allow total customization of their states when using the `by_state` option. The pending, scheduled, available, and running states are required whenever customizing this list.

### Added

- The `UniqueOpts` class gains an `exclude_kind` option for cases where uniqueness needs to be guaranteed across multiple job types.
- Unique jobs utilizing `by_args` can now also opt to have a subset of the job's arguments considered for uniqueness. For example, you could choose to consider only the `customer_id` field while ignoring the other fields:

  ```python
  UniqueOpts(by_args=["customer_id"])
  ```

  Any fields considered in uniqueness are also sorted alphabetically in order to guarantee a consistent result across implementations, even if the encoded JSON isn't sorted consistently.

### Changed

- Unique jobs have been improved to allow bulk insertion of unique jobs via `Client#insert_many`.

  This updated implementation is significantly faster due to the removal of advisory locks in favor of an index-backed uniqueness system, while allowing some flexibility in which job states are considered. However, not all states may be removed from consideration when using the `by_state` option; pending, scheduled, available, and running states are required whenever customizing this list.

## [0.7.0] - 2024-07-30

### Changed

- Now compatible with "fast path" unique job insertion that uses a unique index instead of advisory lock and fetch [as introduced in River #451](https://github.com/riverqueue/river/pull/451). [PR #36](https://github.com/riverqueue/riverqueue-python/pull/36).

## [0.6.3] - 2024-07-08

### Fixed

- Various Python syntax fixes in README examples. [PR #34](https://github.com/riverqueue/riverqueue-python/pull/34).

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
