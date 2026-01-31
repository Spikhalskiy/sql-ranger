# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased]

## [0.0.3] - 2026-01-31

### Added

- Support for multiple partition columns (e.g., day, month, year)

### Breaking Changes

- `PartitionColumn` has been renamed to `TablePartition`.
- The previous `DatePartitionColumn` class has been renamed to `DateTablePartition`; a new `DatePartitionColumn` dataclass has been introduced and is used within `DateTablePartition`.
- The `max_date_range_days` parameter has been replaced by `max_date_range`, which now expects a `datetime.timedelta` value.
- An `enforced_level` parameter has been added to control how strictly partitioning is enforced.
- Partition configuration now accepts lists of columns instead of a single column, which may require updating existing configurations.
- The enum value `MISSING_DAY_FILTER` has been renamed to `MISSING_PARTITION_FILTER`.
- The enum value `DAY_FILTER_WITH_FUNCTION` has been renamed to `PARTITION_COLUMN_WITH_FUNCTION`.
- The `PartitionViolation.estimated_days` field has been renamed to `estimated_range` and its type changed from `int` (days) to `datetime.timedelta`.

### Fixed

- Improves handling of date ranges for OR conditions.

## [0.0.2] - 2026-01-15

### Added

- Flexible configuration for partition column names
- Support for custom date formats for range checks

### Fixed

- Support for enforcing partitions in hierarchical queries

## [0.0.1] - 2026-01-01

### Added

- Initial minimal working implementation with hardcoded "day" partition column support.