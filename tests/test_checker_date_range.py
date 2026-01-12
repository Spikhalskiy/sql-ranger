"""Tests for date range estimations in partition_checker module."""
from datetime import timedelta

import pytest

from sqlranger.checker import (
    DatePartitionColumn,
    DateTablePartition,
    PartitionChecker,
    PartitionViolationType,
    TablePartition,
)


class TestDateRangeEstimation:
    """Test suite for date range estimation functionality."""

    def test_checker_with_date_partition_column_excessive_range(self):
        """Test DatePartitionColumn enforces max_date_range_days."""
        sql = """
              SELECT day, SUM(quantity)
              FROM gridhive.fact.sales_history
              WHERE day BETWEEN '2021-01-01' AND '2021-12-31' \
              """
        partition_cols = [
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=100),
            )
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE
        assert results[0].estimated_range > timedelta(days=100)

    def test_estimate_range_with_between(self):
        """Test date range estimation with BETWEEN clause."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=20),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations
        # 2021-09-13 to 2021-09-26 is 14 days (inclusive)

    def test_estimate_range_with_gte_and_lte(self):
        """Test date range estimation with >= and <= operators."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day >= '2021-09-13' AND day <= '2021-09-26'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "inventory_log",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=20),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_excessive_date_range(self):
        """Test detection of excessive date range."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-12-31'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=100),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE
        assert results[0].estimated_range is not None
        assert results[0].estimated_range > timedelta(days=100)

    def test_single_day_equals(self):
        """Test date range estimation for single day with equals."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "inventory_log",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=5),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_no_max_days_skips_range_check(self):
        """Test that without max_days, range check is skipped."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-12-31'
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_date_function_in_comparison(self):
        """Test date range estimation with date functions."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= date('2021-09-13') AND day <= date('2021-09-26')
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=20),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations


    def test_union_adjacent_multi_month_ranges_excessive(self):
        """Test UNION where each SELECT has an adjacent multi-month BETWEEN range that exceeds max days."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-02-01'
        UNION ALL
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-02-02' AND '2021-03-05'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=30),
            )
        ])
        results = checker.find_violations(sql)

        # Each part covers more than 30 days individually (first: 32 days), so should flag excessive range
        assert len(results) == 2
        for violation in results:
            assert violation.violation == PartitionViolationType.EXCESSIVE_DATE_RANGE
            assert violation.estimated_range is not None
            assert violation.estimated_range > timedelta(days=30)

        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-01-18'
        UNION ALL
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-19' AND '2021-02-05'
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=30),
            )
        ])
        results = checker.find_violations(sql)

        # Each part doesn't span more than allowed, should not flag excessive range
        assert len(results) == 0

    def test_checker_with_multiple_date_partition_columns_different_ranges(self):
        """Test multiple tables with different max_date_range_days."""
        sql = """
              SELECT a.day, b.event_time
              FROM gridhive.fact.sales_history a
                       JOIN events.log_table b ON a.day = b.event_time
              WHERE a.day BETWEEN '2021-09-01' AND '2021-09-15'
                AND b.event_time BETWEEN '2021-09-01' AND '2021-09-15' \
              """
        partition_cols = [
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=10),
            ),
            DateTablePartition(
                "log_table",
                [DatePartitionColumn("event_time", "YYYY-mm-dd")],
                max_date_range=timedelta(days=30),
            )
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 1  # Only sales_history has violation
        # sales_history should have excessive range (15 days > 10 max)
        sales_result = results[0]
        assert sales_result.table_name == "sales_history"
        assert sales_result.violation == PartitionViolationType.EXCESSIVE_DATE_RANGE


    @pytest.mark.skip("https://github.com/Spikhalskiy/sql-ranger/issues/12")
    def test_processes_simple_or_correctly(self):
        """
        Test query with a simple OR. This test is making sure that expressions are processed as a tree,
        not as a trivial list of comparisons.
        """
        sql = """
              SELECT * FROM gridhive.fact.sales_history
              WHERE '2021-09-13' <= day OR '2021-09-26' >= day \
              """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=30),
            )
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        # Should still detect as valid since we check both sides
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE

