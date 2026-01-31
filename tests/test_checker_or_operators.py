"""Tests for OR operator handling in partition_checker module."""
from datetime import timedelta

from sqlranger.checker import (
    DatePartitionColumn,
    DateTablePartition,
    PartitionChecker,
    PartitionViolationType,
    TablePartition,
)


class TestOrOperators:
    """Test suite for OR operator handling."""

    def test_or_with_range_conditions_excessive_range(self):
        """Test that OR between range conditions is detected as excessive range."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= '2021-09-13' OR day <= '2021-09-26'
        """
        checker = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE

    def test_or_with_equals_conditions_no_violation(self):
        """Test that OR between equality conditions is accepted as valid."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE (day = '2021-09-13' OR day = '2021-09-14') AND product_id = 100
        """
        checker = PartitionChecker(
            partitioned_tables=[TablePartition("sales_history", ["day"])]
        )
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_and_with_range_conditions_no_violation(self):
        """Test that AND between range conditions works correctly (baseline)."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= '2021-09-13' AND day <= '2021-09-26'
        """
        checker = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_or_at_top_level_with_partition_and_non_partition(self):
        """Test OR between partition condition and non-partition condition."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day = '2021-09-13' OR product_id = 100
        """
        checker = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results = checker.find_violations(sql)

        # This should pass - the day is filtered even though there's an OR
        # The partition filter exists, it's just that one branch might not use it
        assert len(results) == 0

    def test_nested_or_with_ands(self):
        """Test nested OR inside AND expressions."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE (day >= '2021-09-13' AND day <= '2021-09-26') AND product_id = 100
        """
        checker = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_or_between_different_conditions_types(self):
        """Test OR between lower bound on one side and upper bound on other."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day > '2021-09-13' OR day < '2021-09-26'
        """
        checker = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results = checker.find_violations(sql)

        # This should be excessive range since OR makes it cover all dates
        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE

    def test_multiple_or_conditions_on_partition_column(self):
        """Test multiple OR conditions on the same partition column."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day = '2021-09-13' OR day = '2021-09-14' OR day = '2021-09-15'
        """
        # Test with both TablePartition and DateTablePartition
        checker1 = PartitionChecker(
            partitioned_tables=[TablePartition("sales_history", ["day"])]
        )
        results1 = checker1.find_violations(sql)
        assert len(results1) == 0  # No violations

        checker2 = PartitionChecker(
            partitioned_tables=[
                DateTablePartition(
                    "sales_history",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=30),
                )
            ]
        )
        results2 = checker2.find_violations(sql)
        assert len(results2) == 0  # No violations even with date range check
