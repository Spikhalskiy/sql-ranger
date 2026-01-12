"""Test suite for the check_partition_usage convenience function."""
from datetime import timedelta

from sqlranger import (
    DatePartitionColumn,
    DateTablePartition,
    PartitionViolationType,
    TablePartition,
    check_partition_usage,
)


class TestConvenienceFunction:
    """Test suite for check_partition_usage convenience function."""

    def test_convenience_function_default_tables(self):
        """Test convenience function with partitioned tables."""
        sql = """
              SELECT * FROM gridhive.fact.sales_history
              WHERE day = '2021-09-13' \
              """
        results = check_partition_usage(sql, partitioned_tables=[TablePartition("sales_history", ["day"])])

        assert len(results) == 0  # No violations

    def test_convenience_function_custom_tables(self):
        """Test convenience function with custom partitioned tables."""
        sql = "SELECT * FROM order_events WHERE day = '2021-09-13'"
        results = check_partition_usage(sql, partitioned_tables=[TablePartition("order_events", ["day"])])

        assert len(results) == 0  # No violations

    def test_convenience_function_with_max_days(self):
        """Test convenience function with max_date_range_days parameter."""
        sql = """
              SELECT * FROM gridhive.fact.inventory_log
              WHERE day BETWEEN '2021-01-01' AND '2021-12-31' \
              """
        results = check_partition_usage(
            sql,
            partitioned_tables=[
                DateTablePartition(
                    "inventory_log",
                    [DatePartitionColumn("day", "YYYY-mm-dd")],
                    max_date_range=timedelta(days=100),
                )
            ]
        )

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE
