"""Tests for nested hierarchical partitions."""
from datetime import timedelta

from sqlranger.checker import (
    DatePartitionColumn,
    DateTablePartition,
    PartitionChecker,
    PartitionViolationType,
    TablePartition,
)


class TestHierarchicalPartitions:
    """Test suite for hierarchical partition validation."""

    def test_two_level_partition_both_enforced(self):
        """Test table with two partition levels, both enforced."""
        sql = """
        SELECT * FROM warehouse.inventory
        WHERE city = 'Seattle' AND warehouse = 'W1' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("inventory", ["city", "warehouse"])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_two_level_partition_missing_first(self):
        """Test table with two partition levels, missing first level filter."""
        sql = """
        SELECT * FROM warehouse.inventory
        WHERE warehouse = 'W1' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("inventory", ["city", "warehouse"])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "city" in results[0].message

    def test_two_level_partition_missing_second(self):
        """Test table with two partition levels, missing second level filter."""
        sql = """
        SELECT * FROM warehouse.inventory
        WHERE city = 'Seattle' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("inventory", ["city", "warehouse"])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "warehouse" in results[0].message

    def test_three_level_partition_enforced_level_2(self):
        """Test table with three partition levels, only first two enforced."""
        sql = """
        SELECT * FROM warehouse.inventory
        WHERE city = 'Seattle' AND warehouse = 'W1' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("inventory", ["city", "warehouse", "building_number"], enforced_level=2)
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations, building_number not required

    def test_three_level_partition_enforced_level_2_missing_second(self):
        """Test table with three partition levels, enforced level 2, missing second level."""
        sql = """
        SELECT * FROM warehouse.inventory
        WHERE city = 'Seattle' AND building_number = 'B5'
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("inventory", ["city", "warehouse", "building_number"], enforced_level=2)
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "warehouse" in results[0].message

    def test_hierarchical_date_partitions(self):
        """Test table with hierarchical date partitions (year, month, day)."""
        sql = """
        SELECT * FROM sales.history
        WHERE year = 2021 AND month = 9 AND day = 13
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("year", "YYYY"),
                DatePartitionColumn("month", "mm"),
                DatePartitionColumn("day", "DD")
            ])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_hierarchical_date_partitions_enforced_level_2(self):
        """Test hierarchical date partitions with enforced_level=2."""
        sql = """
        SELECT * FROM sales.history
        WHERE year = 2021 AND month = 9
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("year", "YYYY"),
                DatePartitionColumn("month", "mm"),
                DatePartitionColumn("day", "DD")
            ], enforced_level=2)
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations, day not required

    def test_hierarchical_date_partitions_with_max_range(self):
        """Test hierarchical date partitions range enforcement."""
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("day", "YYYY-mm-dd"),
                DatePartitionColumn("hour", "HH"),
            ], max_date_range=timedelta(hours=2))
        ])

        sql = """
              SELECT * FROM sales.history
              WHERE day = '2021-09-01' AND hour >= 00 AND hour <= 14
              """
        results = checker.find_violations(sql)
        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE


    def test_hierarchical_date_partitions_with_max_range_valid(self):
        """Test hierarchical date partitions range enforcement with valid range."""
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("day", "YYYY-mm-dd"),
                DatePartitionColumn("hour", "HH"),
            ], max_date_range=timedelta(hours=2))
        ])

        sql = """
              SELECT * FROM sales.history
              WHERE day = '2021-09-01' AND hour = 00
              """
        results = checker.find_violations(sql)
        assert len(results) == 0

        sql = """
              SELECT * FROM sales.history
              WHERE day = '2021-09-01' AND hour >= 00 AND hour <= 01
              """
        results = checker.find_violations(sql)
        assert len(results) == 0

        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("month", "YYYY-mm"),
                DatePartitionColumn("day", "DD"),
                DatePartitionColumn("hour", "HH"),
            ], max_date_range=timedelta(hours=2))
        ])
        sql = """
              SELECT * FROM sales.history
              WHERE month = '2021-09' AND day = '01' AND hour >= 00 AND hour <= 01
              """
        results = checker.find_violations(sql)
        assert len(results) == 0

        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("year", "YYYY"),
                DatePartitionColumn("month", "mm"),
                DatePartitionColumn("day", "dd"),
                DatePartitionColumn("hour", "HH"),
            ], max_date_range=timedelta(hours=2))
        ])
        sql = """
              SELECT * FROM sales.history
              WHERE year = '2025' AND month = '09' AND day = '01' AND hour >= 00 AND hour <= 01
              """
        results = checker.find_violations(sql)
        assert len(results) == 0

    def test_hierarchical_date_partitions_with_max_range_enforce_higher_level_partitions(self):
        """Test hierarchical date partitions range enforcement works correctly for higher-level partitions."""
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("day", "YYYY-mm-dd"),
                DatePartitionColumn("hour", "HH"),
            ],
            enforced_level=1, # we enforce only day partition level
            max_date_range=timedelta(hours=2)) # but the range is formulated in hours
        ])

        sql = """
              SELECT * FROM sales.history
              WHERE day = '2021-09-01'
              """
        results = checker.find_violations(sql)
        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.EXCESSIVE_DATE_RANGE

    def test_hierarchical_date_partitions_missing_middle_level(self):
        """Test hierarchical date partitions missing middle level."""
        sql = """
        SELECT * FROM sales.history
        WHERE year = 2021 AND day = 13
        """
        checker = PartitionChecker(partitioned_tables=[
            DateTablePartition("history", [
                DatePartitionColumn("year", "YYYY"),
                DatePartitionColumn("month", "mm"),
                DatePartitionColumn("day", "dd")
            ])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "month" in results[0].message


