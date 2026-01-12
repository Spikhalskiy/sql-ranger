"""Test suite for input validation, sanitation, pre-processing."""
from datetime import timedelta

from sqlranger import DatePartitionColumn, DateTablePartition, TablePartition


class TestInputValidation:
    """Test suite for input validation."""

    def test_table_partition_empty_partitions(self):
        """Test TablePartition rejects empty partitions list."""
        try:
            TablePartition("test_table", [])
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "cannot be empty" in str(e)

    def test_table_partition_negative_enforced_level(self):
        """Test TablePartition rejects negative enforced_level."""
        try:
            TablePartition("test_table", ["col1"], enforced_level=-1)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "non-negative" in str(e)

    def test_table_partition_enforced_level_exceeds_partitions(self):
        """Test TablePartition rejects enforced_level > number of partitions."""
        try:
            TablePartition("test_table", ["col1", "col2"], enforced_level=5)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "cannot exceed" in str(e)

    def test_date_table_partition_empty_partitions(self):
        """Test DateTablePartition rejects empty partitions list."""
        try:
            DateTablePartition("test_table", [])
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "cannot be empty" in str(e)

    def test_date_table_partition_invalid_partition_type(self):
        """Test DateTablePartition rejects non-DatePartitionColumn items."""
        try:
            DateTablePartition("test_table", ["not_a_date_partition_column"])
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "DatePartitionColumn instances" in str(e)

    def test_date_table_partition_negative_max_date_range(self):
        """Test DateTablePartition rejects negative max_date_range."""
        try:
            DateTablePartition(
                "test_table",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=-1)
            )
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "must be positive" in str(e)

    def test_date_table_partition_zero_max_date_range(self):
        """Test DateTablePartition rejects zero max_date_range."""
        try:
            DateTablePartition(
                "test_table",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=0)
            )
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "must be positive" in str(e)
