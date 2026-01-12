"""Tests for misc functions of partition_checker module."""

from sqlranger.checker import (
    TablePartition,
)


class TestTablePartition:
    """Test suite for TablePartition class."""

    def test_get_nonqualified_table_name_simple(self):
        """Test extracting non-qualified name from simple table name."""
        pc = TablePartition("sales_history", ["day"])
        assert pc.get_nonqualified_table_name() == "sales_history"

    def test_get_nonqualified_table_name_with_schema(self):
        """Test extracting non-qualified name from schema.table."""
        pc = TablePartition("fact.sales_history", ["day"])
        assert pc.get_nonqualified_table_name() == "sales_history"

    def test_get_nonqualified_table_name_fully_qualified(self):
        """Test extracting non-qualified name from catalog.schema.table."""
        pc = TablePartition("gridhive.fact.sales_history", ["day"])
        assert pc.get_nonqualified_table_name() == "sales_history"

