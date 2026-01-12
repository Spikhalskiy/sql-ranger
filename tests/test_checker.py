"""Tests for partition_checker module."""
from datetime import timedelta

from sqlranger.checker import (
    DatePartitionColumn,
    DateTablePartition,
    PartitionChecker,
    PartitionViolationType,
    TablePartition,
)


class TestPartitionChecker:
    """Test suite for PartitionChecker class."""

    def test_valid_query_with_day_equals(self):
        """Test valid query with day = 'date' filter."""
        sql = """
        SELECT day, SUM(quantity) AS total_quantity
        FROM gridhive.fact.sales_history
        WHERE product_id = 12345 AND store_id = 100 AND day = '2025-12-02'
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_valid_query_with_day_equals_placeholder(self):
        """Test valid query with day = 'date' filter."""
        sql = """
              SELECT day, SUM(quantity) AS total_quantity
              FROM gridhive.fact.sales_history
              WHERE product_id = 12345 AND store_id = 100 AND day = :date_param \
              """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_valid_query_with_day_between(self):
        """Test valid query with day BETWEEN filter."""
        sql = """
        SELECT day, hour, SUM(quantity) AS total_quantity
        FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
            AND product_id = 789 AND store_id = 50
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_valid_query_with_day_range(self):
        """Test valid query with day >= and day <= filters."""
        sql = """
        SELECT day, SUM(quantity)
        FROM gridhive.fact.inventory_log
        WHERE day >= '2021-09-13' AND day <= '2021-09-26' AND gridhive_id = 5
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("inventory_log", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_valid_query_with_day_less_and_greater(self):
        """Test valid query with day > and day < filters."""
        sql = """
        SELECT SUM(quantity)
        FROM gridhive.fact.sales_history
        WHERE day > '2021-09-13' AND day < '2021-09-26' AND product_id = 456
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_missing_day_filter_no_where(self):
        """Test query without WHERE clause."""
        sql = "SELECT * FROM gridhive.fact.sales_history"
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "without a WHERE clause" in results[0].message
        assert results[0].table_name == "sales_history"

    def test_missing_day_filter_with_other_filters(self):
        """Test query with WHERE but no day filter."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE product_id = 12345 AND gridhive_id = 10
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("inventory_log", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "without a 'day' column filter" in results[0].message

    def test_day_filter_with_function(self):
        """Test query with function applied to day column."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE DATE_FORMAT(day, '%Y-%m') = '2021-09' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.PARTITION_COLUMN_WITH_FUNCTION
        assert "with a function" in results[0].message

    def test_day_filter_with_extract_function(self):
        """Test query with EXTRACT function on day column."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE EXTRACT(YEAR FROM day) = 2021 AND gridhive_id = 5
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("inventory_log", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.PARTITION_COLUMN_WITH_FUNCTION

    def test_no_finite_range_only_greater(self):
        """Test query with only >= filter (no upper bound)."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= '2021-09-13' AND product_id = 500
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.NO_FINITE_RANGE
        assert "finite range" in results[0].message

    def test_no_finite_range_only_less(self):
        """Test query with only <= filter (no lower bound)."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day <= '2021-09-26' AND gridhive_id = 8
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("inventory_log", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.NO_FINITE_RANGE

    def test_multiple_partitioned_tables(self):
        """Test query with multiple partitioned tables."""
        sql = """
        SELECT a.day, b.quantity
        FROM gridhive.fact.sales_history a
        JOIN gridhive.fact.inventory_log b ON a.day = b.day
        WHERE a.day = '2021-09-13' AND b.day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("sales_history", ["day"]),
            TablePartition("inventory_log", ["day"])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations for any table

    def test_non_partitioned_table_ignored(self):
        """Test that non-partitioned tables are ignored."""
        sql = """
        SELECT * FROM gridhive.dim.products
        WHERE product_id = 12345
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0

    def test_mixed_partitioned_and_non_partitioned(self):
        """Test query with both partitioned and non-partitioned tables."""
        sql = """
        SELECT a.day, b.product_name
        FROM gridhive.fact.sales_history a
        JOIN gridhive.dim.products b ON a.product_id = b.id
        WHERE a.day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

        sql = """
              SELECT a.day, b.product_name
              FROM gridhive.fact.sales_history a
                       JOIN gridhive.dim.products b ON a.product_id = b.id
              WHERE b.day = '2021-09-13' \
              """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert results[0].table_name == "sales_history"

    def test_case_insensitive_table_names(self):
        """Test that table name matching is case-insensitive."""
        sql = """
        SELECT * FROM gridhive.fact.SALES_HISTORY
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert results[0].table_name == "sales_history"

    def test_invalid_sql_incomplete_query(self):
        """Test that incomplete SQL returns QUERY_INVALID_SYNTAX violation."""
        sql = "SELECT * FROM"
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.QUERY_INVALID_SYNTAX
        assert "Failed to parse SQL query" in results[0].message
        assert results[0].table_name is None
        assert results[0].estimated_range is None

    def test_cte(self):
        """Test query with CTE containing day filter."""
        sql = """
        WITH daily_totals AS (
            SELECT sum(quantity) as total_qty
            FROM gridhive.fact.sales_history
            WHERE day = '2025-12-01'
        )
        SELECT * FROM daily_totals
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

        sql = """
        WITH daily_totals AS (
            SELECT day, sum(quantity) as total_qty
            FROM gridhive.fact.sales_history
            WHERE day = '2025-12-01'
        ),
        daily_totals_rev AS (
            SELECT sum(revenue) as total_qty
            FROM gridhive.fact.sales_history
        )
        SELECT * FROM daily_totals a join daily_totals_rev b on a.daily_totals = b.daily_totals_rev \
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert results[0].table_name == "sales_history"

    def test_checker_with_partition_column_objects(self):
        """Test PartitionChecker with PartitionColumn objects."""
        sql = """
        SELECT day, SUM(quantity) AS total_quantity
        FROM gridhive.fact.sales_history
        WHERE product_id = 12345 AND store_id = 100 AND day = '2025-12-02'
        """
        partition_cols = [
            TablePartition("sales_history", ["day"])
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_checker_with_custom_column_name(self):
        """Test PartitionChecker with custom partition column name."""
        sql = """
        SELECT event_date, COUNT(*) AS total_events
        FROM events.log_table
        WHERE event_date = '2025-12-02'
        """
        partition_cols = [
            TablePartition("log_table", ["event_date"])
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_checker_with_custom_column_name_missing_filter(self):
        """Test that checker correctly identifies missing custom column filter."""
        sql = """
        SELECT event_date, COUNT(*) AS total_events
        FROM events.log_table
        WHERE user_id = 123
        """
        partition_cols = [
            TablePartition("log_table", ["event_date"])
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
        assert "event_date" in results[0].message

    def test_checker_with_date_partition_column(self):
        """Test PartitionChecker with DatePartitionColumn objects."""
        sql = """
        SELECT day, SUM(quantity)
        FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
        """
        partition_cols = [
            DateTablePartition(
                "sales_history",
                [DatePartitionColumn("day", "YYYY-mm-dd")],
                max_date_range=timedelta(days=20),
            )
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_checker_with_fully_qualified_table_names(self):
        """Test that PartitionColumn with fully qualified names works correctly."""
        sql = """
        SELECT day, SUM(quantity)
        FROM gridhive.fact.sales_history
        WHERE day = '2025-12-02'
        """
        partition_cols = [
            TablePartition("gridhive.fact.sales_history", ["day"])
        ]
        checker = PartitionChecker(partitioned_tables=partition_cols)
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations


    def test_subquery_with_day_filter(self):
        """Test query with subquery containing day filter."""
        sql = """
        SELECT total
        FROM (
               SELECT SUM(quantity) as total
               FROM gridhive.fact.sales_history
               WHERE day = '2021-09-13'
           ) subq
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_union_both_with_day_filters(self):
        """Test UNION query where both parts have day filters."""
        sql = """
        SELECT day FROM gridhive.fact.sales_history WHERE day = '2021-09-13'
        UNION ALL
        SELECT day FROM gridhive.fact.inventory_log WHERE day = '2021-09-14' \
        """
        checker = PartitionChecker(partitioned_tables=[
            TablePartition("sales_history", ["day"]),
            TablePartition("inventory_log", ["day"])
        ])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations for any table

    def test_day_column_in_select_but_not_where(self):
        """Test query that selects day column but doesn't filter by it."""
        sql = """
        SELECT day, quantity FROM gridhive.fact.sales_history
        WHERE quantity > 100 \
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER

    def test_day_in_having_clause_not_where(self):
        """Test query with day in HAVING but not WHERE."""
        sql = """
        SELECT day, SUM(quantity) as total
        FROM gridhive.fact.sales_history
        GROUP BY day
        HAVING day = '2021-09-13' \
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        # HAVING is not the same as WHERE for partitioning purposes
        assert len(results) == 1
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER

    def test_day_comparison_reversed(self):
        """Test query with day comparison in reversed order."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE '2021-09-13' <= day AND '2021-09-26' >= day \
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("inventory_log", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_complex_where_with_ands_and_ors(self):
        """Test complex WHERE clause with AND/OR logic."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE (day = '2021-09-13' OR day = '2021-09-14') AND product_id = 100 \
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 0  # No violations

    def test_day_checked_on_correct_tables(self):
        """Test query with day comparison in reversed order."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history a join gridhive.fact.inventory b on a.day = b.day
        WHERE '2021-09-13' <= b.day AND '2021-09-26' >= b.day
        """
        checker = PartitionChecker(partitioned_tables=[TablePartition("sales_history", ["day"])])
        results = checker.find_violations(sql)

        assert len(results) == 1
        # Should still detect as valid since we check both sides
        assert results[0].violation == PartitionViolationType.MISSING_PARTITION_FILTER
