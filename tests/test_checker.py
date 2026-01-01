"""Tests for partition_checker module."""
from sqlranger.checker import (
    PartitionChecker,
    PartitionCheckStatus,
    check_partition_usage,
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
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        assert results[0].table_name == "sales_history"

    def test_valid_query_with_day_between(self):
        """Test valid query with day BETWEEN filter."""
        sql = """
        SELECT day, hour, SUM(quantity) AS total_quantity
        FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
            AND product_id = 789 AND store_id = 50
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        assert results[0].table_name == "sales_history"

    def test_valid_query_with_day_range(self):
        """Test valid query with day >= and day <= filters."""
        sql = """
        SELECT day, SUM(quantity)
        FROM gridhive.fact.inventory_log
        WHERE day >= '2021-09-13' AND day <= '2021-09-26' AND gridhive_id = 5
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        assert results[0].table_name == "inventory_log"

    def test_valid_query_with_day_less_and_greater(self):
        """Test valid query with day > and day < filters."""
        sql = """
        SELECT SUM(quantity)
        FROM gridhive.fact.sales_history
        WHERE day > '2021-09-13' AND day < '2021-09-26' AND product_id = 456
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_missing_day_filter_no_where(self):
        """Test query without WHERE clause."""
        sql = "SELECT * FROM gridhive.fact.sales_history"
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER
        assert "without a WHERE clause" in results[0].message
        assert results[0].table_name == "sales_history"

    def test_missing_day_filter_with_other_filters(self):
        """Test query with WHERE but no day filter."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE product_id = 12345 AND gridhive_id = 10
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER
        assert "without a 'day' column filter" in results[0].message

    def test_day_filter_with_function(self):
        """Test query with function applied to day column."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE DATE_FORMAT(day, '%Y-%m') = '2021-09' AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.DAY_FILTER_WITH_FUNCTION
        assert "with a function" in results[0].message

    def test_day_filter_with_extract_function(self):
        """Test query with EXTRACT function on day column."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE EXTRACT(YEAR FROM day) = 2021 AND gridhive_id = 5
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.DAY_FILTER_WITH_FUNCTION

    def test_no_finite_range_only_greater(self):
        """Test query with only >= filter (no upper bound)."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= '2021-09-13' AND product_id = 500
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.NO_FINITE_RANGE
        assert "finite date range" in results[0].message

    def test_no_finite_range_only_less(self):
        """Test query with only <= filter (no lower bound)."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day <= '2021-09-26' AND gridhive_id = 8
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.NO_FINITE_RANGE

    def test_multiple_partitioned_tables(self):
        """Test query with multiple partitioned tables."""
        sql = """
        SELECT a.day, b.quantity
        FROM gridhive.fact.sales_history a
        JOIN gridhive.fact.inventory_log b ON a.day = b.day
        WHERE a.day = '2021-09-13' AND b.day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history", "inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 2
        assert all(r.status == PartitionCheckStatus.VALID for r in results)
        table_names = {r.table_name for r in results}
        assert "sales_history" in table_names
        assert "inventory_log" in table_names

    def test_non_partitioned_table_ignored(self):
        """Test that non-partitioned tables are ignored."""
        sql = """
        SELECT * FROM gridhive.dim.products
        WHERE product_id = 12345
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 0

    def test_mixed_partitioned_and_non_partitioned(self):
        """Test query with both partitioned and non-partitioned tables."""
        sql = """
        SELECT a.day, b.product_name
        FROM gridhive.fact.sales_history a
        JOIN gridhive.dim.products b ON a.product_id = b.id
        WHERE a.day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        assert results[0].table_name == "sales_history"

    def test_custom_partitioned_tables(self):
        """Test with custom list of partitioned tables."""
        sql = "SELECT * FROM order_events WHERE other_col = 1"
        checker = PartitionChecker(partitioned_tables=["order_events"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER
        assert results[0].table_name == "order_events"

    def test_case_insensitive_table_names(self):
        """Test that table name matching is case-insensitive."""
        sql = """
        SELECT * FROM gridhive.fact.SALES_HISTORY
        WHERE day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_invalid_sql_returns_empty(self):
        """Test that invalid SQL returns empty results."""
        sql = "THIS IS NOT VALID SQL !!!"
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 0

    def test_cte_with_day_filter(self):
        """Test query with CTE containing day filter."""
        sql = """
        WITH daily_totals AS (
            SELECT sum(quantity) as total_qty
            FROM gridhive.fact.sales_history
            WHERE day = '2025-12-01' and hour='09'
        )
        SELECT * FROM daily_totals
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID


class TestDateRangeEstimation:
    """Test suite for date range estimation functionality."""

    def test_estimate_range_with_between(self):
        """Test date range estimation with BETWEEN clause."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"], max_days=20)
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        # 2021-09-13 to 2021-09-26 is 14 days (inclusive)

    def test_estimate_range_with_gte_and_lte(self):
        """Test date range estimation with >= and <= operators."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day >= '2021-09-13' AND day <= '2021-09-26'
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"], max_days=20)
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_excessive_date_range(self):
        """Test detection of excessive date range."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-12-31'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"], max_days=100)
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.EXCESSIVE_DATE_RANGE
        assert results[0].estimated_days is not None
        assert results[0].estimated_days > 100

    def test_single_day_equals(self):
        """Test date range estimation for single day with equals."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"], max_days=5)
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_no_max_days_skips_range_check(self):
        """Test that without max_days, range check is skipped."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day BETWEEN '2021-01-01' AND '2021-12-31'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])  # no max_days
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_date_function_in_comparison(self):
        """Test date range estimation with date functions."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day >= date('2021-09-13') AND day <= date('2021-09-26')
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"], max_days=20)
        results = checker.check_query(sql)

        assert len(results) == 1
        # Should still validate properly even with date functions on the value side
        assert results[0].status == PartitionCheckStatus.VALID


class TestConvenienceFunction:
    """Test suite for check_partition_usage convenience function."""

    def test_convenience_function_default_tables(self):
        """Test convenience function with default partitioned tables."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE day = '2021-09-13'
        """
        results = check_partition_usage(sql, partitioned_tables=["sales_history"])

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_convenience_function_custom_tables(self):
        """Test convenience function with custom partitioned tables."""
        sql = "SELECT * FROM order_events WHERE day = '2021-09-13'"
        results = check_partition_usage(sql, partitioned_tables=["order_events"])

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID
        assert results[0].table_name == "order_events"

    def test_convenience_function_with_max_days(self):
        """Test convenience function with max_days parameter."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE day BETWEEN '2021-01-01' AND '2021-12-31'
        """
        results = check_partition_usage(sql, partitioned_tables=["inventory_log"], max_days=100)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.EXCESSIVE_DATE_RANGE


class TestEdgeCases:
    """Test suite for edge cases and complex queries."""

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
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.VALID

    def test_union_both_with_day_filters(self):
        """Test UNION query where both parts have day filters."""
        sql = """
        SELECT day FROM gridhive.fact.sales_history WHERE day = '2021-09-13'
        UNION ALL
        SELECT day FROM gridhive.fact.inventory_log WHERE day = '2021-09-14'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history", "inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 2
        assert all(r.status == PartitionCheckStatus.VALID for r in results)

    def test_day_column_in_select_but_not_where(self):
        """Test query that selects day column but doesn't filter by it."""
        sql = """
        SELECT day, quantity FROM gridhive.fact.sales_history
        WHERE quantity > 100
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER

    def test_day_in_having_clause_not_where(self):
        """Test query with day in HAVING but not WHERE."""
        sql = """
        SELECT day, SUM(quantity) as total
        FROM gridhive.fact.sales_history
        GROUP BY day
        HAVING day = '2021-09-13'
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        # HAVING is not the same as WHERE for partitioning purposes
        assert len(results) == 1
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER

    def test_day_comparison_reversed(self):
        """Test query with day comparison in reversed order."""
        sql = """
        SELECT * FROM gridhive.fact.inventory_log
        WHERE '2021-09-13' <= day AND '2021-09-26' >= day
        """
        checker = PartitionChecker(partitioned_tables=["inventory_log"])
        results = checker.check_query(sql)

        assert len(results) == 1
        # Should still detect as valid since we check both sides
        assert results[0].status == PartitionCheckStatus.VALID

    def test_complex_where_with_ands_and_ors(self):
        """Test complex WHERE clause with AND/OR logic."""
        sql = """
        SELECT * FROM gridhive.fact.sales_history
        WHERE (day = '2021-09-13' OR day = '2021-09-14') AND product_id = 100
        """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        # Both days are specific dates, so it should be valid
        assert results[0].status == PartitionCheckStatus.VALID

    def test_day_checked_on_correct_tables(self):
        """Test query with day comparison in reversed order."""
        sql = """
              SELECT * FROM gridhive.fact.sales_history a join gridhive.fact.inventory b on a.day = b.day
              WHERE '2021-09-13' <= b.day AND '2021-09-26' >= b.day
              """
        checker = PartitionChecker(partitioned_tables=["sales_history"])
        results = checker.check_query(sql)

        assert len(results) == 1
        # Should still detect as valid since we check both sides
        assert results[0].status == PartitionCheckStatus.MISSING_DAY_FILTER
