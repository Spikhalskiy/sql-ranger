"""
Partition checker for validating SQL queries against partitioning requirements.

This module provides functionality to verify that SQL queries accessing partitioned tables
include proper partition filters (day column) to ensure efficient query execution.
"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import sqlglot
from sqlglot import exp


class PartitionCheckStatus(Enum):
    """Status of partition check validation."""

    VALID = "VALID"
    MISSING_DAY_FILTER = "MISSING_DAY_FILTER"
    DAY_FILTER_WITH_FUNCTION = "DAY_FILTER_WITH_FUNCTION"
    NO_FINITE_RANGE = "NO_FINITE_RANGE"
    EXCESSIVE_DATE_RANGE = "EXCESSIVE_DATE_RANGE"


@dataclass
class PartitionCheckResult:
    """Result of partition validation check."""

    status: PartitionCheckStatus
    message: str
    table_name: str | None = None
    estimated_days: int | None = None


class PartitionChecker:
    """Validates SQL queries for proper partition usage on specified tables."""

    def __init__(
            self,
            partitioned_tables: list[str],
            max_days: int | None = None,
    ):
        """
        Initialize the PartitionChecker.

        Args:
            partitioned_tables: List of table names (case-insensitive) that require partitioning.
            max_days: Maximum allowed date range in days. If None, date range is not checked.
        """
        self.partitioned_tables = (
            {table.lower() for table in partitioned_tables}
        )
        self.max_days = max_days

    def check_query(self, sql: str) -> list[PartitionCheckResult]:
        """
        Check a SQL query for proper partition usage.

        Args:
            sql: The SQL query to validate.

        Returns:
            List of PartitionCheckResult objects, one for each partitioned table found.
            Empty list if no partitioned tables are used or if query is invalid.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="trino")
        except Exception:
            # If parsing fails, return empty list
            return []

        results = []
        tables = self._extract_tables(parsed)

        for table_name in tables:
            if table_name.lower() in self.partitioned_tables:
                result = self._check_table_partition(parsed, table_name)
                results.append(result)

        return results

    def _extract_tables(self, parsed: exp.Expression) -> set[str]:
        """
        Extract table names from parsed SQL.

        Args:
            parsed: Parsed SQL expression.

        Returns:
            Set of table names (unqualified, just the table name part).
        """
        tables = set()
        for table in parsed.find_all(exp.Table):
            if table.name:
                tables.add(table.name)
        return tables

    def _check_table_partition(self, parsed: exp.Expression, table_name: str) -> PartitionCheckResult:
        """
        Check partition requirements for a specific table.

        Args:
            parsed: Parsed SQL expression.
            table_name: Name of the table to check.

        Returns:
            PartitionCheckResult with validation status.
        """
        # Find all WHERE clauses in the query
        where_clauses = list(parsed.find_all(exp.Where))

        if not where_clauses:
            return PartitionCheckResult(
                status=PartitionCheckStatus.MISSING_DAY_FILTER,
                message=f"Table '{table_name}' is used without a WHERE clause containing a 'day' filter",
                table_name=table_name,
            )

        # Check if any WHERE clause has a day filter
        day_conditions = []
        for where in where_clauses:
            conditions = self._extract_day_conditions(where, table_name)
            day_conditions.extend(conditions)

        if not day_conditions:
            return PartitionCheckResult(
                status=PartitionCheckStatus.MISSING_DAY_FILTER,
                message=f"Table '{table_name}' is used without a 'day' column filter in WHERE clause",
                table_name=table_name,
            )

        # Check if day column is used without functions
        for condition in day_conditions:
            if self._has_function_on_day_column(condition):
                return PartitionCheckResult(
                    status=PartitionCheckStatus.DAY_FILTER_WITH_FUNCTION,
                    message=(
                        f"Table '{table_name}' uses 'day' column with a function, which disables partitioning. "
                        "Use raw 'day' column in comparisons."
                    ),
                    table_name=table_name,
                )

        # Check for finite range
        if not self._has_finite_range(day_conditions):
            return PartitionCheckResult(
                status=PartitionCheckStatus.NO_FINITE_RANGE,
                message=(
                    f"Table '{table_name}' does not have a finite date range. "
                    "Use BETWEEN or combination of >= and <= operators."
                ),
                table_name=table_name,
            )

        # Optional: Check date range if max_days is set
        if self.max_days is not None:
            estimated_days = self._estimate_date_range(day_conditions)
            if estimated_days is not None and estimated_days > self.max_days:
                return PartitionCheckResult(
                    status=PartitionCheckStatus.EXCESSIVE_DATE_RANGE,
                    message=(
                        f"Table '{table_name}' has an excessive date range of approximately "
                        f"{estimated_days} days (max: {self.max_days})"
                    ),
                    table_name=table_name,
                    estimated_days=estimated_days,
                )

        return PartitionCheckResult(
            status=PartitionCheckStatus.VALID,
            message=f"Table '{table_name}' has proper partition filtering",
            table_name=table_name,
        )

    def _extract_day_conditions(self, where: exp.Where, table_name: str) -> list[exp.Expression]:
        """
        Extract conditions involving the 'day' column from a WHERE clause.

        Args:
            where: WHERE clause expression.
            table_name: Name of the table to extract the day conditions for.

        Returns:
            List of expressions that reference the 'day' column.
        """
        day_conditions = []

        # Find all comparison and BETWEEN expressions
        for node in where.walk():
            is_comparison = isinstance(node, (exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Between))
            if is_comparison and self._references_day_column_of_table(node, table_name):
                day_conditions.append(node)

        return day_conditions

    def _references_day_column(self, condition: exp.Expression) -> bool:
        """
        Check if a condition references the 'day' column.

        Args:
            condition: Expression to check.

        Returns:
            True if the expression references a 'day' column.
        """
        return any(column.name and column.name.lower() == "day" for column in condition.find_all(exp.Column))

    def _get_expr_collumn_table(self, column: exp.Column, condition: exp.Expression) -> exp.Table | None:
        """
        Get the table from the condition's parent select for a given column.

        Args:
            column: Column
            condition: Expression the column belongs to
        """
        tables = {table.alias.lower() : table for table in list(condition.parent_select.find_all(exp.Table))}
        return tables.get(column.table.lower(), None)

    def _references_day_column_of_table(self, condition: exp.Expression, table_name: str) -> bool:
        """
        Check if a condition references the 'day' column.

        Args:
            condition: Expression to check.
            table_name: Name of the table to check the day column of.

        Returns:
            True if the expression references a 'day' column.
        """
        return any(column.name and column.name.lower() == "day"
                   and self._get_expr_collumn_table(column, condition).name.lower() == table_name.lower()
                   for column in condition.find_all(exp.Column))

    def _has_function_on_day_column(self, condition: exp.Expression) -> bool:
        """
        Check if day column is wrapped in a function (which breaks partitioning).

        Args:
            condition: Expression to check.

        Returns:
            True if day column is used inside a function.
        """
        # Walk through the expression tree
        for node in condition.walk():
            # Check if this is a function call
            if isinstance(node, (exp.Func, exp.Anonymous)):
                # Check if any of the function's arguments contain the day column
                for column in node.find_all(exp.Column):
                    if column.name and column.name.lower() == "day":
                        return True
        return False

    def _has_finite_range(self, conditions: list[exp.Expression]) -> bool:
        """
        Check if conditions define a finite date range.

        A finite range requires either:
        - A BETWEEN clause
        - Both >= (or >) and <= (or <) operators
        - An = operator

        Args:
            conditions: List of day column conditions.

        Returns:
            True if conditions define a finite range.
        """
        has_between = False
        has_lower_bound = False
        has_upper_bound = False
        has_equals = False

        for condition in conditions:
            if isinstance(condition, exp.Between):
                has_between = True
            elif isinstance(condition, exp.EQ):
                has_equals = True
            elif isinstance(condition, (exp.GTE, exp.GT)):
                has_lower_bound = True
            elif isinstance(condition, (exp.LTE, exp.LT)):
                has_upper_bound = True

        return has_between or has_equals or (has_lower_bound and has_upper_bound)

    def _estimate_date_range(self, conditions: list[exp.Expression]) -> int | None:
        """
        Estimate the date range in days from conditions.

        This is a best-effort estimation that only works with:
        - String date literals in YYYY-MM-DD format
        - Simple date function calls (date, from_iso8601_date)

        Args:
            conditions: List of day column conditions.

        Returns:
            Estimated number of days, or None if cannot be estimated.
        """
        start_date: datetime | None = None
        end_date: datetime | None = None

        for condition in conditions:
            if isinstance(condition, exp.Between):
                # Extract dates from BETWEEN clause
                low = self._extract_date_value(condition.args.get("low"))
                high = self._extract_date_value(condition.args.get("high"))
                if low and high:
                    start_date = low
                    end_date = high
                    break
            elif isinstance(condition, exp.EQ):
                # Single date
                date_val = self._extract_date_from_comparison(condition)
                if date_val:
                    return 1
            elif isinstance(condition, (exp.GTE, exp.GT)):
                date_val = self._extract_date_from_comparison(condition)
                if date_val and (start_date is None or date_val < start_date):
                    start_date = date_val
            elif isinstance(condition, (exp.LTE, exp.LT)):
                date_val = self._extract_date_from_comparison(condition)
                if date_val and (end_date is None or date_val > end_date):
                    end_date = date_val

        if start_date and end_date:
            return (end_date - start_date).days + 1

        return None

    def _extract_date_from_comparison(self, condition: exp.Expression) -> datetime | None:
        """
        Extract a date value from a comparison expression.

        Args:
            condition: Comparison expression (EQ, LT, LTE, GT, GTE).

        Returns:
            Datetime object if date can be extracted, None otherwise.
        """
        # Get the right side of the comparison (assuming day column is on the left)
        # Check which side has the day column
        if self._references_day_column(condition.this):
            return self._extract_date_value(condition.expression)
        if self._references_day_column(condition.expression):
            return self._extract_date_value(condition.this)

        return None

    def _extract_date_value(self, expr: exp.Expression | None) -> datetime | None:
        """
        Extract a datetime value from an expression.

        Args:
            expr: Expression that might contain a date value.

        Returns:
            Datetime object if date can be extracted, None otherwise.
        """
        if expr is None:
            return None

        # Handle string literals
        if isinstance(expr, exp.Literal):
            return self._parse_date_string(expr.this)

        # Handle date functions like date('2021-09-13') or from_iso8601_date('2021-09-13')
        if isinstance(expr, exp.Func):
            func_name = expr.sql_name().lower()
            if func_name in ("date", "from_iso8601_date"):
                # Get first argument
                args = expr.args.get("expressions") or []
                if args and isinstance(args[0], exp.Literal):
                    return self._parse_date_string(args[0].this)

        return None

    def _parse_date_string(self, date_str: str) -> datetime | None:
        """
        Parse a date string in YYYY-MM-DD format.

        Args:
            date_str: Date string to parse.

        Returns:
            Datetime object if parsing succeeds, None otherwise.
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            return None


def check_partition_usage(
        sql: str,
        partitioned_tables: list[str] | None = None,
        max_days: int | None = None,
) -> list[PartitionCheckResult]:
    """
    Convenience function to check SQL query for proper partition usage.

    Args:
        sql: The SQL query to validate.
        partitioned_tables: List of table names that require partitioning.
        max_days: Maximum allowed date range in days. If None, date range is not checked.

    Returns:
        List of PartitionCheckResult objects, one for each partitioned table found.
        Empty list if no partitioned tables are used or if query is invalid.

    Example:
        >>> results = check_partition_usage(
        ...     "SELECT * FROM warehouse.fact.sales_history WHERE day = '2021-09-13'"
        ... )
        >>> results[0].status
        <PartitionCheckStatus.VALID: 'valid'>
    """
    checker = PartitionChecker(partitioned_tables=partitioned_tables, max_days=max_days)
    return checker.check_query(sql)
