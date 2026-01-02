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


class PartitionColumn:
    """Configuration for a partitioned table column."""

    def __init__(self, table_name: str, column_name: str):
        """
        Initialize the PartitionColumn.

        Args:
            table_name: Full table name (e.g., 'gridhive.fact.sales_history').
            column_name: Name of the partition column (e.g., 'day').
        """
        self.table_name = table_name
        self.column_name = column_name

    def get_nonqualified_table_name(self) -> str:
        """
        Get the non-qualified table name (after the last dot).

        Returns:
            Short table name without schema/catalog prefix.

        Example:
            >>> pc = PartitionColumn('gridhive.fact.sales_history', 'day')
            >>> pc.get_nonqualified_table_name()
            'sales_history'
        """
        return self.table_name.split(".")[-1]


class DatePartitionColumn(PartitionColumn):
    """Configuration for a date-partitioned table column."""

    def __init__(
        self,
        table_name: str,
        column_name: str,
        date_pattern: str,
        max_date_range_days: int | None = None,
    ):
        """
        Initialize the DatePartitionColumn.

        Args:
            table_name: Full table name (e.g., 'gridhive.fact.sales_history').
            column_name: Name of the partition column (e.g., 'day').
            date_pattern: Date format pattern (e.g., 'YYYY-MM-dd').
            max_date_range_days: Maximum allowed date range in days. If None, range is not checked.
        """
        super().__init__(table_name, column_name)
        self.date_pattern = date_pattern
        self.max_date_range_days = max_date_range_days


class PartitionCheckViolation(Enum):
    """Type of partition check violation."""

    MISSING_DAY_FILTER = "MISSING_DAY_FILTER"
    DAY_FILTER_WITH_FUNCTION = "DAY_FILTER_WITH_FUNCTION"
    NO_FINITE_RANGE = "NO_FINITE_RANGE"
    EXCESSIVE_DATE_RANGE = "EXCESSIVE_DATE_RANGE"
    QUERY_INVALID_SYNTAX = "QUERY_INVALID_SYNTAX"


@dataclass
class PartitionCheckResult:
    """Result of partition validation check representing a violation."""

    violation: PartitionCheckViolation
    message: str
    table_name: str | None = None
    estimated_days: int | None = None


class PartitionChecker:
    """Validates SQL queries for proper partition usage on specified tables."""

    def __init__(self, partitioned_tables: list[PartitionColumn]):
        """
        Initialize the PartitionChecker.

        Args:
            partitioned_tables: List of PartitionColumn objects defining partition configuration.

        Raises:
            ValueError: If multiple tables with the same non-qualified name are configured.
        """
        # Build configuration mapping keyed by non-qualified table name, while
        # validating that there are no duplicate short names which would cause
        # configurations to be silently overwritten.
        self._partition_configs: dict[str, PartitionColumn] = {}
        for pc in partitioned_tables:
            key = pc.get_nonqualified_table_name().lower()
            if key in self._partition_configs:
                existing = self._partition_configs[key]
                raise ValueError(
                    f"Duplicate partition configuration for non-qualified table "
                    f"name '{key}': '{existing.table_name}' and '{pc.table_name}'. "
                    "Use distinct non-qualified names or adjust the configuration."
                )
            self._partition_configs[key] = pc

    def check_query(self, sql: str) -> list[PartitionCheckResult]:
        """
        Check a SQL query for proper partition usage.

        Args:
            sql: The SQL query to validate.

        Returns:
            List of PartitionCheckResult objects for tables with violations.
            Empty list if all partitioned tables are properly filtered.
            Returns a list with QUERY_INVALID_SYNTAX violation if query parsing fails.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="trino")
        except Exception as e:
            # If parsing fails, return QUERY_INVALID_SYNTAX violation
            return [
                PartitionCheckResult(
                    violation=PartitionCheckViolation.QUERY_INVALID_SYNTAX,
                    message=f"Failed to parse SQL query: {e!s}",
                    table_name=None,
                    estimated_days=None,
                )
            ]

        violations = []
        tables = self._extract_tables(parsed)

        for table_name in tables:
            if table_name.lower() in self._partition_configs:
                partition_config = self._partition_configs[table_name.lower()]
                result = self._check_table_partition_hierarchically(parsed, table_name, partition_config)
                violations += result

        return violations

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

    def _check_table_partition_in_specific_sql(
            self, select_sql: exp.Expression, partition_config: PartitionColumn
    ) -> PartitionCheckResult | None:
        """
        Check partition requirements for a specific table referenced in the FROM clause of the SQL query.

        Args:
            select_sql: Parsed SQL expression.
            partition_config: PartitionColumn configuration for the table.

        Returns:
            PartitionCheckResult with violation details if validation fails, None if valid.
        """
        column_name = partition_config.column_name
        table_name = partition_config.get_nonqualified_table_name()

        # Find all WHERE clauses in the query
        where_clauses = list(select_sql.find_all(exp.Where))

        if not where_clauses:
            return PartitionCheckResult(
                violation=PartitionCheckViolation.MISSING_DAY_FILTER,
                message=f"Table '{table_name}' is used without a WHERE clause containing a '{column_name}' filter",
                table_name=table_name,
            )

        # Check if any WHERE clause has a partition column filter
        partition_conditions = []
        for where in where_clauses:
            conditions = self._extract_partition_conditions(where, table_name, column_name)
            partition_conditions.extend(conditions)

        if not partition_conditions:
            return PartitionCheckResult(
                violation=PartitionCheckViolation.MISSING_DAY_FILTER,
                message=f"Table '{table_name}' is used without a '{column_name}' column filter in WHERE clause",
                table_name=table_name,
            )

        # Check if partition column is used without functions
        for condition in partition_conditions:
            if self._has_function_on_column(condition, column_name):
                return PartitionCheckResult(
                    violation=PartitionCheckViolation.DAY_FILTER_WITH_FUNCTION,
                    message=(
                        f"Table '{table_name}' uses '{column_name}' column with a function, "
                        "which disables partitioning. "
                        f"Use raw '{column_name}' column in comparisons."
                    ),
                    table_name=table_name,
                )

        # Check for finite range
        if not self._has_finite_range(partition_conditions):
            return PartitionCheckResult(
                violation=PartitionCheckViolation.NO_FINITE_RANGE,
                message=(
                    f"Table '{table_name}' does not have a finite date range. "
                    "Use BETWEEN or combination of >= and <= operators."
                ),
                table_name=table_name,
            )

        # Check date range if configured
        if isinstance(partition_config, DatePartitionColumn) and partition_config.max_date_range_days is not None:
            max_days = partition_config.max_date_range_days
            estimated_days = self._estimate_date_range(partition_conditions)
            if estimated_days is not None and estimated_days > max_days:
                return PartitionCheckResult(
                    violation=PartitionCheckViolation.EXCESSIVE_DATE_RANGE,
                    message=(
                        f"Table '{table_name}' has an excessive date range of approximately "
                        f"{estimated_days} days (max: {max_days})"
                    ),
                    table_name=table_name,
                    estimated_days=estimated_days,
                )

        return None

    def _check_table_partition_hierarchically(
        self, select_sql: exp.Expression, table_name: str, partition_config: PartitionColumn
    ) -> list[PartitionCheckResult]:
        """
        Check partition requirements for a specific table in the specific SQL query.

        Args:
            select_sql: Parsed SQL expression.
            table_name: Name of the table to check.
            partition_config: PartitionColumn configuration for the table.

        Returns:
            List of PartitionCheckResult with violation details if validation fails, empty if valid.
        """
        results = []
        from_clauses = filter(
            lambda from_clause: isinstance(from_clause.this, exp.Table)
            and from_clause.this.name
            and from_clause.this.name.lower() == table_name.lower(),
            select_sql.find_all(exp.From),
        )
        for from_clause in from_clauses:
            check_result = self._check_table_partition_in_specific_sql(from_clause.parent_select, partition_config)
            if check_result is not None:
                results.append(check_result)


        # No violations found - return empty list
        return results

    def _extract_partition_conditions(
        self, where: exp.Where, table_name: str, column_name: str
    ) -> list[exp.Expression]:
        """
        Extract conditions involving the partition column from a WHERE clause.

        Args:
            where: WHERE clause expression.
            table_name: Name of the table to extract the partition conditions for.
            column_name: Name of the partition column.

        Returns:
            List of expressions that reference the partition column.
        """
        partition_conditions = []

        # Find all comparison and BETWEEN expressions
        for node in where.walk():
            is_comparison = isinstance(node, (exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Between))
            if is_comparison and self._references_column_of_table(node, table_name, column_name):
                partition_conditions.append(node)

        return partition_conditions

    def _get_expr_column_table(self, column: exp.Column, condition: exp.Expression) -> exp.Table | None:
        """
        Get the table from the condition's parent select for a given column.

        Args:
            column: Column
            condition: Expression the column belongs to

        Returns:
            Table object if found, None otherwise.
        """
        if not getattr(condition, "parent_select", None):
            return None

        if not column.table:
            return None

        tables = {
            (table.alias or table.name).lower(): table
            for table in condition.parent_select.find_all(exp.Table)
        }
        return tables.get(column.table.lower(), None)

    def _references_column_of_table(self, condition: exp.Expression, table_name: str, column_name: str) -> bool:
        """
        Check if a condition references the specified column of a specific table.

        Args:
            condition: Expression to check.
            table_name: Name of the table to check the column of.
            column_name: Name of the column to check for.

        Returns:
            True if the expression references the specified column of the table.
        """
        for column in condition.find_all(exp.Column):
            if not (column.name and column.name.lower() == column_name.lower()):
                continue

            # If column doesn't specify a table, assume it's from the table we're checking
            if not column.table:
                return True

            table = self._get_expr_column_table(column, condition)
            if table and table.name.lower() == table_name.lower():
                return True
        return False

    def _has_function_on_column(self, condition: exp.Expression, column_name: str) -> bool:
        """
        Check if the specified column is wrapped in a function (which breaks partitioning).

        Args:
            condition: Expression to check.
            column_name: Name of the column to check for.

        Returns:
            True if the column is used inside a function.
        """
        # Walk through the expression tree
        for node in condition.walk():
            # Check if this is a function call
            if isinstance(node, (exp.Func, exp.Anonymous)):
                # Check if any of the function's arguments contain the column
                for column in node.find_all(exp.Column):
                    if column.name and column.name.lower() == column_name.lower():
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
        # Get the right side of the comparison
        # Check which side has a column reference
        has_column_left = any(isinstance(n, exp.Column) for n in condition.this.walk())
        has_column_right = any(isinstance(n, exp.Column) for n in condition.expression.walk())

        if has_column_left and not has_column_right:
            return self._extract_date_value(condition.expression)
        if has_column_right and not has_column_left:
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
        partitioned_tables: list[PartitionColumn],
) -> list[PartitionCheckResult]:
    """
    Convenience function to check SQL query for proper partition usage.

    Args:
        sql: The SQL query to validate.
        partitioned_tables: List of PartitionColumn objects defining partition configuration.

    Returns:
        List of PartitionCheckResult objects for tables with violations.
        Empty list if all partitioned tables are properly filtered.
        Returns a list with QUERY_INVALID_SYNTAX violation if query parsing fails.

    Example:
        >>> from sqlranger import PartitionColumn
        >>> results = check_partition_usage(
        ...     "SELECT * FROM gridhive.fact.sales_history WHERE day = '2021-09-13'",
        ...     [PartitionColumn("sales_history", "day")]
        ... )
        >>> len(results)  # Empty list means no violations
        0
    """
    checker = PartitionChecker(partitioned_tables=partitioned_tables)
    return checker.check_query(sql)
