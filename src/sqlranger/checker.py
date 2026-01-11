"""
Partition checker for validating SQL queries against partitioning requirements.

This module provides functionality to verify that SQL queries accessing partitioned tables
include proper partition filters (day column) to ensure efficient query execution.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

import sqlglot
from sqlglot import exp


@dataclass
class DatePartitionColumn:
    """Configuration for a single date partition column with its format pattern."""

    column_name: str
    format_pattern: str


class TablePartition:
    """Configuration for a partitioned table with hierarchical partition columns."""

    def __init__(
        self,
        table_name: str,
        partitions: list[str],
        enforced_level: int | None = None,
    ):
        """
        Initialize the TablePartition.

        Args:
            table_name: Full table name (e.g., 'gridhive.fact.sales_history').
            partitions: Ordered list of partition column names from root to smallest sub-partition.
            enforced_level: Number of partition levels to enforce. If None, all partitions are enforced.
        """
        self.table_name = table_name
        self.partitions = partitions
        self.enforced_level = enforced_level if enforced_level is not None else len(partitions)

    def get_nonqualified_table_name(self) -> str:
        """
        Get the non-qualified table name (after the last dot).

        Returns:
            Short table name without schema/catalog prefix.

        Example:
            >>> tp = TablePartition('gridhive.fact.sales_history', ['day'])
            >>> tp.get_nonqualified_table_name()
            'sales_history'
        """
        return self.table_name.split(".")[-1]

    def get_enforced_partitions(self) -> list[str]:
        """
        Get the list of enforced partition columns.

        Returns:
            List of partition column names that must be filtered.
        """
        return self.partitions[:self.enforced_level]


class DateTablePartition(TablePartition):
    """Configuration for a date-partitioned table with hierarchical date partition columns."""

    def __init__(
        self,
        table_name: str,
        partitions: list[DatePartitionColumn],
        enforced_level: int | None = None,
        max_date_range: timedelta | None = None,
    ):
        """
        Initialize the DateTablePartition.

        Args:
            table_name: Full table name (e.g., 'gridhive.fact.sales_history').
            partitions: Ordered list of DatePartitionColumn objects from root to smallest sub-partition.
            enforced_level: Number of partition levels to enforce. If None, all partitions are enforced.
            max_date_range: Maximum allowed date range as timedelta. If None, range is not checked.
        """
        # Extract column names for parent class
        column_names = [p.column_name for p in partitions]
        super().__init__(table_name, column_names, enforced_level)
        self.date_partitions = partitions
        self.max_date_range = max_date_range


class PartitionViolationType(Enum):
    """Type of partition check violation."""

    MISSING_DAY_FILTER = "MISSING_DAY_FILTER"
    PARTITION_COLUMN_WITH_FUNCTION = "PARTITION_COLUMN_WITH_FUNCTION"
    NO_FINITE_RANGE = "NO_FINITE_RANGE"
    EXCESSIVE_DATE_RANGE = "EXCESSIVE_DATE_RANGE"
    QUERY_INVALID_SYNTAX = "QUERY_INVALID_SYNTAX"


@dataclass
class PartitionViolation:
    """Result of partition validation check representing a violation."""

    violation: PartitionViolationType
    message: str
    table_name: str | None = None
    estimated_days: int | None = None


class PartitionChecker:
    """Validates SQL queries for proper partition usage on specified tables."""

    def __init__(self, partitioned_tables: list[TablePartition]):
        """
        Initialize the PartitionChecker.

        Args:
            partitioned_tables: List of TablePartition objects defining partition configuration.

        Raises:
            ValueError: If multiple tables with the same non-qualified name are configured.
        """
        # Build configuration mapping keyed by non-qualified table name, while
        # validating that there are no duplicate short names which would cause
        # configurations to be silently overwritten.
        self._partition_configs: dict[str, TablePartition] = {}
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

    def find_violations(self, sql: str) -> list[PartitionViolation]:
        """
        Check a SQL query for proper partition usage.

        Args:
            sql: The SQL query to validate.

        Returns:
            List of PartitionViolation objects for tables with violations.
            Empty list if all partitioned tables are properly filtered.
            Returns a list with QUERY_INVALID_SYNTAX violation if query parsing fails.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="trino")
        except Exception as e:
            # If parsing fails, return QUERY_INVALID_SYNTAX violation
            return [
                PartitionViolation(
                    violation=PartitionViolationType.QUERY_INVALID_SYNTAX,
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
            self, select_sql: exp.Expression, partition_config: TablePartition
    ) -> PartitionViolation | None:
        """
        Check partition requirements for a specific table referenced in the FROM clause of the SQL query.

        Args:
            select_sql: Parsed SQL expression.
            partition_config: TablePartition configuration for the table.

        Returns:
            PartitionViolation with violation details if validation fails, None if valid.
        """
        table_name = partition_config.get_nonqualified_table_name()
        enforced_partitions = partition_config.get_enforced_partitions()

        # Find all WHERE clauses in the query
        where_clauses = list(select_sql.find_all(exp.Where))

        if not where_clauses:
            missing_columns = ", ".join(f"'{col}'" for col in enforced_partitions)
            return PartitionViolation(
                violation=PartitionViolationType.MISSING_DAY_FILTER,
                message=f"Table '{table_name}' is used without a WHERE clause containing filters for {missing_columns}",
                table_name=table_name,
            )

        # Check each enforced partition column
        for column_name in enforced_partitions:
            # Check if any WHERE clause has this partition column filter
            partition_conditions = []
            for where in where_clauses:
                conditions = self._extract_partition_conditions(where, table_name, column_name)
                partition_conditions.extend(conditions)

            if not partition_conditions:
                return PartitionViolation(
                    violation=PartitionViolationType.MISSING_DAY_FILTER,
                    message=f"Table '{table_name}' is used without a '{column_name}' column filter in WHERE clause",
                    table_name=table_name,
                )

            # Check if partition column is used without functions
            for condition in partition_conditions:
                if self._has_function_on_column(condition, column_name):
                    return PartitionViolation(
                        violation=PartitionViolationType.PARTITION_COLUMN_WITH_FUNCTION,
                        message=(
                            f"Table '{table_name}' uses '{column_name}' column with a function, "
                            "which disables partitioning. "
                            f"Use raw '{column_name}' column in comparisons."
                        ),
                        table_name=table_name,
                    )

            # Check for finite range
            if not self._has_finite_range(partition_conditions):
                return PartitionViolation(
                    violation=PartitionViolationType.NO_FINITE_RANGE,
                    message=(
                        f"Table '{table_name}' does not have a finite range on '{column_name}'. "
                        "Use BETWEEN or combination of >= and <= operators."
                    ),
                    table_name=table_name,
                )

        # Check date range if configured (only for DateTablePartition)
        if (isinstance(partition_config, DateTablePartition)
            and partition_config.max_date_range is not None
            and enforced_partitions):
            # Collect conditions for all enforced partition columns
            all_conditions = []
            for column_name in enforced_partitions:
                for where in where_clauses:
                    conditions = self._extract_partition_conditions(where, table_name, column_name)
                    all_conditions.extend(conditions)

            # Check if we have equality conditions for multiple partition levels
            # If so, the range is restricted by the finest granularity level
            equality_count = sum(1 for cond in all_conditions if isinstance(cond, exp.EQ))

            # If we have equality on multiple levels, the range is at the finest granularity
            # For example: day='2021-09-01' AND hour=00 means 1 hour, not 1 day
            if equality_count >= len(enforced_partitions):
                # All enforced levels have equality - use finest granularity
                # Assume each level divides by 24 (day->hour), 60 (hour->minute), etc.
                # For day+hour with both equality, range is 1 hour = 3600 seconds
                estimated_seconds = 86400.0 / (24 ** (len(enforced_partitions) - 1))
            else:
                # Use the first column to estimate range
                first_column = enforced_partitions[0]
                first_column_conditions = []
                for where in where_clauses:
                    conditions = self._extract_partition_conditions(where, table_name, first_column)
                    first_column_conditions.extend(conditions)
                estimated_seconds = self._estimate_date_range(first_column_conditions)

            max_seconds = partition_config.max_date_range.total_seconds()
            if estimated_seconds is not None and estimated_seconds > max_seconds:
                estimated_days = estimated_seconds / 86400.0
                max_days = max_seconds / 86400.0
                return PartitionViolation(
                    violation=PartitionViolationType.EXCESSIVE_DATE_RANGE,
                    message=(
                        f"Table '{table_name}' has an excessive date range of approximately "
                        f"{estimated_days} days (max: {max_days})"
                    ),
                    table_name=table_name,
                    estimated_days=estimated_days,
                )

        return None

    def _check_table_partition_hierarchically(
        self, select_sql: exp.Expression, table_name: str, partition_config: TablePartition
    ) -> list[PartitionViolation]:
        """
        Check partition requirements for a specific table in the specific SQL query.

        Args:
            select_sql: Parsed SQL expression.
            table_name: Name of the table to check.
            partition_config: TablePartition configuration for the table.

        Returns:
            List of PartitionViolation with violation details if validation fails, empty if valid.
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

    def _estimate_date_range(self, conditions: list[exp.Expression]) -> float | None:
        """
        Estimate the date range in seconds from conditions.

        This is a best-effort estimation that only works with:
        - String date literals in YYYY-MM-DD format
        - Simple date function calls (date, from_iso8601_date)

        Args:
            conditions: List of day column conditions.

        Returns:
            Estimated number of seconds, or None if cannot be estimated.
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
                # Single date - assume 1 day in seconds
                date_val = self._extract_date_from_comparison(condition)
                if date_val:
                    return 86400.0  # 1 day in seconds
            elif isinstance(condition, (exp.GTE, exp.GT)):
                date_val = self._extract_date_from_comparison(condition)
                if date_val and (start_date is None or date_val < start_date):
                    start_date = date_val
            elif isinstance(condition, (exp.LTE, exp.LT)):
                date_val = self._extract_date_from_comparison(condition)
                if date_val and (end_date is None or date_val > end_date):
                    end_date = date_val

        if start_date and end_date:
            return (end_date - start_date).total_seconds() + 86400.0  # +1 day for inclusive range

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
        partitioned_tables: list[TablePartition],
) -> list[PartitionViolation]:
    """
    Convenience function to check SQL query for proper partition usage.

    Args:
        sql: The SQL query to validate.
        partitioned_tables: List of TablePartition objects defining partition configuration.

    Returns:
        List of PartitionViolation objects for tables with violations.
        Empty list if all partitioned tables are properly filtered.
        Returns a list with QUERY_INVALID_SYNTAX violation if query parsing fails.

    Example:
        >>> from sqlranger import TablePartition
        >>> results = check_partition_usage(
        ...     "SELECT * FROM gridhive.fact.sales_history WHERE day = '2021-09-13'",
        ...     [TablePartition("sales_history", ["day"])]
        ... )
        >>> len(results)  # Empty list means no violations
        0
    """
    checker = PartitionChecker(partitioned_tables=partitioned_tables)
    return checker.find_violations(sql)
