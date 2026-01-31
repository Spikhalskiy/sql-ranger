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

# When OR operators separate partition conditions (e.g., day >= X OR day <= Y),
# the effective range is infinite. We use a very large value (100 years in seconds)
# to represent this for range estimation, which will trigger EXCESSIVE_DATE_RANGE.
INFINITE_RANGE_SECONDS = 100 * 365.25 * 24 * 3600


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

        Raises:
            ValueError: If partitions list is empty, enforced_level is negative,
                       or enforced_level exceeds the number of partitions.
        """
        if not partitions:
            raise ValueError("partitions list cannot be empty")
        if enforced_level is not None and enforced_level < 0:
            raise ValueError(f"enforced_level must be non-negative, got {enforced_level}")
        if enforced_level is not None and enforced_level > len(partitions):
            raise ValueError(
                f"enforced_level ({enforced_level}) cannot exceed number of partitions ({len(partitions)})"
            )

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

        Raises:
            ValueError: If partitions list is empty, contains non-DatePartitionColumn items,
                       or max_date_range is not positive.
        """
        if not partitions:
            raise ValueError("partitions list cannot be empty")
        if not all(isinstance(p, DatePartitionColumn) for p in partitions):
            raise ValueError("All items in partitions list must be DatePartitionColumn instances")
        if max_date_range is not None and max_date_range.total_seconds() <= 0:
            raise ValueError(f"max_date_range must be positive, got {max_date_range}")

        # Extract column names for parent class
        column_names = [p.column_name for p in partitions]
        super().__init__(table_name, column_names, enforced_level)
        self.date_partitions = partitions
        self.max_date_range = max_date_range


class PartitionViolationType(Enum):
    """Type of partition check violation."""

    MISSING_PARTITION_FILTER = "MISSING_PARTITION_FILTER"
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
    estimated_range: timedelta | None = None


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
                    estimated_range=None,
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
                violation=PartitionViolationType.MISSING_PARTITION_FILTER,
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
                    violation=PartitionViolationType.MISSING_PARTITION_FILTER,
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

            # Check for finite range (e.g., BETWEEN, =, or both >= and <=)
            # Note: OR operators between partition conditions are detected later during
            # range estimation for DateTablePartition, where they trigger EXCESSIVE_DATE_RANGE.
            # For basic TablePartition, OR conditions still allow the partition filter check to pass.
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
            # Walk the WHERE clause tree and calculate ranges for each subtree
            max_seconds = partition_config.max_date_range.total_seconds()

            for where in where_clauses:
                # Calculate the range for this WHERE clause by walking the tree
                violation = self._check_tree_range(
                    where.this,
                    table_name,
                    partition_config.date_partitions,
                    max_seconds
                )
                if violation:
                    return violation

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

    def _check_tree_range(
        self,
        node: exp.Expression,
        table_name: str,
        date_partitions: list,
        max_seconds: float,
    ) -> PartitionViolation | None:
        """
        Check date range by walking the expression tree recursively.

        For OR nodes, each branch must independently satisfy the range constraint.
        The total range is the union of all branches.

        Args:
            node: Expression node to check (typically the WHERE clause body).
            table_name: Name of the table being checked.
            date_partitions: List of DatePartitionColumn objects.
            max_seconds: Maximum allowed range in seconds.

        Returns:
            PartitionViolation if any subtree exceeds the max range, None otherwise.
        """
        estimated_seconds = self._calculate_tree_range(node, table_name, date_partitions)

        if estimated_seconds is not None and estimated_seconds > max_seconds:
            estimated_range = timedelta(seconds=estimated_seconds)
            estimated_str = self._format_time_range(estimated_seconds)
            max_str = self._format_time_range(max_seconds)
            return PartitionViolation(
                violation=PartitionViolationType.EXCESSIVE_DATE_RANGE,
                message=(
                    f"Table '{table_name}' has an excessive date range of approximately "
                    f"{estimated_str} (max: {max_str})"
                ),
                table_name=table_name,
                estimated_range=estimated_range,
            )

        return None

    def _calculate_tree_range(
        self,
        node: exp.Expression,
        table_name: str,
        date_partitions: list,
    ) -> float | None:
        """
        Calculate the date range for an expression tree node recursively.

        Args:
            node: Expression node to analyze.
            table_name: Name of the table.
            date_partitions: List of DatePartitionColumn objects.

        Returns:
            Estimated range in seconds, or None if cannot be estimated.
            Returns INFINITE_RANGE_SECONDS for truly infinite ranges (e.g., day >= X OR day <= Y).
        """
        # Handle OR nodes: calculate range for each branch
        if isinstance(node, exp.Or):
            left_range = self._calculate_tree_range(node.this, table_name, date_partitions)
            right_range = self._calculate_tree_range(node.expression, table_name, date_partitions)

            # Check if both branches have partition conditions
            left_has_conditions = self._has_partition_conditions(node.this, table_name, date_partitions)
            right_has_conditions = self._has_partition_conditions(node.expression, table_name, date_partitions)

            # If neither branch has partition conditions, return None (no range to check)
            if not left_has_conditions and not right_has_conditions:
                return None

            # If only one branch has partition conditions, use that branch's range
            # (the other branch doesn't add to the date range we're checking)
            if left_has_conditions and not right_has_conditions:
                return left_range
            if right_has_conditions and not left_has_conditions:
                return right_range

            # Both branches have partition conditions
            # If either branch is infinite/None, the whole OR is infinite
            if left_range is None or right_range is None:
                return INFINITE_RANGE_SECONDS

            # If both branches are finite, return the maximum (union of ranges)
            return max(left_range, right_range)

        # Handle AND nodes: calculate range for the combined conditions
        if isinstance(node, exp.And):
            # For AND, we need to collect all conditions and calculate their intersection
            conditions_by_column = {}
            for date_partition in date_partitions:
                column_name = date_partition.column_name
                conditions = self._extract_conditions_from_tree(node, table_name, column_name)
                if conditions:
                    conditions_by_column[column_name] = conditions

            if not conditions_by_column:
                # No partition conditions found
                return None

            # Calculate range from the collected conditions
            return self._estimate_range_from_conditions(conditions_by_column, date_partitions)

        # For leaf nodes (comparisons), extract conditions and calculate range
        conditions_by_column = {}
        for date_partition in date_partitions:
            column_name = date_partition.column_name
            conditions = self._extract_conditions_from_tree(node, table_name, column_name)
            if conditions:
                conditions_by_column[column_name] = conditions

        if not conditions_by_column:
            # No partition conditions found
            return None

        return self._estimate_range_from_conditions(conditions_by_column, date_partitions)

    def _has_partition_conditions(
        self,
        node: exp.Expression,
        table_name: str,
        date_partitions: list,
    ) -> bool:
        """
        Check if a node has any partition column conditions.

        Args:
            node: Expression node to check.
            table_name: Name of the table.
            date_partitions: List of DatePartitionColumn objects.

        Returns:
            True if the node references any partition column, False otherwise.
        """
        for date_partition in date_partitions:
            column_name = date_partition.column_name
            conditions = self._extract_conditions_from_tree(node, table_name, column_name)
            if conditions:
                return True
        return False

    def _estimate_range_from_conditions(
        self,
        conditions_by_column: dict[str, list[exp.Expression]],
        date_partitions: list,
    ) -> float | None:
        """
        Estimate date range from a set of conditions.

        This is similar to the original _estimate_hierarchical_time_range but without
        the OR detection logic (that's now handled in tree walking).

        Args:
            conditions_by_column: Dictionary mapping column names to their conditions.
            date_partitions: List of DatePartitionColumn objects.

        Returns:
            Estimated range in seconds, or None if cannot be estimated.
        """
        # Extract value ranges for each partition level
        values_by_column = {}
        for date_partition in date_partitions:
            column_name = date_partition.column_name
            conditions = conditions_by_column.get(column_name, [])
            if not conditions:
                continue

            min_val, max_val = self._extract_value_range(conditions)
            values_by_column[column_name] = (min_val, max_val)

        if not values_by_column:
            return None

        # Build datetime objects from the extracted values
        try:
            start_time = self._build_datetime_from_partitions(
                values_by_column, date_partitions, use_min=True
            )
            end_time = self._build_datetime_from_partitions(
                values_by_column, date_partitions, use_min=False
            )

            if start_time and end_time:
                delta = end_time - start_time
                return delta.total_seconds()

        except (ValueError, TypeError):
            pass

        return None

    def _extract_partition_conditions(
        self, where: exp.Where, table_name: str, column_name: str
    ) -> list[exp.Expression]:
        """
        Extract conditions involving the partition column from a WHERE clause.

        This method walks the expression tree and extracts conditions that are properly
        ANDed together. If partition column conditions are separated by OR operators,
        they are still returned but additional validation is needed.

        Args:
            where: WHERE clause expression.
            table_name: Name of the table to extract the partition conditions for.
            column_name: Name of the partition column.

        Returns:
            List of expressions that reference the partition column.
        """
        return self._extract_conditions_from_tree(where.this, table_name, column_name)

    def _extract_conditions_from_tree(
        self, node: exp.Expression, table_name: str, column_name: str
    ) -> list[exp.Expression]:
        """
        Recursively extract partition conditions from an expression tree.

        This extracts all conditions regardless of logical operators. The caller should
        check if they're properly connected with AND operators.

        Args:
            node: Expression node to process.
            table_name: Name of the table to extract the partition conditions for.
            column_name: Name of the partition column.

        Returns:
            List of partition column conditions found in the tree.
        """
        # Base case: if this is a comparison that references our column, return it
        is_comparison = isinstance(node, (exp.EQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.Between))
        if is_comparison and self._references_column_of_table(node, table_name, column_name):
            return [node]

        # Recursively collect from all children
        partition_conditions = []
        for child in node.iter_expressions():
            conditions = self._extract_conditions_from_tree(child, table_name, column_name)
            partition_conditions.extend(conditions)

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

    def _format_time_range(self, seconds: float) -> str:
        """
        Format a time range in seconds to a human-readable string.

        Selects the most appropriate unit (seconds, minutes, hours, days) based on magnitude.

        Args:
            seconds: Time range in seconds

        Returns:
            Formatted string like "2 hours" or "1.5 days"
        """
        if seconds < 60:
            # Less than 1 minute - show in seconds
            return f"{seconds:.1f} seconds" if seconds != 1 else "1 second"
        if seconds < 3600:
            # Less than 1 hour - show in minutes
            minutes = seconds / 60
            return f"{minutes:.1f} minutes" if minutes != 1 else "1 minute"
        if seconds < 86400:
            # Less than 1 day - show in hours
            hours = seconds / 3600
            return f"{hours:.1f} hours" if hours != 1 else "1 hour"
        # 1 day or more - show in days
        days = seconds / 86400
        return f"{days:.1f} days" if days != 1 else "1 day"

    def _extract_value_range(self, conditions: list[exp.Expression]) -> tuple[str | None, str | None]:
        """
        Extract the min and max values from a list of conditions.

        Returns:
            Tuple of (min_value, max_value) as strings, or (None, None)
        """
        min_val: str | None = None
        max_val: str | None = None

        def compare_values(val1: str, val2: str, operator: str) -> bool:
            """
            Compare two values, attempting numeric comparison first, falling back to string.

            Args:
                val1: First value to compare
                val2: Second value to compare
                operator: Comparison operator ("<" or ">")

            Returns:
                True if val1 operator val2, False otherwise
            """
            try:
                # Try to convert to float for numeric comparison
                num1 = float(val1)
                num2 = float(val2)
                if operator == "<":
                    return num1 < num2
                return num1 > num2
            except (ValueError, TypeError):
                # Fall back to string comparison
                if operator == "<":
                    return val1 < val2
                return val1 > val2

        for condition in conditions:
            if isinstance(condition, exp.Between):
                # Extract from BETWEEN clause
                low = self._extract_literal_value(condition.args.get("low"))
                high = self._extract_literal_value(condition.args.get("high"))
                if low is not None and high is not None:
                    min_val = low
                    max_val = high
                    break
            elif isinstance(condition, exp.EQ):
                # Single value - min and max are the same
                val = self._extract_literal_from_comparison(condition)
                if val is not None:
                    min_val = max_val = val
            elif isinstance(condition, (exp.GTE, exp.GT)):
                val = self._extract_literal_from_comparison(condition)
                if val is not None and (min_val is None or compare_values(val, min_val, "<")):
                    min_val = val
            elif isinstance(condition, (exp.LTE, exp.LT)):
                val = self._extract_literal_from_comparison(condition)
                if val is not None and (max_val is None or compare_values(val, max_val, ">")):
                    max_val = val

        return min_val, max_val

    def _extract_literal_value(self, expr: exp.Expression | None) -> str | None:
        """Extract a literal value as a string from an expression."""
        if expr is None:
            return None

        if isinstance(expr, exp.Literal):
            return str(expr.this)

        return None

    def _extract_literal_from_comparison(self, condition: exp.Expression) -> str | None:
        """Extract a literal value from a comparison expression."""
        # Check which side has a column reference
        has_column_left = any(isinstance(n, exp.Column) for n in condition.this.walk())
        has_column_right = any(isinstance(n, exp.Column) for n in condition.expression.walk())

        if has_column_left and not has_column_right:
            return self._extract_literal_value(condition.expression)
        if has_column_right and not has_column_left:
            return self._extract_literal_value(condition.this)

        return None

    def _build_datetime_from_partitions(
        self,
        values_by_column: dict[str, tuple[str | None, str | None]],
        date_partitions: list,
        use_min: bool,
    ) -> datetime | None:
        """
        Build a datetime object from partition values.

        Args:
            values_by_column: Dictionary of column names to (min, max) value tuples
            date_partitions: List of DatePartitionColumn objects
            use_min: If True, use min values; if False, use max values

        Returns:
            datetime object or None
        """
        # Default values for datetime components
        year = 1970
        month = 1
        day = 1
        hour = 0
        minute = 0
        second = 0

        for date_partition in date_partitions:
            column_name = date_partition.column_name
            if column_name not in values_by_column:
                continue

            min_val, max_val = values_by_column[column_name]
            value = min_val if use_min else max_val
            if value is None:
                continue

            # Parse the value based on the format pattern
            format_pattern = date_partition.format_pattern
            try:
                upper_pattern = format_pattern.upper()

                # Basic component presence flags
                has_year = "YYYY" in upper_pattern or upper_pattern == "Y"
                has_month = "mm" in format_pattern or format_pattern == "m"
                has_day = "DD" in upper_pattern or upper_pattern == "D"
                has_hour = "HH" in upper_pattern or upper_pattern == "H"
                has_minute_token = "MM" in format_pattern or format_pattern == "M"
                has_seconds_token = "SS" in upper_pattern or upper_pattern == "S"
                # More specific composite patterns
                has_full_date = (
                    has_year
                    and has_month
                    and has_day
                    and "-" in format_pattern
                )
                has_year_month = (
                    has_year
                    and has_month
                    and not has_full_date
                    and "-" in format_pattern
                )

                if has_full_date:
                    # Full date string like "2021-09-13"
                    parts = value.split("-")
                    if len(parts) == 3:
                        year = int(parts[0])
                        month = int(parts[1])
                        day = int(parts[2])
                elif has_year_month:
                    # Year-month string like "2021-09"
                    parts = value.split("-")
                    if len(parts) == 2:
                        year = int(parts[0])
                        month = int(parts[1])
                elif has_year:
                    year = int(value)
                elif has_month:
                    month = int(value)
                elif has_day:
                    day = int(value)
                elif has_hour:
                    hour = int(value)
                elif has_minute_token:
                    minute = int(value)
                elif has_seconds_token:
                    second = int(value)
            except (ValueError, IndexError):
                continue

        try:
            dt = datetime(year, month, day, hour, minute, second)
            # When building the maximum datetime, adjust to the end of the finest-granularity period.
            if not use_min:
                granularity_seconds = self._get_finest_granularity(date_partitions, values_by_column)
                # Move to the end of the period: start + granularity - 1 second.
                dt = dt + timedelta(seconds=granularity_seconds) - timedelta(seconds=1)
            return dt
        except ValueError:
            return None

    def _get_finest_granularity(
        self,
        date_partitions: list,
        values_by_column: dict[str, tuple[str | None, str | None]],
    ) -> float:
        """
        Get the finest granularity in seconds for the finest partition level that has conditions.
        This handles when the query has a == check to estimate the length of the targeted time range.

        Returns:
            Granularity in seconds (e.g., 3600 for hour, 86400 for day)
        """
        # Check from finest to coarsest
        for date_partition in reversed(date_partitions):
            if date_partition.column_name in values_by_column:
                format_pattern = date_partition.format_pattern
                format_upper = format_pattern.upper()

                if "SS" in format_upper or format_upper == "S":
                    return 1.0  # second
                if "MM" in format_pattern or format_pattern == "M":
                    return 60.0  # minute
                if "HH" in format_upper or format_upper == "H":
                    return 3600.0  # hour
                if "DD" in format_upper or format_upper == "D":
                    return 86400.0  # day
                if "mm" in format_pattern or format_pattern == "m":
                    return 86400.0 * 30  # month (approximate)
                if "YYYY" in format_upper or format_upper == "Y":
                    return 86400.0 * 365  # year (approximate)

        return 86400.0  # default to day

    def _estimate_date_range(self, conditions: list[exp.Expression]) -> float | None:
        """
        Estimate the date range in seconds from conditions.

        This is a best-effort estimation that only works with:
        - String date literals in YYYY-mm-dd format
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
        Parse a date string in YYYY-mm-dd format.

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
