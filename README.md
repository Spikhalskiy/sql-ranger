

# <img width="128" height="128" alt="ChatGPT_Image_Jan_1__2026__09_22_31_PM-removebg-preview" src="https://github.com/user-attachments/assets/a9798dac-1a1a-4a71-962b-e00be346d0aa" /> SQL Ranger

Enforcer of finite-range partitioning checks presence in SQL Queries.

## Purpose

When dealing with big historical tables that may contain terabytes of data, it's a common practice to partition them based on a day or an hour. 
It's also highly desirable to write queries in a way that absolutely avoids full scans.
This project helps check and ensure that queries have explicit finite boundaries on the values of partitioning columns.

Intended usages that drive the development of this project:
1. In combination with query plan complexity estimates (or actual cost post-execution), it can be used to alert about queries that are not effectively utilizing partitions.
2. The result can be used as feedback to the SQL-generating LLM agent to help it with generating partitioning-aware queries.

## Partition Usage Validation

The `PartitionChecker` validates SQL queries to ensure they properly use partitioning on large tables. This is critical for query performance, as some tables (like `mpcdsplogevent` and `logevent`) are partitioned by day and hour.

### Why Partition Validation?

Large partitioned tables should always be queried with partition filters to:
- Limit the amount of data scanned
- Improve query performance dramatically
- Prevent accidental full table scans

### Usage

#### Basic Validation

```python
from sqlranger import check_partition_usage, PartitionColumn, PartitionViolationType

# Basic validation with PartitionColumn
sql = """
    SELECT day, count(*) AS total
    FROM gridhive.fact.sales_history
    WHERE day = '2021-09-13'
"""
violations = check_partition_usage(
    sql,
    partitioned_tables=[PartitionColumn("sales_history", "day")]
)

if not violations:
    print("✓ All partitioned tables have proper filtering")
else:
    for violation in violations:
        print(f"✗ {violation.message}")
```

#### Advanced Configuration with PartitionColumn

For more control over partition configuration, use `PartitionColumn` and `DatePartitionColumn`:

```python
from sqlranger import PartitionChecker, PartitionColumn, DatePartitionColumn, PartitionViolationType

# Configure partition columns with custom column names
partition_cols = [
    PartitionColumn("gridhive.fact.sales_history", "day"),
    PartitionColumn("events.log_table", "event_date"),
]

checker = PartitionChecker(partitioned_tables=partition_cols)

sql = """
    SELECT event_date, COUNT(*) as total
    FROM events.log_table
    WHERE event_date BETWEEN '2021-09-13' AND '2021-09-26'
"""

violations = checker.find_violations(sql)
if not violations:
    print("✓ All partitioned tables have proper filtering")
else:
    for violation in violations:
        print(f"✗ {violation.violation.value}: {violation.message}")
```

#### Per-Table Date Range Limits

Use `DatePartitionColumn` to enforce different maximum date ranges for different tables:

```python
from sql_ranger import PartitionChecker, DatePartitionColumn

# Configure different max date ranges per table
partition_cols = [
    DatePartitionColumn(
        "gridhive.fact.sales_history",
        "day",
        "YYYY-mm-dd",
        max_date_range_days=30
    ),
    DatePartitionColumn(
        "events.log_table",
        "event_time",
        "YYYY-MM-dd",
        max_date_range_days=7
    ),
]

checker = PartitionChecker(partitioned_tables=partition_cols)

# This query will have a violation for log_table (15 days > 7 max)
# but not for sales_history (15 days <= 30 max)
sql = """
    SELECT a.day, b.event_time
    FROM gridhive.fact.sales_history a
    JOIN events.log_table b ON a.day = b.event_time
    WHERE a.day BETWEEN '2021-09-01' AND '2021-09-15'
      AND b.event_time BETWEEN '2021-09-01' AND '2021-09-15'
"""

violations = checker.find_violations(sql)
# violations will contain one entry for log_table only
```

### Configuration Classes

#### PartitionColumn

`PartitionColumn` is the base class for defining partition configuration. It specifies:
- The full table name (including schema/catalog if applicable)
- The partition column name

**Key Methods:**
- `get_nonqualified_table_name()`: Extracts the short table name (after the last dot)

**Example:**
```python
from sql_ranger import PartitionColumn

# Simple table name
pc1 = PartitionColumn("sales_history", "day")
print(pc1.get_nonqualified_table_name())  # Output: "sales_history"

# Fully qualified table name
pc2 = PartitionColumn("gridhive.fact.sales_history", "event_date")
print(pc2.get_nonqualified_table_name())  # Output: "sales_history"
```

#### DatePartitionColumn

`DatePartitionColumn` extends `PartitionColumn` with additional date-specific configuration:
- `date_pattern`: String describing the date format (e.g., "YYYY-mm-dd")
- `max_date_range_days`: Optional maximum allowed date range for this specific table

**Example:**
```python
from sql_ranger import DatePartitionColumn

# Configure a table with 30-day max range
dpc = DatePartitionColumn(
    "gridhive.fact.sales_history",
    "day",
    "YYYY-MM-dd",
    max_date_range_days=30
)
```

### Validation Rules

The partition checker enforces these rules:

1. **Partition Filter Required**: Any query using a partitioned table must include a partition column filter in the WHERE clause (by default `day`, but configurable via `PartitionColumn`)
2. **Raw Column Only**: The partition column must be used without functions (e.g., `day = '2021-09-13'` is OK, but `DATE_FORMAT(day, '%Y-%m')` breaks partitioning)
3. **Finite Range**: Queries must define a finite date range using:
    - `column = 'date'` (single date)
    - `column BETWEEN 'start' AND 'end'`
    - Both `column >= 'start'` AND `column <= 'end'`
4. **Optional Max Range**: When `max_date_range_days` is configured (via `DatePartitionColumn`), it enforces a maximum date range (best-effort estimation)

### Return Values

The validation functions return a list of `PartitionViolation` objects:
- **Empty list**: All partitioned tables are properly filtered (no violations)
- **Non-empty list**: Contains violation details for each table that fails validation

### Violation Types

| Violation | Description |
|--------|-------------|
| `MISSING_DAY_FILTER` | Query doesn't have a partition column filter in the `WHERE` clause |
| `DAY_FILTER_WITH_FUNCTION` | Partition column is wrapped in a function (breaks partitioning) |
| `NO_FINITE_RANGE` | Query doesn't define a finite date range |
| `EXCESSIVE_DATE_RANGE` | Date range exceeds the configured maximum |

### Example Validation Results

**Valid Query (no violations returned):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
```
Returns: `[]` (empty list - no violations)

**Invalid Query (function on day):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE DATE_FORMAT(day, '%Y-%m') = '2021-09'
```
Returns violation: `DAY_FILTER_WITH_FUNCTION` - Table 'sales_history' uses 'day' column with a function, which disables partitioning.

**Invalid Query (no upper bound):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE day >= '2021-09-13'
```
Returns violation: `NO_FINITE_RANGE` - Table 'sales_history' does not have a finite date range
