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

The `PartitionChecker` validates SQL queries to ensure they properly use partitioning on large tables. 

### Why Partition Validation?

Large partitioned tables should always be queried with partition filters to:
- Limit the amount of data scanned
- Improve query performance dramatically
- Prevent accidental full table scans

### Usage

#### Installation

```bash
pip install sqlranger==0.0.2
```

#### Basic Validation

To ensure explicit partition filtering on a `sales_history` table partitioned by the `day` column:

```python
from sqlranger import check_partition_usage, TablePartition, PartitionViolationType

# Basic validation with TablePartition
sql = """
    SELECT day, count(*) AS total
    FROM gridhive.fact.sales_history
    WHERE day = '2021-09-13'
"""
violations = check_partition_usage(
    sql,
    partitioned_tables=[TablePartition("sales_history", ["day"])]
)

if not violations:
    print("✓ All partitioned tables have proper filtering")
else:
    for violation in violations:
        print(f"✗ {violation.message}")
```

#### Hierarchical Partitions

Use `TablePartition` with multiple columns to enforce hierarchical partitioning:

```python
from sqlranger import TablePartition, PartitionChecker

# Configure hierarchical partitions: City > Warehouse > BuildingNumber
partition_config = TablePartition(
    "warehouse.inventory",
    partitions=["City", "Warehouse", "BuildingNumber"],
    enforced_level=2  # Only enforce City and Warehouse, BuildingNumber is optional
)

checker = PartitionChecker(partitioned_tables=[partition_config])

# This query is valid - both City and Warehouse are filtered
sql = """
    SELECT * FROM warehouse.inventory
    WHERE City = 'Seattle' AND Warehouse = 'W1' AND product_id = 100
"""
violations = checker.find_violations(sql)
# violations will be empty

# This query is invalid - missing Warehouse filter
sql = """
    SELECT * FROM warehouse.inventory
    WHERE City = 'Seattle' AND product_id = 100
"""
violations = checker.find_violations(sql)
# violations will contain an error about missing 'Warehouse' filter
```

#### Date Range Limits

Use `DateTablePartition` to enforce different maximum date ranges for different tables:

```python
from datetime import timedelta
from sqlranger import PartitionChecker, DateTablePartition, DatePartitionColumn

# Configure different max date ranges per table
partition_cols = [
    DateTablePartition(
        "gridhive.fact.sales_history",
        partitions=[DatePartitionColumn("day", "YYYY-mm-dd")],
        max_date_range=timedelta(days=30)
    ),
    DateTablePartition(
        "events.log_table",
        partitions=[DatePartitionColumn("event_time", "YYYY-mm-dd")],
        max_date_range=timedelta(days=7)
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

#### Hierarchical Date Partitions

Configure hierarchical date partitions for tables partitioned by year, month, day, and hour:

```python
from datetime import timedelta
from sqlranger import DateTablePartition, DatePartitionColumn

# Hierarchical date partitions: year > month > day > hour
partition_config = DateTablePartition(
    "gridhive.fact.sales_history",
    partitions=[
        DatePartitionColumn("year", "YYYY"),
        DatePartitionColumn("month", "mm"),
        DatePartitionColumn("day", "dd"),
        DatePartitionColumn("hour", "HH")
    ],
    enforced_level=4,  # All 4 levels must be specified
    max_date_range=timedelta(days=1, hours=12)  # Max 1.5 days
)

# This query is valid - all 4 partition levels are filtered
sql = """
    SELECT * FROM gridhive.fact.sales_history
    WHERE year = 2021 AND month = 9 AND day = 13 AND hour >= 10 AND hour <= 15
"""
```

### Configuration Classes

#### TablePartition

`TablePartition` is the base class for defining partition configuration. It specifies:
- The full table name (including schema/catalog if applicable)
- An ordered list of partition column names (from root to smallest sub-partition)
- Optional `enforced_level` parameter (int) specifying how many levels are enforced

**Example:**
```python
from sqlranger import TablePartition

# Simple single-level partition
tp = TablePartition("sales_history", ["day"])

# Hierarchical partition with 3 levels, only 2 enforced
tp = TablePartition(
    "warehouse.inventory",
    partitions=["City", "Warehouse", "BuildingNumber"],
    enforced_level=2  # Only City and Warehouse are enforced
)
```

#### DateTablePartition

`DateTablePartition` extends `TablePartition` with additional date-specific configuration:
- `partitions`: List of `DatePartitionColumn` objects (each with column name and format pattern)
- `enforced_level`: Optional number of partition levels to enforce
- `max_date_range`: Optional maximum allowed date range as timedelta object

**Example:**
```python
from datetime import timedelta
from sqlranger import DateTablePartition, DatePartitionColumn

# Single date partition with max range
dtp = DateTablePartition(
    "gridhive.fact.sales_history",
    partitions=[DatePartitionColumn("day", "YYYY-mm-dd")],
    max_date_range=timedelta(days=30)
)

# Hierarchical date partition
dtp = DateTablePartition(
    "gridhive.fact.sales_history",
    partitions=[
        DatePartitionColumn("year", "YYYY"),
        DatePartitionColumn("month", "mm"),
        DatePartitionColumn("day", "dd")
    ],
    enforced_level=3,  # All 3 levels enforced
    max_date_range=timedelta(days=90)
)
```

#### DatePartitionColumn

`DatePartitionColumn` is a dataclass that represents a single date partition column:
- `column_name`: Name of the partition column
- `format_pattern`: Date format pattern

**Supported Format Patterns:**

| Pattern      | Description                   | Example Value |
|--------------|-------------------------------|---------------|
| `YYYY`       | 4-digit year                  | `2021`        |
| `Y`          | Year (short form)             | `2021`        |
| `mm`         | 2-digit month                 | `09`          |
| `m`          | Month (short form)            | `9`           |
| `dd`         | 2-digit day                   | `13`          |
| `d`          | Day (short form)              | `13`          |
| `HH`         | 2-digit hour (24-hour format) | `14`          |
| `H`          | Hour (short form)             | `14`          |
| `MM`         | 2-digit minute                | `03`          |
| `M`          | Minutes (short form)          | `3`           |
| `SS`         | 2-digit second                | `45`          |
| `S`          | Second (short form)           | `45`          |
| `YYYY-mm-dd` | Full date string              | `2021-09-13`  |
| `YYYY-mm`    | Year-month string             | `2021-09`     |

**Notes:**
- Format patterns are case-sensitive for disambiguation (e.g., `mm` for month vs `MM` for minute)
- Composite patterns like `YYYY-mm-dd` are parsed as complete date strings
- When using individual components, each partition column should have its own `DatePartitionColumn`

**Example:**
```python
from sqlranger import DatePartitionColumn

# Define individual date partition columns
year_col = DatePartitionColumn("year", "YYYY")
month_col = DatePartitionColumn("month", "mm")
day_col = DatePartitionColumn("day", "dd")
hour_col = DatePartitionColumn("hour", "HH")

# Or use a full date format for a single column
full_date_col = DatePartitionColumn("day", "YYYY-mm-dd")
```

### Validation Rules

The partition checker enforces these rules:

1. **Partition Filter Required**: Any query using a partitioned table must include filters for all enforced partition columns in the WHERE clause
2. **Raw Column Only**: Partition columns must be used without functions (e.g., `day = '2021-09-13'` is OK, but `DATE_FORMAT(day, '%Y-%m')` breaks partitioning)
3. **Finite Range**: Queries must define a finite range for each partition column using:
    - `column = 'value'` (single value)
    - `column BETWEEN 'start' AND 'end'`
    - Both `column >= 'start'` AND `column <= 'end'`
4. **Optional Max Range**: When `max_date_range` is configured (via `DateTablePartition`), it enforces a maximum date range (best-effort estimation)
5. **Hierarchical Enforcement**: All partition columns from the root to `enforced_level` must be filtered

#### PartitionViolationType

| Violation | Description |
|--------|-------------|
| `MISSING_PARTITION_FILTER` | Query doesn't have a required partition column filter in the `WHERE` clause |
| `PARTITION_COLUMN_WITH_FUNCTION` | Partition column is wrapped in a function (breaks partitioning) |
| `NO_FINITE_RANGE` | Query doesn't define a finite range for a partition column |
| `EXCESSIVE_DATE_RANGE` | Date range exceeds the configured maximum |

#### PartitionViolation

`find_violations` returns a list of `PartitionViolation` objects, each containing:
- `violation`: Type of violation, see `PartitionViolationType` enum
- `message`: Human-or-LLM-readable description of the violation
- `table_name`: Name of the table with the violation
- `estimated_range`: Estimated excessive duration of time range for `EXCESSIVE_DATE_RANGE` violation type

### Return Values

The validation functions return a list of `PartitionViolation` objects:
- **Empty list**: All partitioned tables are properly filtered (no violations)
- **Non-empty list**: Contains violation details for each table that fails validation

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
WHERE DATE_FORMAT(day, '%Y-%M') = '2021-09'
```
Returns violation: `PARTITION_COLUMN_WITH_FUNCTION` - Table 'sales_history' uses 'day' column with a function, which disables partitioning.

**Invalid Query (no upper bound):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE day >= '2021-09-13'
```
Returns violation: `NO_FINITE_RANGE` - Table 'sales_history' does not have a finite date range
