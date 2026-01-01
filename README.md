# SQL Ranger

Enforcer of partitioning or finite-range check presence in SQL Queries

## Partition Usage Validation

The `PartitionChecker` validates SQL queries to ensure they properly use partitioning on large tables. This is critical for query performance, as some tables (like `mpcdsplogevent` and `logevent`) are partitioned by day and hour.

### Why Partition Validation?

Large partitioned tables should always be queried with partition filters to:
- Limit the amount of data scanned
- Improve query performance dramatically
- Prevent accidental full table scans

### Usage

```python
from sql_ranger import check_partition_usage, PartitionCheckStatus

# Simple validation
sql = """
    SELECT day, count(*) AS total
    FROM gridhive.fact.sales_history
    WHERE day = '2021-09-13'
"""
results = check_partition_usage(sql, partitioned_tables=["sales_history"])

for result in results:
    if result.status == PartitionCheckStatus.VALID:
        print(f"✓ {result.message}")
    else:
        print(f"✗ {result.message}")
```

### Validation Rules

The partition checker enforces these rules:

1. **Day Filter Required**: Any query using a partitioned table must include a `day` column filter in the WHERE clause
2. **Raw Column Only**: The `day` column must be used without functions (e.g., `day = '2021-09-13'` is OK, but `DATE_FORMAT(day, '%Y-%m')` breaks partitioning)
3. **Finite Range**: Queries must define a finite date range using:
    - `day = 'date'` (single date)
    - `day BETWEEN 'start' AND 'end'`
    - Both `day >= 'start'` AND `day <= 'end'`
4. **Optional Max Range**: When `max_days` is configured, enforces maximum date range (best-effort estimation)

### Result Status Types

| Status | Description |
|--------|-------------|
| `VALID` | Query properly uses partitioning |
| `MISSING_DAY_FILTER` | Query doesn't have a `day` filter in WHERE clause |
| `DAY_FILTER_WITH_FUNCTION` | Day column is wrapped in a function (breaks partitioning) |
| `NO_FINITE_RANGE` | Query doesn't define a finite date range |
| `EXCESSIVE_DATE_RANGE` | Date range exceeds the configured maximum |

### Example Validation Results

**Valid Query:**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE day BETWEEN '2021-09-13' AND '2021-09-26'
```
✓ Table 'sales_history' has proper partition filtering

**Invalid Query (function on day):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE DATE_FORMAT(day, '%Y-%m') = '2021-09'
```
✗ Table 'sales_history' uses 'day' column with a function, which disables partitioning

**Invalid Query (no upper bound):**
```sql
SELECT * FROM gridhive.fact.sales_history
WHERE day >= '2021-09-13'
```
✗ Table 'sales_history' does not have a finite date range