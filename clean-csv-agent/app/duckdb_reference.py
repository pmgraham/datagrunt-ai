# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DUCKDB_SQL_REFERENCE = """
## DuckDB SQL Reference for Data Cleaning

NEVER use DELETE or DROP TABLE. Every row in the original data MUST be preserved.
To handle bad values, UPDATE them (e.g. SET to NULL). To handle duplicates, flag
them with a boolean column. Row count before and after cleaning MUST be identical.

All cleaning SQL must target a table called `data`. Do NOT use read_csv_auto()
in cleaning statements — the data is already loaded into `data`.

For querying (query_data tool), use the table name returned by load_csv.

### UPDATE rows
```sql
UPDATE data SET column_name = <expression> WHERE <condition>;
```

### ALTER TABLE
```sql
-- Rename a column
ALTER TABLE data RENAME COLUMN old_name TO new_name;

-- Change column type
ALTER TABLE data ALTER COLUMN column_name SET DATA TYPE new_type;

-- Add a column
ALTER TABLE data ADD COLUMN column_name data_type;

-- Drop a column
ALTER TABLE data DROP COLUMN column_name;
```

### Type Casting
DuckDB uses `CAST` and `TRY_CAST`. Use TRY_CAST when values may fail — it
returns NULL instead of erroring.
```sql
-- Hard cast (errors on failure)
CAST(column_name AS INTEGER)
CAST(column_name AS DOUBLE)
CAST(column_name AS DATE)
CAST(column_name AS BOOLEAN)

-- Safe cast (returns NULL on failure)
TRY_CAST(column_name AS DOUBLE)
TRY_CAST(column_name AS INTEGER)
TRY_CAST(column_name AS DATE)
TRY_CAST(column_name AS TIMESTAMP)
```

### Cleaning type-polluted columns and type coercion
When a column has mixed types (e.g. numbers + junk strings), clean it in two
steps: first UPDATE the bad values, then ALTER the type.
```sql
-- Step 1: NULL out values that can't be cast
UPDATE data
SET salary = NULL
WHERE TRY_CAST(salary AS DOUBLE) IS NULL AND salary IS NOT NULL;

-- Step 2: Change column type (use USING TRY_CAST for safety)
ALTER TABLE data ALTER COLUMN salary TYPE DOUBLE USING TRY_CAST(salary AS DOUBLE);

-- For integer columns (no decimals):
ALTER TABLE data ALTER COLUMN age TYPE INTEGER USING TRY_CAST(age AS INTEGER);

-- For date columns:
ALTER TABLE data ALTER COLUMN date_col TYPE DATE USING TRY_CAST(date_col AS DATE);
```

### NULL handling
```sql
-- Replace NULLs with a default
UPDATE data SET column_name = COALESCE(column_name, 'default_value');

-- Standardize NULL synonyms (case-insensitive)
UPDATE data SET column_name = NULL
WHERE LOWER(TRIM(column_name)) IN (
    'n/a', 'na', 'none', 'null', '-', '--', 'missing',
    'not available', 'unavailable', 'unknown', 'n.a.', ''
);

-- NULLIF: returns NULL if the two expressions are equal
UPDATE data SET column_name = NULLIF(column_name, '');
```

### String functions
```sql
-- Trim whitespace
UPDATE data SET name = TRIM(name);

-- Replace substrings
UPDATE data SET column_name = REPLACE(column_name, 'old', 'new');

-- Regex replace
UPDATE data SET column_name = REGEXP_REPLACE(column_name, 'pattern', 'replacement');

-- Extract with regex
REGEXP_EXTRACT(column_name, 'pattern', group_number)

-- Case conversion
UPPER(column_name)
LOWER(column_name)

-- Substring
SUBSTRING(column_name, start, length)
-- OR
column_name[start:end]
```

### Casing normalization
```sql
-- Title Case for long strings, UPPER for short codes (state abbrevs, etc.)
UPDATE data SET city = CASE
    WHEN LENGTH(TRIM(city)) <= 3 THEN UPPER(city)
    ELSE list_aggr(
        list_transform(
            string_split(city, ' '),
            x -> CONCAT(UPPER(LEFT(x, 1)), LOWER(SUBSTRING(x, 2)))
        ),
        'string_agg', ' '
    )
END
WHERE city IS NOT NULL;
```

### Numeric cleaning
```sql
-- Clamp outliers to a range
UPDATE data SET age = NULL WHERE age < 0 OR age > 120;

-- Round values
UPDATE data SET price = ROUND(price, 2);

-- Absolute value
UPDATE data SET value = ABS(value);
```

### Date and timestamp parsing
DuckDB auto-detects many date formats with TRY_CAST. For non-standard formats
use TRY_STRPTIME (safe) or STRPTIME (errors on failure):
```sql
-- Standardize mixed date formats to YYYY-MM-DD
UPDATE data SET date_col =
    strftime(
        COALESCE(
            TRY_STRPTIME(date_col, '%Y-%m-%d'),
            TRY_STRPTIME(date_col, '%m/%d/%Y'),
            TRY_STRPTIME(date_col, '%d/%m/%Y'),
            TRY_STRPTIME(date_col, '%m-%d-%Y'),
            TRY_STRPTIME(date_col, '%Y/%m/%d'),
            TRY_STRPTIME(date_col, '%d-%m-%Y'),
            TRY_STRPTIME(date_col, '%B %d, %Y'),
            TRY_STRPTIME(date_col, '%b %d, %Y'),
            TRY_STRPTIME(date_col, '%m/%d/%y'),
            TRY_STRPTIME(date_col, '%d/%m/%y')
        ),
        '%Y-%m-%d'
    )
WHERE date_col IS NOT NULL;

-- Format a date to a string
STRFTIME(date_col, '%Y-%m-%d')

-- Common format codes:
-- %Y = 4-digit year, %m = 2-digit month, %d = 2-digit day
-- %H = hour (24h), %M = minute, %S = second
-- %y = 2-digit year, %b = abbreviated month name, %B = full month name
```

### Deduplication
To flag duplicates, add a boolean column instead of deleting rows:
```sql
-- Flag duplicate rows (keeps first occurrence as is_duplicate = false)
ALTER TABLE data ADD COLUMN is_duplicate BOOLEAN DEFAULT false;
UPDATE data SET is_duplicate = true
WHERE rowid NOT IN (
    SELECT MIN(rowid) FROM data
    GROUP BY col1, col2, col3
);
```

### Conditional updates
```sql
-- CASE expressions
UPDATE data
SET category = CASE
    WHEN value > 100 THEN 'high'
    WHEN value > 50 THEN 'medium'
    ELSE 'low'
END;
```

### Boolean normalization
```sql
-- Convert various boolean representations to proper BOOLEAN
UPDATE data
SET is_active = CASE
    WHEN LOWER(is_active) IN ('true', '1', 'yes', 'y', 't') THEN 'true'
    WHEN LOWER(is_active) IN ('false', '0', 'no', 'n', 'f') THEN 'false'
    ELSE NULL
END;
```

### Column Name Normalization
Column names are automatically normalized to lowercase snake_case when loaded
(via `normalize_names=true`). For example:
- "First Name" → "first_name"
- "OrderID" → "order_id"
- "Total Amount ($)" → "total_amount____"

Always use the **normalized** column names in your SQL. Run `get_smart_schema`
to see the actual column names after normalization.

### IMPORTANT DuckDB-specific notes
- DuckDB uses `DOUBLE` not `FLOAT8` or `REAL` for double-precision floats.
- `VARCHAR` is the string type (not `TEXT` or `STRING`).
- `BOOLEAN` is a native type. Strings like 'true'/'false' auto-cast.
- Column names with spaces or special characters MUST be double-quoted: "my column".
- DuckDB supports `STRPTIME` (string→timestamp) and `STRFTIME` (timestamp→string).
- There is no `SAFE_CAST` — use `TRY_CAST` instead.
- `ALTER COLUMN ... SET DATA TYPE` will fail if existing values can't be cast.
  Always clean bad values first, then alter the type.
- `rowid` is a built-in pseudo-column for identifying rows.
- String concatenation uses `||` operator: `col1 || ' ' || col2`.
- Use `EPOCH` to extract unix timestamp: `EPOCH(timestamp_col)`.
"""
