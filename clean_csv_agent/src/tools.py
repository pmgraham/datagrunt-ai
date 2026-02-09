import os
import re as _re
import tempfile
from typing import Any, Dict, List

import duckdb
import polars as pl
from google.adk.tools import ToolContext

from clean_csv_agent.src.datagrunt import CSVReader, DuckDBQueries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_path(path: str) -> str:
    """Validates that the path is absolute and exists.
    
    Prevents directory traversal attacks by ensuring the resolved path
    is within allowed boundaries (implicit by OS permissions for now).
    """
    if not path:
        raise ValueError("Path cannot be empty")
    
    # Resolve absolute path
    abs_path = os.path.abspath(path)
    
    # Check if file exists
    if not os.path.exists(abs_path):
        raise ValueError(f"File not found: {path}")
        
    return abs_path

# Module-level cache for CSVReader instances (keyed by file path)
_readers: Dict[str, CSVReader] = {}


def _get_reader(tool_context: ToolContext) -> CSVReader:
    """Retrieve or create a CSVReader for the current session's CSV file."""
    csv_path = tool_context.state.get("csv_path")
    if not csv_path:
        raise ValueError("No CSV loaded. Use load_csv first.")
    if csv_path not in _readers:
        reader = CSVReader(csv_path, engine="duckdb")
        _readers[csv_path] = reader
        _ensure_table(reader)
    return _readers[csv_path]


def _ensure_table(reader: CSVReader):
    """Ensure the normalized table exists in DuckDB's default connection."""
    table = reader.db_table
    try:
        duckdb.sql(f"SELECT 1 FROM {table} LIMIT 0")
    except Exception:
        queries = DuckDBQueries(reader.filepath)
        duckdb.sql(queries.import_csv_query_normalize_columns())


def _get_column_names(table: str) -> List[str]:
    """Return the list of column names for a DuckDB table."""
    return [
        row["column_name"]
        for row in duckdb.sql(f"DESCRIBE {table}").pl().to_dicts()
    ]


def _validate_column(column: str, table: str) -> Dict[str, Any] | None:
    """Return an error dict if *column* does not exist in *table*, else None."""
    columns = _get_column_names(table)
    if column not in columns:
        return {
            "error": f"Column '{column}' does not exist.",
            "available_columns": columns,
            "table_name": table,
        }
    return None


_DESTRUCTIVE_PATTERN = _re.compile(
    r"^\s*(DELETE\b|DROP\s+TABLE\b|TRUNCATE\b)",
    _re.IGNORECASE,
)


def _reject_destructive(sql: str) -> Dict[str, Any] | None:
    """Return an error dict if the SQL would remove rows, else None."""
    if _DESTRUCTIVE_PATTERN.search(sql):
        return {
            "error": (
                "DELETE, DROP TABLE, and TRUNCATE are not allowed. "
                "Rows must never be removed. Use UPDATE to fix values "
                "or add a flag column to mark problematic rows."
            ),
            "rejected_sql": sql,
        }
    return None


def _run_sql_safe(sql: str, table: str) -> pl.DataFrame:
    """Execute SQL and raise a helpful error with column names on binder failures."""
    try:
        return duckdb.sql(sql).pl()
    except duckdb.BinderException as e:
        columns = _get_column_names(table)
        raise duckdb.BinderException(
            f"{e}\n\nAvailable columns in '{table}': {columns}"
        ) from None


def _to_markdown(frame: pl.DataFrame, exclude: List[str] = None) -> str:
    """Convert a Polars DataFrame to a markdown table string."""
    if frame.is_empty():
        return "No rows."
    exclude = exclude or []
    cols = [c for c in frame.columns if c not in exclude]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    rows = []
    for row in frame.to_dicts():
        row_str = "| " + " | ".join(str(row[c]) for c in cols) + " |"
        rows.append(row_str)
    return "\n".join([header, sep] + rows)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

def inspect_raw_file(tool_context: ToolContext) -> Dict[str, Any]:
    """Reads the first few lines of the raw file to diagnose loading issues.

    Use this if load_csv fails. It helps identify the delimiter (comma, semicolon, tab),
    encoding issues, or unusual headers.
    """
    csv_path = tool_context.state.get("csv_path")
    if not csv_path or not os.path.exists(csv_path):
        return {"error": "No file path found to inspect."}

    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            lines = [f.readline() for _ in range(15)]
        return {
            "file_path": csv_path,
            "raw_lines": lines,
            "message": "Inspect these lines to determine the correct delimiter or if there is a header issue."
        }
    except Exception as e:
        return {"error": f"Failed to read raw file: {e}"}


def load_csv(
    tool_context: ToolContext,
    file_path: str = "",
    csv_content: str = "",
    sep: str = ",",
) -> Dict[str, Any]:
    """Loads data into the session for analysis.

    Use file_path for files on disk. If the first attempt fails, use
    inspect_raw_file to find the right 'sep' (e.g. ';' or '\\t') and try again.
    """
    if csv_content and not file_path:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        )
        tmp.write(csv_content)
        tmp.close()
        file_path = tmp.name

    if not file_path:
        file_path = tool_context.state.get("csv_path", "")

    if not file_path or not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}
        
    try:
        file_path = _validate_path(file_path)
    except ValueError as e:
        return {"error": str(e)}

    # Store path early so inspect_raw_file can use it if we fail
    tool_context.state["csv_path"] = file_path

    # Count source lines (minus header) for verification — fast binary counting
    source_line_count = 0
    with open(file_path, "rb") as fh:
        while True:
            buf = fh.read(1024 * 1024)
            if not buf:
                break
            source_line_count += buf.count(b"\n")
    source_line_count = max(source_line_count - 1, 0)

    try:
        duckdb.sql("INSTALL icu; LOAD icu;")

        reader = CSVReader(file_path, engine="duckdb")
        _readers[file_path] = reader

        table = reader.db_table

        # Load with tolerant flags — never drop rows
        duckdb.sql(f"""
            CREATE OR REPLACE TABLE raw_data AS
            SELECT * FROM read_csv(
                '{file_path}',
                sep = '{sep}',
                auto_detect = true,
                strict_mode = false,
                null_padding = true,
                all_varchar = true
            )
        """)

        # Normalize column names to lowercase snake_case
        cols = duckdb.sql("DESCRIBE raw_data").pl()
        norm_queries = []
        for c in cols.to_dicts():
            old_name = c['column_name']
            temp = _re.sub('(.)([A-Z][a-z]+)', r'\1_\2', old_name)
            new_name = _re.sub('([a-z0-9])([A-Z])', r'\1_\2', temp).lower()
            new_name = _re.sub('[^a-z0-9_]', '_', new_name)
            new_name = _re.sub('_+', '_', new_name).strip('_')
            norm_queries.append(f"\"{old_name}\" AS \"{new_name}\"")

        duckdb.sql(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT {', '.join(norm_queries)} FROM raw_data
        """)
        duckdb.sql("DROP TABLE raw_data")

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "error": f"Could not load data: {e}",
            "suggestion": "Run 'inspect_raw_file' to see what's wrong with the file format."
        }

    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    columns = duckdb.sql(f"DESCRIBE {table}").pl().to_dicts()
    sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    # Verify no rows were lost
    rows_lost = source_line_count - total_rows

    result = {
        "status": "success",
        "total_rows": total_rows,
        "source_rows": source_line_count,
        "table_name": table,
        "columns": [
            {"name": c["column_name"], "type": c["column_type"]}
            for c in columns
        ],
        "sample": _to_markdown(sample),
    }

    if rows_lost > 0:
        result["warning"] = (
            f"{rows_lost} rows from the source file were not loaded. "
            "Inspect the raw file for encoding or delimiter issues."
        )

    return result


def get_smart_schema(tool_context: ToolContext) -> Dict[str, Any]:
    """Identifies schema and quality metrics (nulls/uniques) for the loaded CSV."""
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    analysis = duckdb.sql(f"""
        SELECT
            column_name,
            column_type,
            approx_unique,
            null_percentage::FLOAT as null_percentage
        FROM (SUMMARIZE SELECT * FROM {table})
    """).pl()

    total_count = reader.row_count_without_header

    return {
        "total_records": total_count,
        "columns": analysis.to_dicts(),
    }


def detect_advanced_anomalies(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Uses IQR (Tukey's Fences) to find outliers in a numerical column."""
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    bad = _validate_column(column, table)
    if bad:
        return bad

    stats = duckdb.sql(f"""
        SELECT
            approx_quantile(try_cast("{column}" AS DOUBLE), 0.25) as q1,
            approx_quantile(try_cast("{column}" AS DOUBLE), 0.75) as q3
        FROM {table}
        WHERE try_cast("{column}" AS DOUBLE) IS NOT NULL
    """).pl()

    if stats.is_empty():
        return {"column": column, "iqr_bounds": [], "samples": []}

    stats = stats.to_dicts()[0]
    q1 = stats["q1"]
    q3 = stats["q3"]

    if q1 is None or q3 is None:
        return {"column": column, "iqr_bounds": [], "samples": []}

    iqr = q3 - q1
    upper = q3 + (1.5 * iqr)
    lower = q1 - (1.5 * iqr)

    outliers = duckdb.sql(f"""
        SELECT *, 'Outlier' as reason FROM {table}
        WHERE try_cast("{column}" AS DOUBLE) > {upper}
           OR try_cast("{column}" AS DOUBLE) < {lower} LIMIT 5
    """).pl()

    return {
        "column": column,
        "iqr_bounds": [lower, upper],
        "samples": _to_markdown(outliers),
    }


def detect_type_pollution(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Finds values that don't match the column's main type (e.g., text in a number column).

    It looks for 'number-words' (like 'five') or symbols (like '$') that can be
    converted rather than deleted.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    bad = _validate_column(column, table)
    if bad:
        return bad

    pollutants = duckdb.sql(f"""
        SELECT "{column}" as value, COUNT(*) as count
        FROM {table}
        WHERE try_cast("{column}" AS DOUBLE) IS NULL AND "{column}" IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).pl()

    # Look specifically for number words or currency to suggest conversion
    number_words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
    conversions_found = [
        v['value'] for v in pollutants.to_dicts() 
        if v['value'].lower().strip() in number_words or any(c in v['value'] for c in '$%')
    ]

    return {
        "column": column,
        "pollutants": pollutants.to_dicts(),
        "recoverable_values": conversions_found,
        "suggestion": "Convert these to numbers (e.g., 'five' -> 5) instead of deleting them." if conversions_found else "Check if these are typos or should be cleared."
    }


def get_value_distribution(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Shows the top unique values and their counts to spot typos or casing issues.

    Use this to identify if 'New York' and 'new york' are both present,
    suggesting a need for standardization.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    bad = _validate_column(column, table)
    if bad:
        return bad

    dist = duckdb.sql(f"""
        SELECT "{column}" as value, COUNT(*) as count
        FROM {table}
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
    """).pl()

    return {
        "column": column,
        "distribution": dist.to_dicts(),
        "total_unique": duckdb.sql(f"SELECT COUNT(DISTINCT \"{column}\") FROM {table}").fetchone()[0]
    }


def detect_date_formats(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Identifies inconsistent date formats within a single column.

    Returns samples of different patterns found (e.g., MM/DD/YYYY vs YYYY-MM-DD).
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    bad = _validate_column(column, table)
    if bad:
        return bad

    # Try common formats and see which ones match
    formats = [
        ('%m/%d/%Y', 'MM/DD/YYYY'),
        ('%d/%m/%Y', 'DD/MM/YYYY'),
        ('%Y-%m-%d', 'YYYY-MM-DD'),
        ('%Y/%m/%d', 'YYYY/MM/DD'),
        ('%d-%b-%Y', 'DD-Mon-YYYY')
    ]
    
    results = []
    for fmt, label in formats:
        match_count = duckdb.sql(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE try_cast(try_strptime("{column}"::VARCHAR, '{fmt}') AS DATE) IS NOT NULL
        """).fetchone()[0]
        if match_count > 0:
            results.append({"format": label, "count": match_count})

    return {
        "column": column,
        "found_formats": results,
        "mixed_formats": len(results) > 1
    }


def suggest_type_coercion(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Suggests if a text column should actually be a Number, Date, or Boolean.

    It checks if values would be valid after removing currency symbols or trimming spaces.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    bad = _validate_column(column, table)
    if bad:
        return bad

    # Check for Number potential (removing $ and %)
    number_potential = duckdb.sql(f"""
        SELECT COUNT(*) FROM {table}
        WHERE try_cast(regexp_replace("{column}"::VARCHAR, '[\\$\\%\\,]', '', 'g') AS DOUBLE) IS NOT NULL
          AND "{column}" IS NOT NULL
    """).fetchone()[0]

    # Check for Date potential
    date_potential = duckdb.sql(f"""
        SELECT COUNT(*) FROM {table}
        WHERE try_cast("{column}" AS DATE) IS NOT NULL
          OR try_cast(try_strptime("{column}"::VARCHAR, '%m/%d/%Y') AS DATE) IS NOT NULL
    """).fetchone()[0]

    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table} WHERE \"{column}\" IS NOT NULL").fetchone()[0]

    suggestions = []
    if total_rows > 0:
        if number_potential / total_rows > 0.9:
            suggestions.append("Number")
        if date_potential / total_rows > 0.9:
            suggestions.append("Date")

    return {
        "column": column,
        "suggested_types": suggestions,
        "confidence": "high" if suggestions else "none"
    }


def check_column_logic(col_a: str, col_b: str, operator: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Checks for logical errors between two columns (e.g., Ship Date < Order Date).

    Supported operators: '<', '>', '=', '!='.
    Returns a sample of rows that fail the logic test.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    for col in (col_a, col_b):
        bad = _validate_column(col, table)
        if bad:
            return bad

    # Map friendly names to operators
    op_map = {'less than': '<', 'greater than': '>', 'equal to': '=', 'not equal to': '!='}
    actual_op = op_map.get(operator.lower(), operator)

    try:
        failures = duckdb.sql(f"""
            SELECT * FROM {table}
            WHERE try_cast("{col_a}" AS DOUBLE) {actual_op} try_cast("{col_b}" AS DOUBLE)
               OR try_cast("{col_a}" AS DATE) {actual_op} try_cast("{col_b}" AS DATE)
            LIMIT 10
        """).pl()
        
        fail_count = duckdb.sql(f"""
            SELECT COUNT(*) FROM {table}
            WHERE try_cast("{col_a}" AS DOUBLE) {actual_op} try_cast("{col_b}" AS DOUBLE)
               OR try_cast("{col_a}" AS DATE) {actual_op} try_cast("{col_b}" AS DATE)
        """).fetchone()[0]
    except Exception as e:
        return {"error": f"Could not compare columns: {e}"}

    return {
        "comparison": f"{col_a} {actual_op} {col_b}",
        "issue_count": fail_count,
        "samples": _to_markdown(failures)
    }


def query_data(sql: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Runs a DuckDB SQL query against the loaded CSV and returns a markdown table.

    The CSV is available as a table whose name was returned by load_csv.
    Write SELECT queries using ONLY the column names listed in the response.
    Do NOT guess or invent column names.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)

    try:
        result = _run_sql_safe(sql, table)
    except Exception as e:
        return {
            "error": str(e),
            "available_columns": columns,
            "table_name": table,
        }

    if result.is_empty():
        return {
            "result": "No results found.",
            "available_columns": columns,
        }

    return {
        "result": _to_markdown(result),
        "available_columns": columns,
    }


def preview_full_plan(sql_statements: List[str], tool_context: ToolContext) -> Dict[str, Any]:
    """Shows the cumulative impact of all proposed cleaning steps in one view.

    It applies all SQL statements to a table called 'data' and returns a
    comparison of the first 10 rows before and after. The SQL statements
    should already target a table called 'data'.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)

    # Copy source into 'data' (what the SQL targets) with row IDs for tracking
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE data AS
        SELECT ROW_NUMBER() OVER () as _row_id, *
        FROM {table}
    """)

    before = duckdb.sql("SELECT * FROM data LIMIT 10").pl()

    # Apply all steps directly — SQL already targets 'data'
    errors = []
    for sql in sql_statements:
        blocked = _reject_destructive(sql)
        if blocked:
            errors.append(blocked)
            continue
        try:
            duckdb.sql(sql)
        except duckdb.BinderException as e:
            errors.append({
                "sql": sql,
                "error": str(e),
                "available_columns": columns,
            })
        except Exception as e:
            errors.append({"sql": sql, "error": str(e)})

    if before.to_dicts():
        row_ids = [r["_row_id"] for r in before.to_dicts()]
        placeholders = ", ".join(str(rid) for rid in row_ids)
        after = duckdb.sql(
            f"SELECT * FROM data WHERE _row_id IN ({placeholders})"
        ).pl()
    else:
        after = duckdb.sql("SELECT * FROM data LIMIT 10").pl()

    return {
        "before": _to_markdown(before, exclude=["_row_id"]),
        "after": _to_markdown(after, exclude=["_row_id"]),
        "errors": errors,
    }


def validate_cleaned_data(tool_context: ToolContext) -> Dict[str, Any]:
    """Validates the cleaned CSV for BigQuery readiness.

    Checks every column for: remaining nulls, type-cast failures, and value
    range sanity. Returns a per-column report with a pass/fail verdict.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    schema = duckdb.sql(
        f"DESCRIBE SELECT * FROM {table}"
    ).pl().to_dicts()

    total_rows = reader.row_count_without_header

    column_reports = []
    all_pass = True

    for col in schema:
        col_name = col["column_name"]
        col_type = col["column_type"]
        issues = []

        null_count = duckdb.sql(f"""
            SELECT COUNT(*) as cnt FROM {table}
            WHERE "{col_name}" IS NULL
        """).pl().to_dicts()[0]["cnt"]
        if null_count > 0:
            issues.append(
                f"{null_count} nulls ({null_count * 100 / total_rows:.1f}%)"
            )

        base_type = col_type.split("(")[0]
        if base_type in ("BIGINT", "INTEGER", "SMALLINT", "DOUBLE", "FLOAT"):
            bad_cast = duckdb.sql(f"""
                SELECT COUNT(*) as cnt FROM {table}
                WHERE "{col_name}" IS NOT NULL
                  AND try_cast("{col_name}" AS {col_type}) IS NULL
            """).pl().to_dicts()[0]["cnt"]
            if bad_cast > 0:
                issues.append(f"{bad_cast} values fail cast to {col_type}")

        if base_type in ("BIGINT", "INTEGER", "SMALLINT", "DOUBLE", "FLOAT"):
            range_info = duckdb.sql(f"""
                SELECT
                    MIN(try_cast("{col_name}" AS DOUBLE)) as min_val,
                    MAX(try_cast("{col_name}" AS DOUBLE)) as max_val
                FROM {table}
                WHERE try_cast("{col_name}" AS DOUBLE) IS NOT NULL
            """).pl().to_dicts()[0]
            col_range = {"min": range_info["min_val"], "max": range_info["max_val"]}
        else:
            col_range = None

        passed = len(issues) == 0
        if not passed:
            all_pass = False

        report = {
            "column": col_name,
            "type": col_type,
            "passed": passed,
            "issues": issues,
        }
        if col_range:
            report["range"] = col_range
        column_reports.append(report)

    return {
        "total_rows": total_rows,
        "bigquery_ready": all_pass,
        "columns": column_reports,
    }


def execute_cleaning_plan(
    sql_statements: List[str], tool_context: ToolContext
) -> Dict[str, Any]:
    """Executes approved DuckDB SQL cleaning statements against the loaded CSV.

    Loads the CSV into a table called 'data', runs each SQL statement
    sequentially, exports the cleaned result to a new file, and updates the
    session state so subsequent tools use the cleaned data.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    # Copy to 'data' table for cleaning operations
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE data AS
        SELECT * FROM {table}
    """)

    rows_before = duckdb.sql("SELECT COUNT(*) FROM data").fetchone()[0]
    columns = _get_column_names("data")

    executed = []
    for i, sql in enumerate(sql_statements):
        blocked = _reject_destructive(sql)
        if blocked:
            executed.append({"step": i + 1, "status": "blocked", **blocked})
            continue
        try:
            duckdb.sql(sql)
            executed.append({"step": i + 1, "sql": sql, "status": "ok"})
        except duckdb.BinderException as e:
            executed.append({
                "step": i + 1,
                "sql": sql,
                "status": "error",
                "error": str(e),
                "available_columns": columns,
            })
        except Exception as e:
            executed.append({
                "step": i + 1,
                "sql": sql,
                "status": "error",
                "error": str(e),
            })

    # Verify no rows were lost during cleaning
    rows_after = duckdb.sql("SELECT COUNT(*) FROM data").fetchone()[0]
    if rows_after < rows_before:
        # Roll back — re-copy from source table
        duckdb.sql(f"""
            CREATE OR REPLACE TABLE data AS
            SELECT * FROM {table}
        """)
        return {
            "error": (
                f"Cleaning dropped {rows_before - rows_after} rows "
                f"({rows_before} → {rows_after}). "
                "Changes were rolled back. Rewrite the plan using only "
                "UPDATE statements — never DELETE rows."
            ),
            "steps_executed": executed,
        }

    # Save cleaned file next to the original with a _cleaned suffix
    old_path = tool_context.state.get("csv_path")
    base, ext = os.path.splitext(old_path)
    cleaned_path = f"{base}_cleaned{ext}"

    duckdb.sql(f"COPY data TO '{cleaned_path}' (HEADER, DELIMITER ',')")

    # Clear old reader, update session to point to cleaned file
    if old_path in _readers:
        del _readers[old_path]
    tool_context.state["csv_path"] = cleaned_path

    sample = duckdb.sql("SELECT * FROM data LIMIT 5").pl()

    return {
        "total_rows": rows_after,
        "rows_before": rows_before,
        "steps_executed": executed,
        "cleaned_file": cleaned_path,
        "sample": _to_markdown(sample),
    }


# ---------------------------------------------------------------------------
# Column Overflow Detection and Repair
# ---------------------------------------------------------------------------

def detect_column_overflow(tool_context: ToolContext) -> Dict[str, Any]:
    """Detects column overflow where fields are split across multiple columns.

    This happens when a CSV field contains unquoted delimiters, causing data
    to shift into extra columns. The tool checks three indicators:

    1. Sequential Sparsity: Columns at the end that are mostly NULL
    2. Row-Level Value Variance: Some rows have more non-null values than others
    3. Overflow Column Pattern: Columns named like 'column_N' or 'unnamed_N'

    Run this immediately after load_csv to detect structural issues.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    findings = {
        "overflow_detected": False,
        "indicators": [],
        "suspected_anchor_column": None,
        "overflow_columns": [],
        "total_columns": len(columns),
        "total_rows": total_rows,
    }

    # 1. Check for sequential sparsity at the end of the table
    # Get null counts per column
    null_counts = {}
    for col in columns:
        result = duckdb.sql(f'''
            SELECT COUNT(*) - COUNT("{col}") as null_count
            FROM {table}
        ''').fetchone()
        null_counts[col] = result[0]

    # Find columns at the end that are mostly NULL (>80% null)
    sparse_threshold = total_rows * 0.8
    overflow_cols = []
    for col in reversed(columns):
        if null_counts[col] >= sparse_threshold:
            overflow_cols.insert(0, col)
        else:
            break  # Stop when we hit a non-sparse column

    if overflow_cols:
        findings["indicators"].append({
            "type": "sequential_sparsity",
            "description": f"Found {len(overflow_cols)} columns at end that are >80% NULL",
            "columns": overflow_cols,
        })
        findings["overflow_columns"] = overflow_cols

    # 2. Check for row-level value count variance
    # Build a query to count non-null values per row
    count_exprs = " + ".join([f'CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END' for col in columns])
    variance_query = f'''
        SELECT
            ({count_exprs}) as non_null_count,
            COUNT(*) as row_count
        FROM {table}
        GROUP BY non_null_count
        ORDER BY non_null_count
    '''
    variance_result = duckdb.sql(variance_query).pl()

    if len(variance_result) > 1:
        min_count = variance_result["non_null_count"].min()
        max_count = variance_result["non_null_count"].max()
        if max_count - min_count >= 2:  # Significant variance
            findings["indicators"].append({
                "type": "row_value_variance",
                "description": f"Rows have between {min_count} and {max_count} non-null values",
                "distribution": variance_result.to_dicts(),
            })

    # 3. Check for overflow column naming patterns
    overflow_pattern = _re.compile(r'^(column_?\d+|unnamed[_:]?\d*|field_?\d+|_\d+)$', _re.IGNORECASE)
    pattern_matches = [col for col in columns if overflow_pattern.match(col)]

    if pattern_matches:
        findings["indicators"].append({
            "type": "overflow_column_names",
            "description": f"Found {len(pattern_matches)} columns with overflow-like names",
            "columns": pattern_matches,
        })
        # Extend overflow columns with pattern matches
        for col in pattern_matches:
            if col not in findings["overflow_columns"]:
                findings["overflow_columns"].append(col)

    # Determine if overflow is detected
    if len(findings["indicators"]) >= 2 or (
        len(findings["indicators"]) == 1 and
        findings["indicators"][0]["type"] == "row_value_variance"
    ):
        findings["overflow_detected"] = True

        # Try to identify the anchor column (last non-overflow column before the overflow)
        if findings["overflow_columns"]:
            first_overflow = findings["overflow_columns"][0]
            overflow_idx = columns.index(first_overflow)
            if overflow_idx > 0:
                findings["suspected_anchor_column"] = columns[overflow_idx - 1]

    return findings


def repair_column_overflow(
    anchor_column: str,
    expected_columns: List[str],
    tool_context: ToolContext
) -> Dict[str, Any]:
    """Repairs column overflow by reconstructing shifted data.

    When a CSV field contains unquoted delimiters, data shifts into extra columns.
    This tool reconstructs the original structure by:

    1. Concatenating all columns from anchor_column to the end
    2. Using regex to properly parse quoted vs unquoted fields
    3. Mapping extracted fields back to the expected schema
    4. Dropping the empty overflow columns

    Args:
        anchor_column: The column where overflow begins (text-heavy field)
        expected_columns: List of column names that SHOULD exist after anchor
                         (e.g., ['outcome', 'status', 'is_duplicate'])

    Returns:
        Result with before/after samples and validation metrics.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)

    # Validate anchor column exists
    if anchor_column not in columns:
        return {
            "error": f"Anchor column '{anchor_column}' not found",
            "available_columns": columns,
        }

    anchor_idx = columns.index(anchor_column)
    columns_before_anchor = columns[:anchor_idx]
    columns_from_anchor = columns[anchor_idx:]

    # Snapshot before
    before_sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    # Create the concatenated string from anchor to end
    concat_parts = [f'COALESCE(CAST("{col}" AS VARCHAR), \'\')' for col in columns_from_anchor]
    concat_expr = " || ',' || ".join(concat_parts)

    # Use DuckDB's regexp_extract_all to parse respecting quotes
    # Pattern: Match either "quoted content" or non-comma content
    parse_query = f'''
        SELECT
            {", ".join([f'"{col}"' for col in columns_before_anchor])},
            regexp_extract_all({concat_expr}, '("(?:[^"]|"")*"|[^,]*)') as parsed_fields
        FROM {table}
    '''

    try:
        parsed = duckdb.sql(parse_query).pl()
    except Exception as e:
        return {
            "error": f"Failed to parse overflow: {str(e)}",
            "query": parse_query,
        }

    # Determine how many fields we expect after the anchor
    # anchor_column + expected_columns
    expected_field_count = 1 + len(expected_columns)

    # Build the reconstruction query
    # We need to handle cases where there are more parsed fields than expected
    # by concatenating excess fields into the anchor column

    reconstruction_cases = []
    for i, row in enumerate(parsed.to_dicts()):
        fields = row.get("parsed_fields", [])
        # Clean up fields - remove surrounding quotes
        cleaned = []
        for f in fields:
            if f and f.startswith('"') and f.endswith('"'):
                cleaned.append(f[1:-1].replace('""', '"'))
            else:
                cleaned.append(f if f else None)

        # If more fields than expected, merge extras into anchor
        if len(cleaned) > expected_field_count:
            excess = len(cleaned) - expected_field_count
            anchor_parts = cleaned[:excess + 1]
            anchor_value = ", ".join([p for p in anchor_parts if p])
            final_fields = [anchor_value] + cleaned[excess + 1:]
        else:
            final_fields = cleaned

        reconstruction_cases.append(final_fields)

    # Create new table with corrected structure
    new_columns = columns_before_anchor + [anchor_column] + expected_columns

    # Build VALUES clause for the corrected data
    # This is a simplified approach - for large datasets we'd use a different method
    rows_data = []
    original_data = duckdb.sql(f"SELECT * FROM {table}").pl().to_dicts()

    for i, orig_row in enumerate(original_data):
        new_row = {}
        # Copy columns before anchor
        for col in columns_before_anchor:
            new_row[col] = orig_row[col]

        # Get parsed fields for this row
        if i < len(reconstruction_cases):
            fields = reconstruction_cases[i]
            # Map to new columns
            for j, col in enumerate([anchor_column] + expected_columns):
                if j < len(fields):
                    new_row[col] = fields[j]
                else:
                    new_row[col] = None
        rows_data.append(new_row)

    # Create new table
    # First create empty table with correct schema
    col_defs = ", ".join([f'"{col}" VARCHAR' for col in new_columns])
    duckdb.sql(f"CREATE OR REPLACE TABLE {table}_repaired ({col_defs})")

    # Insert data
    if rows_data:
        # Use parameterized insert for safety
        placeholders = ", ".join(["?" for _ in new_columns])
        insert_sql = f'INSERT INTO {table}_repaired VALUES ({placeholders})'

        for row in rows_data:
            values = [row.get(col) for col in new_columns]
            duckdb.execute(insert_sql, values)

    # Replace original table
    duckdb.sql(f"DROP TABLE IF EXISTS {table}")
    duckdb.sql(f"ALTER TABLE {table}_repaired RENAME TO {table}")

    # Validate: check row count preserved and value variance eliminated
    after_count = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    after_sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    # Check value variance after repair
    new_columns_list = _get_column_names(table)
    count_exprs = " + ".join([f'CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END' for col in new_columns_list])
    variance_after = duckdb.sql(f'''
        SELECT
            ({count_exprs}) as non_null_count,
            COUNT(*) as row_count
        FROM {table}
        GROUP BY non_null_count
    ''').pl()

    variance_reduced = len(variance_after) == 1

    return {
        "success": True,
        "rows_processed": after_count,
        "columns_before": len(columns),
        "columns_after": len(new_columns_list),
        "columns_removed": len(columns) - len(new_columns_list),
        "variance_eliminated": variance_reduced,
        "new_schema": new_columns_list,
        "before_sample": _to_markdown(before_sample),
        "after_sample": _to_markdown(after_sample),
    }
