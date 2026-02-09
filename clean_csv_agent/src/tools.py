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


def _check_overflow_columns(table: str) -> List[str]:
    """Check for overflow columns (>80% NULL at the end of the table)."""
    columns = _get_column_names(table)
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total_rows == 0:
        return []

    sparse_threshold = total_rows * 0.8
    overflow_cols = []

    for col in reversed(columns):
        null_count = duckdb.sql(f'''
            SELECT COUNT(*) - COUNT("{col}") FROM {table}
        ''').fetchone()[0]
        if null_count >= sparse_threshold:
            overflow_cols.insert(0, col)
        else:
            break

    return overflow_cols


def _normalize_column_names(table: str) -> None:
    """Normalize column names to lowercase snake_case in place."""
    cols = duckdb.sql(f"DESCRIBE {table}").pl()
    renames = []
    for c in cols.to_dicts():
        old_name = c['column_name']
        temp = _re.sub('(.)([A-Z][a-z]+)', r'\1_\2', old_name)
        new_name = _re.sub('([a-z0-9])([A-Z])', r'\1_\2', temp).lower()
        new_name = _re.sub('[^a-z0-9_]', '_', new_name)
        new_name = _re.sub('_+', '_', new_name).strip('_')
        if not new_name:
            new_name = f"column_{cols.to_dicts().index(c)}"
        if new_name != old_name:
            renames.append((old_name, new_name))

    for old_name, new_name in renames:
        try:
            duckdb.sql(f'ALTER TABLE {table} RENAME COLUMN "{old_name}" TO "{new_name}"')
        except Exception:
            pass  # Skip if rename fails (e.g., duplicate names)


def _try_load_csv(file_path: str, table: str, sep: str, quote: str = '"', escape: str = '"') -> bool:
    """Try to load CSV with specific quote/escape params. Returns True on success."""
    try:
        if quote:
            duckdb.sql(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM read_csv(
                    '{file_path}',
                    sep = '{sep}',
                    quote = '{quote}',
                    escape = '{escape}',
                    auto_detect = true,
                    strict_mode = false,
                    null_padding = true,
                    all_varchar = true
                )
            """)
        else:
            duckdb.sql(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM read_csv(
                    '{file_path}',
                    sep = '{sep}',
                    auto_detect = true,
                    strict_mode = false,
                    null_padding = true,
                    all_varchar = true,
                    ignore_errors = true
                )
            """)
        return True
    except Exception:
        return False


def load_csv(
    tool_context: ToolContext,
    file_path: str = "",
    csv_content: str = "",
    sep: str = ",",
) -> Dict[str, Any]:
    """Loads data into the session for analysis.

    Automatically detects and handles column overflow caused by unquoted
    delimiters in fields. Tries multiple quote/escape configurations to
    find the best parsing strategy.

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

    # Count source lines (minus header) for verification
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
    except Exception:
        pass  # Already installed

    reader = CSVReader(file_path, engine="duckdb")
    _readers[file_path] = reader
    table = reader.db_table

    # CSV parsing configurations to try (in order of preference)
    parse_configs = [
        {"quote": '"', "escape": '"', "name": "double-quote"},
        {"quote": '"', "escape": '\\', "name": "backslash-escape"},
        {"quote": "'", "escape": "'", "name": "single-quote"},
        {"quote": "", "escape": "", "name": "auto-detect"},
    ]

    best_config = None
    best_overflow_count = float('inf')

    for config in parse_configs:
        if not _try_load_csv(file_path, table, sep, config["quote"], config["escape"]):
            continue

        overflow_cols = _check_overflow_columns(table)

        if len(overflow_cols) < best_overflow_count:
            best_overflow_count = len(overflow_cols)
            best_config = config

            # No overflow = perfect, stop searching
            if len(overflow_cols) == 0:
                break

    # If best config isn't already loaded, reload it
    if best_config and best_config != parse_configs[-1]:
        _try_load_csv(file_path, table, sep, best_config["quote"], best_config["escape"])

    # Normalize column names
    _normalize_column_names(table)

    # Remove completely empty rows (100% NULL across all columns)
    columns_list = _get_column_names(table)
    null_conditions = " AND ".join([f'"{col}" IS NULL' for col in columns_list])
    empty_row_count = duckdb.sql(f"""
        SELECT COUNT(*) FROM {table} WHERE {null_conditions}
    """).fetchone()[0]

    if empty_row_count > 0:
        duckdb.sql(f"""
            DELETE FROM {table} WHERE {null_conditions}
        """)

    # Final overflow check after normalization
    final_overflow = _check_overflow_columns(table)

    # Get final stats
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    columns = duckdb.sql(f"DESCRIBE {table}").pl().to_dicts()
    sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    # rows_lost excludes empty rows (those are reported separately)
    rows_lost = source_line_count - total_rows - empty_row_count

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

    if best_config:
        result["parse_config"] = best_config["name"]

    if len(final_overflow) > 0:
        result["warning"] = (
            f"Detected {len(final_overflow)} potential overflow columns: {final_overflow}. "
            "Some rows may have misaligned data due to unquoted delimiters in the source file."
        )
        result["overflow_columns"] = final_overflow

    if rows_lost > 0:
        result["rows_lost_warning"] = (
            f"{rows_lost} rows from the source file were not loaded. "
            "Inspect the raw file for encoding or delimiter issues."
        )

    if empty_row_count > 0:
        result["empty_rows_removed"] = empty_row_count

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
# Batch Tools (for parallel processing optimization)
# ---------------------------------------------------------------------------

def profile_all_columns(tool_context: ToolContext) -> Dict[str, Any]:
    """Analyzes schema and suggests type coercions for ALL columns in one call.

    This is a batch operation that combines get_smart_schema and suggest_type_coercion
    for every column, reducing LLM round-trips from O(N) to O(1).

    Returns:
        Schema info with type suggestions for each column.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    total_rows = reader.row_count_without_header
    columns = _get_column_names(table)

    # Get schema summary
    schema = duckdb.sql(f"""
        SELECT
            column_name,
            column_type,
            approx_unique,
            null_percentage::FLOAT as null_percentage
        FROM (SUMMARIZE SELECT * FROM {table})
    """).pl().to_dicts()

    # Build type coercion suggestions for all columns in one query batch
    coercion_results = []
    for col in columns:
        # Check for Number potential (removing $ and %)
        number_potential = duckdb.sql(f"""
            SELECT COUNT(*) FROM {table}
            WHERE try_cast(regexp_replace("{col}"::VARCHAR, '[\\$\\%\\,]', '', 'g') AS DOUBLE) IS NOT NULL
              AND "{col}" IS NOT NULL
        """).fetchone()[0]

        # Check for Date potential
        date_potential = duckdb.sql(f"""
            SELECT COUNT(*) FROM {table}
            WHERE try_cast("{col}" AS DATE) IS NOT NULL
              OR try_cast(try_strptime("{col}"::VARCHAR, '%m/%d/%Y') AS DATE) IS NOT NULL
        """).fetchone()[0]

        col_total = duckdb.sql(f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NOT NULL').fetchone()[0]

        suggestions = []
        if col_total > 0:
            if number_potential / col_total > 0.9:
                suggestions.append("Number")
            if date_potential / col_total > 0.9:
                suggestions.append("Date")

        if suggestions:
            coercion_results.append({
                "column": col,
                "suggested_types": suggestions,
            })

    return {
        "total_records": total_rows,
        "total_columns": len(columns),
        "schema": schema,
        "type_coercion_suggestions": coercion_results,
    }


def audit_all_columns(tool_context: ToolContext) -> Dict[str, Any]:
    """Detects data quality issues across ALL columns in one call.

    This is a batch operation that combines detect_type_pollution,
    detect_advanced_anomalies, and detect_date_formats for every column,
    reducing LLM round-trips from O(N) to O(1).

    Returns:
        Quality issues found across all columns.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    pollution_issues = []
    outlier_issues = []
    date_format_issues = []

    number_words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']

    for col in columns:
        # --- Type Pollution ---
        pollutants = duckdb.sql(f"""
            SELECT "{col}" as value, COUNT(*) as count
            FROM {table}
            WHERE try_cast("{col}" AS DOUBLE) IS NULL AND "{col}" IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """).pl().to_dicts()

        if pollutants:
            recoverable = [
                v['value'] for v in pollutants
                if isinstance(v['value'], str) and (
                    v['value'].lower().strip() in number_words or
                    any(c in v['value'] for c in '$%')
                )
            ]
            if recoverable or len(pollutants) > 0:
                # Check if this column looks numeric (has some castable values)
                numeric_count = duckdb.sql(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE try_cast("{col}" AS DOUBLE) IS NOT NULL
                """).fetchone()[0]
                if numeric_count > 0:  # Only report if column has numeric values
                    pollution_issues.append({
                        "column": col,
                        "pollutants": pollutants[:3],
                        "recoverable_values": recoverable,
                    })

        # --- Outliers (IQR) ---
        stats = duckdb.sql(f"""
            SELECT
                approx_quantile(try_cast("{col}" AS DOUBLE), 0.25) as q1,
                approx_quantile(try_cast("{col}" AS DOUBLE), 0.75) as q3,
                COUNT(try_cast("{col}" AS DOUBLE)) as numeric_count
            FROM {table}
            WHERE try_cast("{col}" AS DOUBLE) IS NOT NULL
        """).pl().to_dicts()

        if stats and stats[0]["q1"] is not None and stats[0]["q3"] is not None:
            q1, q3 = stats[0]["q1"], stats[0]["q3"]
            iqr = q3 - q1
            if iqr > 0:  # Only check if there's variance
                lower, upper = q1 - (1.5 * iqr), q3 + (1.5 * iqr)
                outlier_count = duckdb.sql(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE try_cast("{col}" AS DOUBLE) > {upper}
                       OR try_cast("{col}" AS DOUBLE) < {lower}
                """).fetchone()[0]
                if outlier_count > 0:
                    outlier_issues.append({
                        "column": col,
                        "iqr_bounds": [round(lower, 2), round(upper, 2)],
                        "outlier_count": outlier_count,
                    })

        # --- Mixed Date Formats ---
        formats = [
            ('%m/%d/%Y', 'MM/DD/YYYY'),
            ('%d/%m/%Y', 'DD/MM/YYYY'),
            ('%Y-%m-%d', 'YYYY-MM-DD'),
            ('%Y/%m/%d', 'YYYY/MM/DD'),
        ]
        found_formats = []
        for fmt, label in formats:
            match_count = duckdb.sql(f"""
                SELECT COUNT(*) FROM {table}
                WHERE try_cast(try_strptime("{col}"::VARCHAR, '{fmt}') AS DATE) IS NOT NULL
            """).fetchone()[0]
            if match_count > 0:
                found_formats.append({"format": label, "count": match_count})

        if len(found_formats) > 1:
            date_format_issues.append({
                "column": col,
                "formats_found": found_formats,
            })

    return {
        "total_rows": total_rows,
        "columns_analyzed": len(columns),
        "type_pollution": pollution_issues,
        "outliers": outlier_issues,
        "mixed_date_formats": date_format_issues,
    }


def analyze_all_patterns(tool_context: ToolContext) -> Dict[str, Any]:
    """Analyzes value distributions and consistency issues across ALL columns.

    This is a batch operation that combines get_value_distribution and
    consistency checks for every column, reducing LLM round-trips from O(N) to O(1).

    Returns:
        Pattern issues (casing inconsistencies, whitespace, etc.) across all columns.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    casing_issues = []
    whitespace_issues = []
    missing_value_patterns = []

    # Get column types to skip non-text columns for string operations
    col_types = {row[0]: row[1] for row in duckdb.sql(f"DESCRIBE {table}").fetchall()}

    for col in columns:
        col_type = col_types.get(col, "").upper()

        # Skip boolean and numeric columns for text-based analysis
        is_text_col = "VARCHAR" in col_type or "TEXT" in col_type or "CHAR" in col_type

        # Get unique count to determine if it's a categorical column
        unique_count = duckdb.sql(f'SELECT COUNT(DISTINCT "{col}") FROM {table}').fetchone()[0]

        # Only analyze text columns with reasonable cardinality (likely categorical)
        if is_text_col and unique_count is not None and 1 < unique_count <= 100:
            # --- Casing Inconsistencies ---
            # Find values that differ only by case
            try:
                casing_check = duckdb.sql(f"""
                    SELECT LOWER(CAST("{col}" AS VARCHAR)) as normalized, COUNT(DISTINCT "{col}") as variants
                    FROM {table}
                    WHERE "{col}" IS NOT NULL
                    GROUP BY LOWER(CAST("{col}" AS VARCHAR))
                    HAVING COUNT(DISTINCT "{col}") > 1
                    LIMIT 5
                """).pl().to_dicts()

                if casing_check:
                    # Get examples of the variants
                    examples = []
                    for item in casing_check[:3]:
                        variants = duckdb.sql(f"""
                            SELECT DISTINCT "{col}" as value FROM {table}
                            WHERE LOWER(CAST("{col}" AS VARCHAR)) = '{item["normalized"].replace("'", "''")}'
                            LIMIT 3
                        """).pl().to_dicts()
                        examples.extend([v["value"] for v in variants])
                    casing_issues.append({
                        "column": col,
                        "inconsistent_groups": len(casing_check),
                        "examples": examples[:5],
                    })
            except Exception:
                pass  # Skip columns that can't be analyzed

        # --- Whitespace Issues (only for text columns) ---
        if is_text_col:
            try:
                whitespace_count = duckdb.sql(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE "{col}" IS NOT NULL
                      AND (
                        CAST("{col}" AS VARCHAR) != TRIM(CAST("{col}" AS VARCHAR))
                        OR CAST("{col}" AS VARCHAR) LIKE '%  %'
                      )
                """).fetchone()[0]

                if whitespace_count and whitespace_count > 0:
                    whitespace_issues.append({
                        "column": col,
                        "affected_rows": whitespace_count,
                    })
            except Exception:
                pass

        # --- Missing Value Patterns (N/A, empty strings, etc.) - only text columns ---
        if is_text_col:
            try:
                missing_patterns = duckdb.sql(f"""
                    SELECT CAST("{col}" AS VARCHAR) as value, COUNT(*) as count
                    FROM {table}
                    WHERE "{col}" IS NOT NULL
                      AND (
                        LOWER(TRIM(CAST("{col}" AS VARCHAR))) IN ('n/a', 'na', 'null', 'none', '-', '--', 'unknown', '')
                        OR CAST("{col}" AS VARCHAR) = ''
                      )
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 5
                """).pl().to_dicts()

                if missing_patterns:
                    missing_value_patterns.append({
                        "column": col,
                        "patterns": missing_patterns,
                    })
            except Exception:
                pass

    return {
        "total_rows": total_rows,
        "columns_analyzed": len(columns),
        "casing_inconsistencies": casing_issues,
        "whitespace_issues": whitespace_issues,
        "missing_value_patterns": missing_value_patterns,
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


def repair_column_overflow(tool_context: ToolContext) -> Dict[str, Any]:
    """Repairs column overflow by reloading the CSV with proper quote/escape handling.

    When a CSV field contains commas (e.g., "Punjab, Haryana"), it should be
    quoted. If the initial load didn't handle quotes properly, data shifts
    into extra columns.

    This tool:
    1. Identifies overflow columns (sparse columns at the end)
    2. Reloads the CSV with proper quote/escape parameters
    3. Compares the new load - if it has fewer columns, use it
    4. Tries multiple parsing strategies until one works

    No parameters needed - automatically detects and repairs overflow.

    Returns:
        Result with before/after comparison and repair statistics.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    csv_path = tool_context.state.get("csv_path")
    if not csv_path:
        return {"error": "No CSV path found in session state."}

    columns = _get_column_names(table)
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    # Step 1: Identify overflow columns (>80% NULL and at the end)
    null_counts = {}
    for col in columns:
        result = duckdb.sql(f'''
            SELECT COUNT(*) - COUNT("{col}") as null_count
            FROM {table}
        ''').fetchone()
        null_counts[col] = result[0]

    sparse_threshold = total_rows * 0.8
    overflow_cols = []
    for col in reversed(columns):
        if null_counts[col] >= sparse_threshold:
            overflow_cols.insert(0, col)
        else:
            break

    if not overflow_cols:
        return {
            "repaired": False,
            "message": "No overflow columns detected (no sparse columns at end of table).",
            "columns": columns,
        }

    original_col_count = len(columns)
    original_overflow_count = len(overflow_cols)

    # Snapshot before
    before_sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    # Step 2: Try reloading with different quote/escape configurations
    parse_configs = [
        # Config 1: Standard CSV with double-quote
        {"quote": '"', "escape": '"', "name": "double-quote escaped"},
        # Config 2: Double-quote with backslash escape
        {"quote": '"', "escape": '\\', "name": "backslash escaped"},
        # Config 3: Single quote
        {"quote": "'", "escape": "'", "name": "single-quote"},
        # Config 4: No quoting, ignore errors
        {"quote": '', "escape": '', "name": "no quotes"},
    ]

    best_result = None
    best_overflow_count = original_overflow_count

    for config in parse_configs:
        try:
            # Load into a test table
            if config["quote"]:
                load_query = f"""
                    CREATE OR REPLACE TABLE {table}_test AS
                    SELECT * FROM read_csv(
                        '{csv_path}',
                        auto_detect = true,
                        quote = '{config["quote"]}',
                        escape = '{config["escape"]}',
                        strict_mode = false,
                        null_padding = true,
                        all_varchar = true
                    )
                """
            else:
                # No quote character - let DuckDB auto-detect
                load_query = f"""
                    CREATE OR REPLACE TABLE {table}_test AS
                    SELECT * FROM read_csv(
                        '{csv_path}',
                        auto_detect = true,
                        strict_mode = false,
                        null_padding = true,
                        all_varchar = true,
                        ignore_errors = true
                    )
                """

            duckdb.sql(load_query)

            # Check the new table's column count and overflow
            test_columns = _get_column_names(f"{table}_test")
            test_rows = duckdb.sql(f"SELECT COUNT(*) FROM {table}_test").fetchone()[0]

            # Count overflow in test table
            test_null_counts = {}
            for col in test_columns:
                result = duckdb.sql(f'''
                    SELECT COUNT(*) - COUNT("{col}") as null_count
                    FROM {table}_test
                ''').fetchone()
                test_null_counts[col] = result[0]

            test_sparse_threshold = test_rows * 0.8
            test_overflow = []
            for col in reversed(test_columns):
                if test_null_counts[col] >= test_sparse_threshold:
                    test_overflow.insert(0, col)
                else:
                    break

            # If this config has fewer overflow columns, it's better
            if len(test_overflow) < best_overflow_count:
                best_overflow_count = len(test_overflow)
                best_result = {
                    "config": config,
                    "columns": test_columns,
                    "overflow": test_overflow,
                    "rows": test_rows,
                }

                # If no overflow, we found the fix
                if len(test_overflow) == 0:
                    break

        except Exception as e:
            # This config didn't work, try next
            continue
        finally:
            # Clean up test table if we're not using it
            if best_result is None or best_result["config"] != config:
                duckdb.sql(f"DROP TABLE IF EXISTS {table}_test")

    # Step 3: Apply the best result
    if best_result and best_overflow_count < original_overflow_count:
        # Normalize column names in the new table
        test_columns = _get_column_names(f"{table}_test")
        norm_parts = []
        for col in test_columns:
            new_name = col.lower()
            new_name = _re.sub(r'[\s\-]+', '_', new_name)
            new_name = _re.sub(r'[^a-z0-9_]', '', new_name)
            new_name = _re.sub(r'_+', '_', new_name)
            new_name = new_name.strip('_')
            if new_name and new_name[0].isdigit():
                new_name = f"col_{new_name}"
            if not new_name:
                new_name = f"column_{test_columns.index(col)}"
            norm_parts.append(f'"{col}" AS "{new_name}"')

        duckdb.sql(f"""
            CREATE OR REPLACE TABLE {table}_normalized AS
            SELECT {', '.join(norm_parts)}
            FROM {table}_test
        """)

        # Replace original table
        duckdb.sql(f"DROP TABLE IF EXISTS {table}")
        duckdb.sql(f"DROP TABLE IF EXISTS {table}_test")
        duckdb.sql(f"ALTER TABLE {table}_normalized RENAME TO {table}")

        after_columns = _get_column_names(table)
        after_sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

        return {
            "repaired": True,
            "method": f"Reloaded CSV with {best_result['config']['name']} parsing",
            "columns_before": original_col_count,
            "columns_after": len(after_columns),
            "overflow_before": original_overflow_count,
            "overflow_after": best_overflow_count,
            "rows": best_result["rows"],
            "new_schema": after_columns,
            "before_sample": _to_markdown(before_sample),
            "after_sample": _to_markdown(after_sample),
        }

    # Cleanup
    duckdb.sql(f"DROP TABLE IF EXISTS {table}_test")

    # No config improved things - just remove overflow columns and flag rows
    first_overflow_idx = columns.index(overflow_cols[0])
    real_columns = columns[:first_overflow_idx]

    overflow_check_expr = " OR ".join([
        f'("{col}" IS NOT NULL AND TRIM(CAST("{col}" AS VARCHAR)) != \'\')'
        for col in overflow_cols
    ])

    shifted_count = duckdb.sql(f"""
        SELECT COUNT(*) FROM {table}
        WHERE {overflow_check_expr}
    """).fetchone()[0]

    real_cols_select = ", ".join([f'"{col}"' for col in real_columns])
    duckdb.sql(f'''
        CREATE OR REPLACE TABLE {table}_cleaned AS
        SELECT
            {real_cols_select},
            CASE WHEN ({overflow_check_expr}) THEN true ELSE false END as is_shifted
        FROM {table}
    ''')

    duckdb.sql(f"DROP TABLE IF EXISTS {table}")
    duckdb.sql(f"ALTER TABLE {table}_cleaned RENAME TO {table}")

    after_columns = _get_column_names(table)
    after_sample = duckdb.sql(f"SELECT * FROM {table} LIMIT 5").pl()

    return {
        "repaired": False,
        "message": "Could not fix overflow by re-parsing. Removed overflow columns and flagged affected rows.",
        "columns_before": original_col_count,
        "columns_after": len(after_columns),
        "rows_flagged": shifted_count,
        "new_schema": after_columns,
        "before_sample": _to_markdown(before_sample),
        "after_sample": _to_markdown(after_sample),
        "note": f"Flagged {shifted_count} rows with is_shifted=true that may have data alignment issues.",
    }


# ---------------------------------------------------------------------------
# Era Detection and Column Normalization
# ---------------------------------------------------------------------------

# Pattern to match years with era designations
_ERA_PATTERN = _re.compile(
    r'^\s*(\d+)\s*(BC|BCE|AD|CE|B\.C\.|B\.C\.E\.|A\.D\.|C\.E\.)\s*$',
    _re.IGNORECASE
)

# Pattern for era at the start (e.g., "AD 2000")
_ERA_PREFIX_PATTERN = _re.compile(
    r'^\s*(BC|BCE|AD|CE|B\.C\.|B\.C\.E\.|A\.D\.|C\.E\.)\s*(\d+)\s*$',
    _re.IGNORECASE
)


def detect_era_in_years(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Detects if a column contains years with era designations (BC, BCE, AD, CE).

    Checks for patterns like:
    - "2000 BC", "500 BCE", "1066 AD", "2024 CE"
    - "BC 2000", "AD 1066" (era prefix)
    - Variations with periods: "B.C.", "A.D.", etc.

    Args:
        column: The column name to check for era patterns.

    Returns:
        Detection results including affected rows and era distribution.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    col_error = _validate_column(column, table)
    if col_error:
        return col_error

    # Get all values from the column
    values = duckdb.sql(f'SELECT "{column}" FROM {table} WHERE "{column}" IS NOT NULL').pl()

    era_rows = []
    era_distribution = {"BC": 0, "BCE": 0, "AD": 0, "CE": 0}

    for row in values.to_dicts():
        val = str(row[column]).strip()

        # Check suffix pattern (e.g., "2000 BC")
        match = _ERA_PATTERN.match(val)
        if match:
            year, era = match.groups()
            normalized_era = era.upper().replace(".", "")
            if normalized_era in ("BC", "BCE"):
                era_distribution["BC"] += 1
            else:
                era_distribution["AD"] += 1
            era_rows.append({"value": val, "year": year, "era": normalized_era})
            continue

        # Check prefix pattern (e.g., "AD 2000")
        match = _ERA_PREFIX_PATTERN.match(val)
        if match:
            era, year = match.groups()
            normalized_era = era.upper().replace(".", "")
            if normalized_era in ("BC", "BCE"):
                era_distribution["BC"] += 1
            else:
                era_distribution["AD"] += 1
            era_rows.append({"value": val, "year": year, "era": normalized_era})

    total_rows = len(values)
    era_count = len(era_rows)

    if era_count == 0:
        return {
            "era_detected": False,
            "column": column,
            "message": "No era designations found in this column.",
        }

    return {
        "era_detected": True,
        "column": column,
        "total_rows": total_rows,
        "rows_with_era": era_count,
        "percentage": round(era_count / total_rows * 100, 1) if total_rows > 0 else 0,
        "era_distribution": {k: v for k, v in era_distribution.items() if v > 0},
        "samples": era_rows[:10],
        "recommendation": f"Extract era into separate 'era' column and keep numeric year in '{column}'",
    }


def extract_era_column(column: str, tool_context: ToolContext) -> Dict[str, Any]:
    """Extracts era designations from a year column into a separate 'era' column.

    Transforms values like "2000 BC" into:
    - Original column: 2000 (numeric year)
    - New 'era' column: "BC" (preserves original text exactly)

    IMPORTANT: Era values are preserved exactly as they appear in the source data.
    If the original says "BC", the era column will say "BC" (not normalized to "BCE").
    This is a cleaning operation, not an enrichment operation.

    Args:
        column: The column containing years with era designations.

    Returns:
        Result with before/after samples showing the transformation.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    col_error = _validate_column(column, table)
    if col_error:
        return col_error

    # Check if era column already exists
    columns = _get_column_names(table)
    era_col_name = "era"
    if era_col_name in columns:
        era_col_name = f"{column}_era"

    # Snapshot before
    before_sample = duckdb.sql(f'SELECT * FROM {table} LIMIT 5').pl()

    # Add the era column
    duckdb.sql(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{era_col_name}" VARCHAR')

    # Extract era exactly as it appears in the data (preserve original text)
    # Handle suffix patterns (e.g., "2000 BC", "1500 B.C.E.")
    duckdb.sql(f'''
        UPDATE {table}
        SET "{era_col_name}" = TRIM(regexp_extract("{column}", '(?i)(BCE?|B\\.C\\.E?\\.?|CE|AD|A\\.D\\.?|C\\.E\\.?)\\s*$', 1))
        WHERE "{column}" IS NOT NULL
          AND regexp_matches("{column}", '(?i)(BCE?|B\\.C\\.E?\\.?|CE|AD|A\\.D\\.?|C\\.E\\.?)\\s*$')
    ''')

    # Handle prefix patterns (e.g., "AD 2000", "B.C. 500")
    duckdb.sql(f'''
        UPDATE {table}
        SET "{era_col_name}" = TRIM(regexp_extract("{column}", '(?i)^\\s*(BCE?|B\\.C\\.E?\\.?|CE|AD|A\\.D\\.?|C\\.E\\.?)\\s+', 1))
        WHERE "{column}" IS NOT NULL
          AND "{era_col_name}" IS NULL
          AND regexp_matches("{column}", '(?i)^\\s*(BCE?|B\\.C\\.E?\\.?|CE|AD|A\\.D\\.?|C\\.E\\.?)\\s+\\d')
    ''')

    # Extract just the numeric year (only for rows where we found an era)
    duckdb.sql(f'''
        UPDATE {table}
        SET "{column}" = regexp_extract("{column}", '(\\d+)', 1)
        WHERE "{era_col_name}" IS NOT NULL AND "{era_col_name}" != ''
    ''')

    # Get stats
    era_counts = duckdb.sql(f'''
        SELECT "{era_col_name}" as era, COUNT(*) as count
        FROM {table}
        WHERE "{era_col_name}" IS NOT NULL
        GROUP BY "{era_col_name}"
    ''').pl()

    after_sample = duckdb.sql(f'SELECT * FROM {table} LIMIT 5').pl()

    rows_updated = duckdb.sql(f'''
        SELECT COUNT(*) FROM {table} WHERE "{era_col_name}" IS NOT NULL
    ''').fetchone()[0]

    return {
        "success": True,
        "column": column,
        "era_column_created": era_col_name,
        "rows_updated": rows_updated,
        "era_distribution": era_counts.to_dicts() if not era_counts.is_empty() else [],
        "before_sample": _to_markdown(before_sample),
        "after_sample": _to_markdown(after_sample),
    }


def normalize_column_names(tool_context: ToolContext) -> Dict[str, Any]:
    """Normalizes all column names to a consistent format.

    Transformations applied:
    - Convert to lowercase
    - Replace spaces with underscores
    - Remove special characters (except underscores)
    - Collapse multiple underscores to single
    - Remove leading/trailing underscores
    - Ensure names don't start with a digit

    Returns:
        Mapping of old names to new names and the updated schema.
    """
    reader = _get_reader(tool_context)
    table = reader.db_table
    _ensure_table(reader)

    columns = _get_column_names(table)
    renames = {}

    for col in columns:
        # Normalize the column name
        new_name = col.lower()
        # Replace spaces and hyphens with underscores
        new_name = _re.sub(r'[\s\-]+', '_', new_name)
        # Remove special characters except underscores
        new_name = _re.sub(r'[^a-z0-9_]', '', new_name)
        # Collapse multiple underscores
        new_name = _re.sub(r'_+', '_', new_name)
        # Remove leading/trailing underscores
        new_name = new_name.strip('_')
        # Ensure doesn't start with digit
        if new_name and new_name[0].isdigit():
            new_name = f"col_{new_name}"
        # Handle empty names
        if not new_name:
            new_name = f"column_{columns.index(col)}"

        if new_name != col:
            renames[col] = new_name

    if not renames:
        return {
            "normalized": False,
            "message": "All column names are already normalized.",
            "columns": columns,
        }

    # Check for conflicts (two columns normalizing to same name)
    new_names = list(renames.values())
    if len(new_names) != len(set(new_names)):
        # Find duplicates and add suffix
        seen = {}
        for old, new in list(renames.items()):
            if new in seen:
                seen[new] += 1
                renames[old] = f"{new}_{seen[new]}"
            else:
                seen[new] = 0

    # Apply renames
    for old_name, new_name in renames.items():
        try:
            duckdb.sql(f'ALTER TABLE {table} RENAME COLUMN "{old_name}" TO "{new_name}"')
        except Exception as e:
            return {
                "error": f"Failed to rename '{old_name}' to '{new_name}': {str(e)}",
                "partial_renames": renames,
            }

    new_columns = _get_column_names(table)

    return {
        "normalized": True,
        "renames": renames,
        "columns_renamed": len(renames),
        "new_schema": new_columns,
    }
