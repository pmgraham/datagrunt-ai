import os
import duckdb
import re

class CSVReader:
    """
    A simple wrapper around DuckDB for reading CSV files.
    Replaces the missing datagrunt.CSVReader class.
    """
    def __init__(self, filepath: str, engine: str = "duckdb"):
        self.filepath = filepath
        self.engine = engine
        self._db_table = self._generate_table_name(filepath)

    def _generate_table_name(self, filepath: str) -> str:
        """Generates a safe table name from the filename."""
        base = os.path.basename(filepath)
        name, _ = os.path.splitext(base)
        # Sanitize: replace non-alphanumeric with defaults, keep it simple
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
        return f"table_{safe_name}"

    @property
    def db_table(self) -> str:
        return self._db_table

    @property
    def row_count_without_header(self) -> int:
        """Counts rows in the CSV, excluding the header."""
        try:
            # Efficiently count lines using duckdb's CSV sniffer or read_csv
            # Using count(*) on read_csv is usually fast enough for moderate files
            query = f"SELECT COUNT(*) FROM read_csv('{self.filepath}', auto_detect=true, header=true)"
            return duckdb.sql(query).fetchone()[0]
        except Exception:
            # Fallback to python line counting if duckdb fails
            with open(self.filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for _ in f) - 1

class DuckDBQueries:
    """
    Helper for generating DuckDB queries.
    Replaces the missing datagrunt.core.databases.databases.DuckDBQueries class.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath

    def import_csv_query_normalize_columns(self) -> str:
        """
        Generates a SQL query to import the CSV into a table with normalized column names.
        """
        # We need a robust way to get the table name, similar to CSVReader
        # In the original code, it seems they might be loosely coupled, but here
        # we can reconstruct the table name logic or just use a standard one.
        # However, looking at tools.py, this is called when `_ensure_table` fails to find the table.
        # So this should CREATE the table.
        
        reader = CSVReader(self.filepath)
        table_name = reader.db_table
        
        return f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv('{self.filepath}', auto_detect=true, normalize_names=true);
        """
