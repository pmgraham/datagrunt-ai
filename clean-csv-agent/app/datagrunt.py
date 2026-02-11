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

import os
import re

import duckdb


class CSVReader:
    """
    A simple wrapper around DuckDB for reading CSV files.
    """
    def __init__(self, filepath: str, engine: str = "duckdb"):
        self.filepath = filepath
        self.engine = engine
        self._db_table = self._generate_table_name(filepath)

    def _generate_table_name(self, filepath: str) -> str:
        """Generates a safe table name from the filename.

        Converts the filename stem to snake_case. Temp files (from uploads
        via the UI) are detected and mapped to 'uploaded_data'.

        Examples:
            'MY FILE.csv'         -> 'my_file'
            '/tmp/tmpXb3kq.csv'   -> 'uploaded_data'
            'Sales-Report 2024.csv' -> 'sales_report_2024'
        """
        stem = os.path.splitext(os.path.basename(filepath))[0]
        # Temp files from uploads have no meaningful name
        if stem.startswith("tmp") and len(stem) <= 12:
            return "uploaded_data"
        name = re.sub(r'[^\w]+', '_', stem)
        name = re.sub(r'_+', '_', name).strip('_').lower()
        return name or "uploaded_data"

    @property
    def db_table(self) -> str:
        return self._db_table

    @property
    def row_count_without_header(self) -> int:
        """Counts rows in the CSV, excluding the header."""
        try:
            query = f"SELECT COUNT(*) FROM read_csv('{self.filepath}', auto_detect=true, header=true)"
            return duckdb.sql(query).fetchone()[0]
        except Exception:
            # Fallback to python line counting if duckdb fails
            with open(self.filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for _ in f) - 1


class DuckDBQueries:
    """
    Helper for generating DuckDB queries.
    """
    def __init__(self, filepath: str):
        self.filepath = filepath

    def import_csv_query_normalize_columns(self) -> str:
        """
        Generates a SQL query to import the CSV into a table with normalized column names.
        """
        reader = CSVReader(self.filepath)
        table_name = reader.db_table

        return f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv(
                '{self.filepath}',
                auto_detect=true,
                normalize_names=true,
                quote='"',
                escape='"',
                ignore_errors=true
            );
        """
