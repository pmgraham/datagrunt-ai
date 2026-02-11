# ruff: noqa
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

from google.adk.agents import Agent
from google.adk.apps import App
from google.adk.models import Gemini
from google.genai import types

import google.auth

from app.callbacks import intercept_file_upload
from app.prompts import COORDINATOR_PROMPT
from app import tools

_, project_id = google.auth.default()
os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
os.environ["GOOGLE_CLOUD_LOCATION"] = "global"
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "True"


# ---------------------------------------------------------------------------
# Model Configuration
# ---------------------------------------------------------------------------

DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "gemini-3-flash-preview")


def _gemini_model(env_var: str) -> Gemini:
    """Create a Gemini model wrapper using env var override or default."""
    return Gemini(
        model=os.getenv(env_var, DEFAULT_MODEL),
        retry_options=types.HttpRetryOptions(attempts=3),
    )


# ---------------------------------------------------------------------------
# Coordinator Agent
# ---------------------------------------------------------------------------

root_agent = Agent(
    name="DataGruntScientist",
    description=(
        "CSV cleaning assistant. Loads CSV files, profiles columns, audits "
        "data quality, detects structural issues (column overflow, era "
        "designations), then proposes and executes cleaning plans."
    ),
    model=_gemini_model("COORDINATOR_MODEL"),
    instruction=COORDINATOR_PROMPT,
    before_model_callback=intercept_file_upload,
    tools=[
        tools.load_csv,
        tools.fix_unknown_values,
        tools.inspect_raw_file,
        tools.profile_all_columns,
        tools.audit_all_columns,
        tools.analyze_all_patterns,
        tools.detect_era_in_years,
        tools.extract_era_column,
        tools.preview_full_plan,
        tools.execute_cleaning_plan,
        tools.validate_cleaned_data,
        tools.query_data,
    ],
)


app = App(
    root_agent=root_agent,
    name="app",
)
