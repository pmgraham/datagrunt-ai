import os

from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.tools.agent_tool import AgentTool
from google.adk.tools.function_tool import FunctionTool

from clean_csv_agent.prompts import (
    AUDITOR_PROMPT,
    COORDINATOR_PROMPT,
    PATTERN_PROMPT,
    PROFILER_PROMPT,
)
from clean_csv_agent.src import tools

load_dotenv()

# ---------------------------------------------------------------------------
# Model Configuration
# ---------------------------------------------------------------------------

DEFAULT_MODEL_FALLBACK = "gemini-3-flash-preview"
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", DEFAULT_MODEL_FALLBACK)

# ---------------------------------------------------------------------------
# Specialized Agents (The "Workers") - Return raw findings
# ---------------------------------------------------------------------------

profiler_agent = Agent(
    name="Profiler",
    description=(
        "Analyzes CSV structure and schema. Returns column types, statistics, "
        "and type coercion recommendations for each column."
    ),
    model=os.getenv("PROFILER_MODEL", DEFAULT_MODEL),
    instruction=PROFILER_PROMPT,
    tools=[
        FunctionTool(func=tools.get_smart_schema),
        FunctionTool(func=tools.suggest_type_coercion),
    ],
)

auditor_agent = Agent(
    name="Auditor",
    description=(
        "Audits data quality issues. Detects type pollution (text in numeric columns), "
        "statistical outliers via IQR, and mixed date formats."
    ),
    model=os.getenv("AUDITOR_MODEL", DEFAULT_MODEL),
    instruction=AUDITOR_PROMPT,
    tools=[
        FunctionTool(func=tools.detect_type_pollution),
        FunctionTool(func=tools.detect_advanced_anomalies),
        FunctionTool(func=tools.detect_date_formats),
    ],
)

pattern_agent = Agent(
    name="PatternExpert",
    description=(
        "Identifies consistency issues and patterns. Analyzes value distributions, "
        "checks logical relationships between columns, and finds whitespace issues."
    ),
    model=os.getenv("PATTERN_EXPERT_MODEL", DEFAULT_MODEL),
    instruction=PATTERN_PROMPT,
    tools=[
        FunctionTool(func=tools.get_value_distribution),
        FunctionTool(func=tools.check_column_logic),
        FunctionTool(func=tools.query_data),
    ],
)

# ---------------------------------------------------------------------------
# Coordinator Agent (The "Interface")
# ---------------------------------------------------------------------------

root_agent = Agent(
    name="DataGruntScientist",
    description=(
        "CSV cleaning assistant. Loads CSV files, detects structural issues "
        "(column overflow, era designations), coordinates specialized agents "
        "for profiling and auditing, then proposes and executes cleaning plans."
    ),
    model=os.getenv("COORDINATOR_MODEL", DEFAULT_MODEL),
    instruction=COORDINATOR_PROMPT,
    tools=[
        AgentTool(agent=profiler_agent),
        AgentTool(agent=auditor_agent),
        AgentTool(agent=pattern_agent),
        FunctionTool(func=tools.load_csv),
        FunctionTool(func=tools.inspect_raw_file),
        FunctionTool(func=tools.detect_column_overflow),
        FunctionTool(func=tools.repair_column_overflow),
        FunctionTool(func=tools.detect_era_in_years),
        FunctionTool(func=tools.extract_era_column),
        FunctionTool(func=tools.normalize_column_names),
        FunctionTool(func=tools.preview_full_plan),
        FunctionTool(func=tools.execute_cleaning_plan),
        FunctionTool(func=tools.validate_cleaned_data),
        FunctionTool(func=tools.query_data),
    ],
)
