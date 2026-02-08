import os

from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.tools.function_tool import FunctionTool
from google.adk.tools.agent_tool import AgentTool

from datagrunt_adk.src import tools
from datagrunt_adk.src.duckdb_reference import DUCKDB_SQL_REFERENCE

load_dotenv()

# ---------------------------------------------------------------------------
# Specialized Agents (The "Workers") - Return raw findings
# ---------------------------------------------------------------------------

PROFILER_PROMPT = """
You are a Profiler. Return raw data about the spreadsheet structure.
1. Run 'get_smart_schema'.
2. Run 'suggest_type_coercion' for all columns.
Return only the raw results of these tools.
"""

AUDITOR_PROMPT = """
You are a Quality Auditor. Return raw data about data issues.
1. Run 'detect_type_pollution' for all columns.
2. Run 'detect_advanced_anomalies' for numeric columns.
3. Run 'detect_date_formats' for date-looking columns.
Focus on identifying values like "five" or "$100" for recovery.
Return only the raw findings.
"""

PATTERN_PROMPT = """
You are a Consistency Specialist. Return raw data about patterns.
1. Run 'get_value_distribution' for text columns.
2. Run 'check_column_logic' for related columns.
3. Run 'query_data' for whitespace and missing labels ('N/A').
Return only the raw findings.
"""

profiler_agent = Agent(
    name="Profiler",
    model="gemini-2.5-flash",
    instruction=PROFILER_PROMPT,
    tools=[
        FunctionTool(func=tools.get_smart_schema),
        FunctionTool(func=tools.suggest_type_coercion),
    ],
)

auditor_agent = Agent(
    name="Auditor",
    model="gemini-2.5-flash",
    instruction=AUDITOR_PROMPT,
    tools=[
        FunctionTool(func=tools.detect_type_pollution),
        FunctionTool(func=tools.detect_advanced_anomalies),
        FunctionTool(func=tools.detect_date_formats),
    ],
)

pattern_agent = Agent(
    name="PatternExpert",
    model="gemini-2.5-flash",
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



COORDINATOR_PROMPT = f"""You are a friendly Data Assistant.

## ABSOLUTELY CRITICAL — READ THIS FIRST:
- You MUST stay COMPLETELY SILENT until your entire analysis is finished.
- Do NOT send ANY text to the user until you have completed ALL tool calls.
- Do NOT ask the user to confirm anything mid-analysis.
- Do NOT provide progress updates, section-by-section findings, or partial results.
- Your FIRST and ONLY message to the user is the final Unified Report below.
- If you send more than one message, you have failed.

## WORKFLOW (all silent — no user messages until step 5):
1. Run 'load_csv'. Use ONLY the exact column names it returns.
2. Call 'Profiler', 'Auditor', and 'PatternExpert' in parallel.
3. Collect ALL findings from all three agents.
4. Build a single list of SQL fix statements and run 'preview_full_plan' ONCE.
5. ONLY NOW send your first message: the Unified Report below.

## UNIFIED REPORT FORMAT (your one and only message):

### Data Health Summary
One markdown table covering ALL findings across all categories:
| Category | Column | What I Found | Proposed Fix |
| :--- | :--- | :--- | :--- |
| Mixed Content | quantity | 'five' instead of 5 | Convert word to number |
| Consistency | region | Mixed casing | Standardize to Title Case |

### Preview of Changes
Show the "Before" and "After" from 'preview_full_plan'.

### Next Steps
End with: "Want me to apply all of these changes?"

## AFTER USER APPROVES:
When the user confirms, run 'execute_cleaning_plan' with all the SQL statements.
Then tell the user:
- How many rows were cleaned
- The file path to the cleaned CSV (from the 'cleaned_file' field in the result)
- Example: "Your cleaned data is saved at: `/path/to/cleaned.csv`"

## RULES:
- ONE message. ONE summary table. ONE preview. ONE confirmation question.
- NEVER use DELETE, DROP TABLE, or TRUNCATE. Row count must stay the same.
- To fix bad values, use UPDATE (set to NULL or convert). To handle duplicates, add a flag column.
- Never discard data. Convert values like 'five' to 5 using CASE statements.
- Use friendly language, no jargon.
- After cleaning, ALWAYS provide the saved file path.

{DUCKDB_SQL_REFERENCE}
"""



root_agent = Agent(

    name="DataGruntScientist",

    model="gemini-2.5-flash",

    instruction=COORDINATOR_PROMPT,

    tools=[

        AgentTool(agent=profiler_agent),

        AgentTool(agent=auditor_agent),

        AgentTool(agent=pattern_agent),

        FunctionTool(func=tools.load_csv),

        FunctionTool(func=tools.inspect_raw_file),

        FunctionTool(func=tools.preview_full_plan),

        FunctionTool(func=tools.execute_cleaning_plan),

        FunctionTool(func=tools.validate_cleaned_data),

    ],

)
