# DataGrunt AI

AI-powered CSV data cleaning assistant that eliminates the grunt work from data transformations. Upload a messy CSV, get back a clean one — with a detailed report explaining every change.

Built with [Google ADK](https://google.github.io/adk-docs/) (Agent Development Kit) and powered by Gemini.

## What It Does

DataGrunt analyzes your CSV files, identifies data quality issues, and applies SQL-based transformations to clean them — all without deleting a single row.

**Upload a file and DataGrunt will:**

1. **Parse intelligently** — Handles quoted fields, unescaped delimiters, and malformed CSVs with multi-strategy parsing
2. **Detect issues** — Type pollution, inconsistent casing, mixed date formats, statistical outliers, column overflow, era designations in year columns, and missing value patterns
3. **Generate a report** — A single comprehensive analysis with findings organized by severity, proposed cleaning plan, affected row counts, and before/after previews
4. **Clean the data** — Applies transformations via DuckDB SQL with safeguards against data loss
5. **Export results** — Saves a cleaned CSV alongside the original

After the initial analysis, you can ask follow-up questions, modify the cleaning plan, or query the data directly through chat.

## Architecture

DataGrunt uses a multi-agent architecture with specialized workers:

| Agent | Role |
|-------|------|
| **Coordinator** | Orchestrates the workflow, generates the final report, handles follow-up chat |
| **Profiler** | Analyzes column types and suggests type coercions |
| **Auditor** | Detects data quality issues — type pollution, outliers, mixed date formats |
| **PatternExpert** | Finds consistency problems — casing, whitespace, missing value patterns |

Each specialist processes all columns in a single tool call (O(1) LLM round-trips), keeping analysis fast regardless of column count.

### Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS |
| API | FastAPI, Server-Sent Events (SSE) for streaming |
| Data Processing | DuckDB (SQL), Polars (DataFrames) |
| AI | Google Gemini, Google ADK |

## Getting Started

### Prerequisites

- Python 3.13+
- Node.js 18+
- A [Google Gemini API key](https://aistudio.google.com/apikey) or Google Cloud project with Vertex AI enabled

### Backend Setup

```bash
cd clean_csv_agent

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials (see Configuration below)

# Start the server
python -m uvicorn clean_csv_agent.server:app --host 0.0.0.0 --port 8000 --reload
```

### Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start dev server
npm run dev
```

The frontend runs at `http://localhost:3000` and proxies API requests to the backend at port 8000.

## Configuration

Create a `.env` file in the `clean_csv_agent/` directory. Choose one authentication method:

### Option A: Gemini API Key (Simplest)

```env
GOOGLE_API_KEY=your-api-key-here
GOOGLE_GENAI_USE_VERTEXAI=false
```

### Option B: Google Cloud Vertex AI

```env
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_CLOUD_LOCATION=global
GOOGLE_GENAI_USE_VERTEXAI=true
```

### Model Selection

```env
# Default model for all agents
DEFAULT_MODEL=gemini-2.5-flash

# Optional: override per agent
COORDINATOR_MODEL=gemini-2.5-flash
PROFILER_MODEL=gemini-2.5-flash
AUDITOR_MODEL=gemini-2.5-flash
PATTERN_EXPERT_MODEL=gemini-2.5-flash
```

## Data Cleaning Tools

DataGrunt includes 30+ tools for data analysis and transformation:

| Category | Tools |
|----------|-------|
| **Loading** | `load_csv` — Multi-strategy CSV parsing with quote/escape auto-detection |
| **Profiling** | `profile_all_columns` — Schema analysis and type coercion suggestions |
| **Auditing** | `audit_all_columns` — Type pollution, outlier, and date format detection |
| **Patterns** | `analyze_all_patterns` — Casing, whitespace, and missing value analysis |
| **Overflow** | `detect_column_overflow`, `repair_column_overflow` — Fix misaligned columns from malformed delimiters |
| **Era Detection** | `detect_era_in_years`, `extract_era_column` — Split "2000 BC" into year + era columns |
| **Preview** | `preview_full_plan` — Before/after comparison of proposed changes |
| **Execution** | `execute_cleaning_plan` — Apply SQL transformations with rollback on row loss |
| **Validation** | `validate_cleaned_data` — Post-cleaning data integrity checks |
| **Query** | `query_data` — Ad-hoc SQL queries for follow-up questions |

### Safeguards

- **No row deletion** — DELETE, DROP TABLE, and TRUNCATE statements are rejected
- **Row count verification** — Rolls back if any rows are lost during cleaning
- **Value preservation** — Converts number words to digits (e.g., "five" to 5) rather than discarding them

## Project Structure

```
datagrunt-ai/
├── clean_csv_agent/
│   ├── agent.py              # Agent definitions (Coordinator + 3 specialists)
│   ├── prompts.py            # Agent instructions and workflow rules
│   ├── server.py             # FastAPI server (upload, preview, download, streaming)
│   ├── requirements.txt
│   ├── .env.example
│   └── src/
│       ├── tools.py          # 30+ data cleaning tool functions
│       ├── datagrunt.py      # CSV reader and DuckDB query helpers
│       └── duckdb_reference.py  # SQL reference embedded in agent prompts
└── frontend/
    ├── App.tsx               # Main application component
    ├── components/           # UI components (chat, file upload, data table, etc.)
    ├── services/
    │   └── adkService.ts     # API client (file upload, SSE streaming)
    ├── package.json
    └── vite.config.ts
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
