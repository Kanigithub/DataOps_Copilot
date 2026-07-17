# FlowCraft AI - Intelligent ETL/ELT Pipeline Builder

**FlowCraft AI** is an AI-powered ETL/ELT pipeline builder that automatically generates production-ready data pipelines for Databricks and Delta Lake. Using a multi-agent orchestration system, it transforms business requirements into fully-configured, deployable infrastructure with Databricks Asset Bundles (DAB) and GitHub Actions CI/CD workflows.

---

## 🎯 Overview

FlowCraft AI solves the complex problem of manually designing and coding ETL/ELT pipelines by leveraging AI agents to:

- **Discover** data sources and profile datasets
- **Detect** schema drift and data quality issues
- **Transform** raw data into Delta Lake tables using Python and SQL
- **Ensure** trust and quality through data validation
- **Deploy** to Databricks with Infrastructure-as-Code (DAB + GitHub Actions)
- **Refine** and optimize based on feedback

The system is designed for **data engineers, architects, and platform teams** who want to rapidly prototype and deploy data pipelines with minimal manual coding.

---

## 🏗️ Architecture

### Stack
- **Language(s):** Python 3.x
- **Framework / Runtime:** FastAPI 0.115.0 + Uvicorn + Jinja2 templating
- **LLM Integration:** Azure OpenAI (DIAL API proxy) with retry logic and exponential backoff
- **Data Processing:** Pandas 2.2.2, Jupyter Notebook format (nbformat 5.10.4)
- **Configuration:** Python-dotenv, Pydantic 2.8.2 for schema validation
- **Deployment:** Databricks Asset Bundles, GitHub Actions workflows

### Project Structure

```
FlowCraft_AI_Codieme/
├── app/                              # Main application package
│   ├── main.py                       # FastAPI server, HTTP endpoints
│   ├── orchestrator.py               # Full pipeline orchestration (6 agents)
│   ├── orchestrator_phased.py        # Two-phase orchestration (Phase 1/Phase 2)
│   ├── llm.py                        # Azure OpenAI client with retry logic
│   ├── memory.py                     # Context summarization between agents
│   ├── artifact_writer.py            # Markdown, Jupyter, SQL, DAB/GitHub Actions generation
│   ├── job_store.py                  # In-memory job status tracking
│   ├── schemas.py                    # Pydantic models for request/response validation
│   │
│   ├── agents/                       # AI agent prompt files (system instructions)
│   │   ├── source_discovery.agent.md         # Discovers data sources & profiles
│   │   ├── schema_drift.agent.md              # Detects schema changes
│   │   ├── transformation.agent.md            # Generates Python/SQL transforms
│   │   ├── trust_quality.agent.md             # Data quality validation rules
│   │   ├── deployment.agent.md                # DAB/GitHub Actions config
│   │   └── refinement.agent.md                # Optimization feedback
│   │
│   ├── templates/                   # Jinja2 HTML templates
│   │   └── index.html                # Single-page app (SPA)
│   │
│   └── static/                       # Frontend assets (CSS, JS)
│       ├── css/                      # Styling
│       └── js/                       # Client-side interactivity
│
├── requirements.txt                  # Python dependencies
├── .env                              # Configuration (API keys, endpoints)
└── README.md                         # This file
```

### Data Flow

1. **User Input → FastAPI Server**
   - User provides business story, constraints, data sources via web UI
   - Files (schemas, data samples) are uploaded and stored in `uploads/{run_id}/`

2. **Agent Orchestration**
   - `orchestrator.py` or `orchestrator_phased.py` runs agents sequentially
   - Each agent receives:
     - System prompt (agent instructions from `.agent.md`)
     - User prompt (business requirements + context summary)
   - Agent outputs are saved as Markdown artifacts

3. **Context Memory**
   - `memory.py` summarizes agent outputs to ≤10 bullet points
   - Summary is passed to next agent, maintaining context across pipeline

4. **Artifact Generation**
   - `artifact_writer.py` extracts code fences from agent output
   - Generates:
     - Jupyter Notebooks (`.ipynb`) with Python/SQL cells
     - SQL query files (`.sql`)
     - Databricks Asset Bundle files (`databricks.yml`, `resources/jobs.yml`)
     - GitHub Actions workflow (`databricks-bundle.yml`)

5. **Job Status Tracking**
   - `job_store.py` maintains in-memory state (queued → running → done/error)
   - Web UI polls `/api/status/{run_id}` for real-time updates

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Azure OpenAI API key (or DIAL proxy endpoint)
- Optional: Databricks workspace + token (for deployment)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Kanigithub/FlowCraft_AI_Codieme.git
   cd FlowCraft_AI_Codieme
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys:
   # - DIAL_API_KEY (Azure OpenAI via DIAL proxy)
   # - DIAL_ENDPOINT (usually https://ai-proxy.lab.epam.com)
   # - DIAL_DEPLOYMENT (model name, e.g., gpt-5-mini-2025-08-07)
   ```

### Running the Server

```bash
# Development (hot-reload)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

Visit `http://localhost:8000` in your browser.

### Quick Test

```bash
# Health check
curl http://localhost:8000/health
# Response: {"status": "ok"}

# Ping LLM (verify API connection)
curl http://localhost:8000/api/dial_ping
# Response: {"reply": "pong"}
```

---

## 📋 API Endpoints

### Pages
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Home page (SPA) |
| `GET` | `/health` | Health check |
| `GET` | `/favicon.ico` | Favicon (204 No Content) |

### Pipeline Execution
| Method | Endpoint | Body | Description |
|--------|----------|------|-------------|
| `POST` | `/api/run_async` | `RunRequest` | Start full 6-agent pipeline (async, background task) |
| `POST` | `/api/run_phase1` | `dict` | Run Phase 1 agents (source_discovery, schema_drift, transformation) |
| `POST` | `/api/run_phase2/{run_id}` | `RunRequest` | Run Phase 2 agents (trust_quality, deployment, refinement) |

### Status & Artifacts
| Method | Endpoint | Parameters | Description |
|--------|----------|-----------|-------------|
| `GET` | `/api/status/{run_id}` | `run_id` | Get job status, outputs, artifacts |
| `POST` | `/api/inputs/{run_id}` | `user_story`, `files` | Save business requirements + upload data files |
| `GET` | `/api/dial_ping` | — | Test LLM connectivity |

### Request Schema (`RunRequest`)
```python
{
  "user_story": str,           # Business requirements (optional)
  "platform": str,             # Default: "Databricks"
  "constraints": str,          # Technical constraints
  "known_sources": str,        # Source system descriptions
  "drift_inputs": str,         # Schema drift considerations
  "target_style": str,         # Default: "databricks_notebooks_and_sql"
  "pipeline_name": str,        # Display name
  "comments": str              # Additional notes
}
```

### Response (`/api/run_async`)
```json
{
  "run_id": "abc123def456...",
  "status": "done" | "running" | "error",
  "outputs": [
    {"agent": "source_discovery", "content": "..."},
    {"agent": "schema_drift", "content": "..."},
    ...
  ],
  "artifacts_written": [
    "/generated/runs/abc123/01_source_discovery.md",
    "/generated/runs/abc123/04_transform/notebooks/pipeline_transform.ipynb",
    "/generated/runs/abc123/06_deploy/databricks.yml",
    ...
  ]
}
```

---

## 🤖 AI Agents

Each agent is a specialized LLM task with a dedicated prompt file. Agents run sequentially, with outputs from previous agents informing subsequent decisions.

### Agent Order (Full Pipeline)
1. **Source Discovery** (`source_discovery.agent.md`)
   - Identifies data sources, lineage, table structures
   - Output: source inventory, table schemas

2. **Schema Drift** (`schema_drift.agent.md`)
   - Detects breaking changes, evolution patterns
   - Output: drift rules, migration strategies

3. **Transformation** (`transformation.agent.md`)
   - Generates Python/SQL code for ETL logic
   - Output: transformation logic, code blocks (fenced in Markdown)

4. **Trust & Quality** (`trust_quality.agent.md`)
   - Defines data quality checks, validation rules
   - Output: DQ checks, anomaly detection

5. **Deployment** (`deployment.agent.md`)
   - Generates DAB config, GitHub Actions workflow
   - Output: `databricks.yml`, `jobs.yml`, GitHub workflow

6. **Refinement** (`refinement.agent.md`)
   - Optimization suggestions, feedback incorporation
   - Output: refinement notes, next steps

### Two-Phase Mode
Allows human approval between phases:
- **Phase 1:** source_discovery → schema_drift → transformation
- **Phase 2:** trust_quality → deployment → refinement

---

## 📦 Generated Artifacts

FlowCraft AI outputs production-ready configurations in the `generated/runs/{run_id}/` directory:

```
generated/runs/{run_id}/
├── 01_source_discovery.md                 # Agent output
├── 02_schema_drift.md
├── 03_transformation.md
├── 04_trust_quality.md
├── 05_deployment.md
├── 06_refinement.md
│
├── 04_transform/
│   ├── notebooks/
│   │   └── pipeline_transform.ipynb       # Jupyter notebook with Python/SQL cells
│   └── sql/
│       ├── query_01.sql                   # Extracted SQL files
│       ├── query_02.sql
│       └── ...
│
└── 06_deploy/
    ├── databricks.yml                     # Databricks Asset Bundle config
    ├── resources/
    │   └── jobs.yml                       # Job definitions
    ├── generated_pipeline/
    │   └── notebooks/
    │       └── pipeline_transform.ipynb   # Notebook copy for deployment
    └── .github/workflows/
        └── databricks-bundle.yml          # GitHub Actions CI/CD
```

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```dotenv
# Azure OpenAI (via DIAL proxy)
DIAL_API_KEY=sk-xxx...
DIAL_ENDPOINT=https://ai-proxy.lab.epam.com
DIAL_API_VERSION=2025-04-01-preview
DIAL_DEPLOYMENT=gpt-5-mini-2025-08-07

# Optional: Direct OpenAI
# OPENAI_API_KEY=sk-xxx...
# OPENAI_MODEL=gpt-4o-mini

# Optional: Ollama (local LLM)
# OLLAMA_URL=http://127.0.0.1:11434
# OLLAMA_MODEL=llama3.1:8b

# Optional: Atlassian Jira integration
# ATLASSIAN_BASE_URL=https://your-domain.atlassian.net
# ATLASSIAN_EMAIL=user@example.com
# ATLASSIAN_API_TOKEN=xxx...
```

### Supported File Types for Upload
- **Data:** `.xlsx`, `.xls`, `.csv`, `.json`
- **Documents:** `.pdf`, `.doc`, `.docx`, `.txt`, `.md`
- **Config:** `.yml`, `.yaml`, `.sql`

---

## 🛠️ Development

### Project Structure Notes
- **Commented Code:** `app/main.py` contains extensive commented-out code from the original implementation. Core functionality is in lines 207–349.
- **LLM Retry Logic:** `app/llm.py` implements exponential backoff (2, 4, 6 seconds) and 3 retry attempts for transient failures.
- **Job Store:** In-memory store (`job_store.py`) with thread-safe locking. Consider replacing with Redis for multi-worker deployments.
- **Template Caching:** `main.py` disables Jinja2 template caching (`cache_size=0`) to avoid corruption during development.

### Adding a New Agent
1. Create `app/agents/my_agent.agent.md` with system instructions
2. Add agent to `AGENT_ORDER` in `orchestrator.py` or `PHASE1`/`PHASE2` in `orchestrator_phased.py`
3. Define schema updates in `schemas.py` if needed
4. Update prompt building in `orchestrator.py` or `orchestrator_phased.py`

### Modifying Artifact Generation
Edit `app/artifact_writer.py` to add new output formats:
- `write_*()` functions handle file generation
- Use `_extract_code_fences()` to parse Markdown code blocks
- Extend with `write_terraform()`, `write_dbt()`, etc. as needed

---

## 🐛 Troubleshooting

### "Missing DIAL_API_KEY" Error
- **Cause:** Environment variable not set
- **Fix:** Add `DIAL_API_KEY` to `.env` and restart the server

### LLM Timeouts
- **Cause:** Agent taking >120 seconds
- **Fix:** Increase `timeout` in `app/llm.py` line 14, or reduce prompt complexity

### Job Status Returns 404
- **Cause:** `run_id` not found in memory store
- **Fix:** Job store is in-memory; restart loses all jobs. Use Redis for persistence.

### File Upload Fails
- **Cause:** Unsupported file type
- **Fix:** Check `ALLOWED_EXTS` in `main.py` line 227

---

## 📚 Key Technologies

| Component | Version | Purpose |
|-----------|---------|---------|
| **FastAPI** | 0.115.0 | Web framework, async request handling |
| **Uvicorn** | 0.30.6 | ASGI server |
| **Pydantic** | 2.8.2 | Data validation, schema enforcement |
| **Pandas** | 2.2.2 | Data profiling, analysis |
| **nbformat** | 5.10.4 | Jupyter Notebook generation |
| **OpenAI SDK** | 1.40.6 | Azure OpenAI integration |
| **Jinja2** | 3.1.4 | HTML templating |
| **python-dotenv** | 1.0.1 | Environment config management |
| **PyYAML** | 6.0.2 | YAML parsing (DAB configs) |

---

## 🎨 Frontend

The web UI (`app/templates/index.html`) is a single-page application (SPA) with:
- **Sidebar Navigation:** Dashboard, Pipelines, Runs, Artifacts, Agents, Approvals, Data Sources, Connections, Settings
- **Main Panel:** Pipeline builder with real-time agent journey visualization
- **KPI Cards:** Pipelines, Runs, Success Rate, Artifacts, Approvals, Active Agents
- **Input Section:** User story textarea + file upload
- **Human Approval:** Phase 1 output review before Phase 2 execution
- **Real-time Artifact Viewer:** Live log display with artifact preview modal
- **Pipeline Summary:** Overall progress, run info, recent runs

**Note:** CSS and JavaScript files are placeholders (empty directories). Styling is inline CSS in `index.html`.

---

## 📄 License

This project is part of the FlowCraft AI initiative. Check LICENSE file for details.

---

## 👥 Contributing

Contributions welcome! Areas for enhancement:
- Multi-worker job store (Redis integration)
- Additional LLM backends (Anthropic, Ollama, etc.)
- More artifact generators (dbt, Terraform, Airflow DAGs)
- Improved error handling and validation
- Unit and integration tests

---

## 📞 Support

For issues, feature requests, or questions, please open a GitHub issue in this repository.

---

**Last Updated:** 2026-07-17  
**Author:** Kanigithub  
**Status:** Active Development
