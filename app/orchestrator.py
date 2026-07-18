from pathlib import Path
from uuid import uuid4
from typing import Dict, Any, List, Tuple, Callable, Optional

from .llm import run_llm
from .memory import summarize
from .artifact_writer import (
    write_text,
    write_notebook_from_md,
    write_sql_files_from_md,
    write_dab_databricks_yml,
    write_dab_jobs_yml,
    write_github_actions_bundle_workflow,
)

AGENT_ORDER = ["refinement","source_discovery","schema_drift","transformation","trust_quality","deployment"]

def load_agent_prompt(agent_name: str) -> str:
    return (Path(__file__).parent / "agents" / f"{agent_name}.agent.md").read_text(encoding="utf-8")

def build_user_prompt(inputs: Dict[str, Any], memory_summary: str, agent_name: str) -> str:
    # Keep prompts SHORT and focused
    return f"""
USER STORY:
{inputs.get("user_story","")}

CONSTRAINTS:
{inputs.get("constraints") or "Not specified"}

KNOWN SOURCES / DATASET PROFILE:
{inputs.get("known_sources") or "Not specified"}

DRIFT INPUTS:
{inputs.get("drift_inputs") or "Not specified"}

CONTEXT SUMMARY (from previous steps):
{memory_summary or "None"}

OUTPUT REQUIREMENTS:
- Databricks + Delta Lake
- Transformations must include Python and SQL fenced blocks.
- Deployment must include Databricks Asset Bundles files and GitHub Actions (bundle validate + deploy dev).

TASK:
Run agent = {agent_name}. Keep output concise and implementation-ready.
""".strip()

def run_pipeline(inputs: Dict[str, Any], progress_cb: Optional[Callable]=None) -> Tuple[str, List[Dict[str,str]], List[str]]:
    run_id = str(uuid4())
    run_dir = Path("generated") / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    outputs: List[Dict[str, str]] = []
    artifacts: List[str] = []
    memory_summary = ""

    for i, agent in enumerate(AGENT_ORDER, start=1):
        if progress_cb:
            progress_cb(status="running", current_agent=agent, outputs=outputs, artifacts=artifacts)

        system_prompt = load_agent_prompt(agent)
        user_prompt = build_user_prompt(inputs, memory_summary, agent)

        content = run_llm(agent=agent, system_prompt=system_prompt, user_prompt=user_prompt)

        outputs.append({"agent": agent, "content": content})
        artifacts.append(write_text(run_dir / f"{i:02d}_{agent}.md", content))

        # Update summary (short context only)
        memory_summary = summarize(content)

        if agent == "transformation":
            tdir = run_dir / "04_transform"
            artifacts.append(write_notebook_from_md(content, tdir / "notebooks" / "pipeline_transform.ipynb", "Pipeline Transform"))
            artifacts.extend(write_sql_files_from_md(content, tdir / "sql"))

        if agent == "deployment":
            ddir = run_dir / "06_deploy"
            artifacts.append(write_dab_databricks_yml(ddir / "databricks.yml", bundle_name=f"ai_etl_{run_id[:8]}"))

            src_nb = run_dir / "04_transform" / "notebooks" / "pipeline_transform.ipynb"
            nb_dest = ddir / "generated_pipeline" / "notebooks" / "pipeline_transform.ipynb"
            nb_dest.parent.mkdir(parents=True, exist_ok=True)
            nb_dest.write_bytes(src_nb.read_bytes())
            artifacts.append(str(nb_dest))

            artifacts.append(write_dab_jobs_yml(
                ddir / "resources" / "jobs.yml",
                job_name=f"AI-ETL Pipeline ({run_id[:8]})",
                notebook_path_in_repo="generated_pipeline/notebooks/pipeline_transform",
            ))

            artifacts.append(write_github_actions_bundle_workflow(
                ddir / ".github" / "workflows" / "databricks-bundle.yml"
            ))

    if progress_cb:
        progress_cb(status="done", current_agent=None, outputs=outputs, artifacts=artifacts)

    return run_id, outputs, artifacts