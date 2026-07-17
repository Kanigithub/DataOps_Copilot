from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

import nbformat as nbf


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> str:
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")
    return str(path)


def _extract_code_fences(md: str) -> List[Tuple[str, str]]:
    blocks = []
    fence = re.compile(r"```(\w+)?\n(.*?)\n```", re.DOTALL)
    for m in fence.finditer(md):
        lang = (m.group(1) or "").strip().lower()
        code = m.group(2)
        blocks.append((lang, code))
    return blocks


def write_notebook_from_md(md: str, out_path: Path, title: str) -> str:
    nb = nbf.v4.new_notebook()
    nb.cells.append(nbf.v4.new_markdown_cell(f"# {title}\n\nGenerated at {datetime.utcnow().isoformat()}Z"))

    blocks = _extract_code_fences(md)
    if not blocks:
        nb.cells.append(nbf.v4.new_markdown_cell(md))
    else:
        nb.cells.append(nbf.v4.new_markdown_cell("## Generated code cells"))
        for lang, code in blocks:
            if lang in ("python", "py", "pyspark"):
                nb.cells.append(nbf.v4.new_code_cell(code))
            elif lang == "sql":
                nb.cells.append(nbf.v4.new_code_cell(f"%sql\n{code}"))
            else:
                nb.cells.append(nbf.v4.new_markdown_cell(f"```{lang}\n{code}\n```"))

    ensure_dir(out_path.parent)
    out_path.write_text(json.dumps(nb, indent=2), encoding="utf-8")
    return str(out_path)


def write_sql_files_from_md(md: str, out_dir: Path) -> List[str]:
    ensure_dir(out_dir)
    paths: List[str] = []
    idx = 1
    for lang, code in _extract_code_fences(md):
        if lang == "sql":
            p = out_dir / f"query_{idx:02d}.sql"
            p.write_text(code.strip() + "\n", encoding="utf-8")
            paths.append(str(p))
            idx += 1
    return paths


# ----------------------------
# Databricks Asset Bundles (DAB)
# ----------------------------

def write_dab_databricks_yml(
    out_path: Path,
    bundle_name: str,
    default_target: str = "dev",
) -> str:
    """
    Minimal databricks.yml bundle file.
    Targets assume GitHub Actions passes host/token via env vars and uses --target.
    """
    content = f"""bundle:
  name: {bundle_name}

include:
  - resources/*.yml

targets:
  dev:
    default: true
  prod:
    mode: production
"""
    ensure_dir(out_path.parent)
    out_path.write_text(content, encoding="utf-8")
    return str(out_path)


def write_dab_jobs_yml(
    out_path: Path,
    job_name: str,
    notebook_path_in_repo: str,
    warehouse_id_var: str = "{{{{var.warehouse_id}}}}",
) -> str:
    """
    Defines a Databricks Job referencing a notebook path that will be uploaded by bundle deploy.
    Note: For DAB, notebook paths are repo-relative and deployed as workspace files.
    """
    content = f"""resources:
  jobs:
    {slug(job_name)}:
      name: "{job_name}"
      tasks:
        - task_key: "run_pipeline_notebook"
          notebook_task:
            notebook_path: "{notebook_path_in_repo}"
          # Optional: if using SQL Warehouse for %sql commands in notebooks, configure via cluster or warehouse task.
          # For simplicity we run on a job cluster:
          job_cluster_key: "etl_job_cluster"

      job_clusters:
        - job_cluster_key: "etl_job_cluster"
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 1
"""
    ensure_dir(out_path.parent)
    out_path.write_text(content, encoding="utf-8")
    return str(out_path)


def write_github_actions_bundle_workflow(out_path: Path) -> str:
    """
    GitHub Actions workflow to validate + deploy DAB.
    Requires secrets:
      DATABRICKS_HOST
      DATABRICKS_TOKEN
    """
    content = """name: Databricks Bundle CI/CD

on:
  push:
    branches: [ "main" ]
  pull_request:

jobs:
  bundle:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle validate

      - name: Bundle deploy (dev)
        if: github.event_name == 'push'
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle deploy --target dev
"""
    ensure_dir(out_path.parent)
    out_path.write_text(content, encoding="utf-8")
    return str(out_path)


def slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not s:
        s = "job"
    return s