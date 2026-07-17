# from __future__ import annotations

# import os
# import shutil
# from pathlib import Path
# from typing import Any, Dict, List, Optional
# from uuid import uuid4

# from fastapi import BackgroundTasks, FastAPI, File, Form, Request, UploadFile
# from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates
# from jinja2 import Environment, FileSystemLoader, select_autoescape

# from .job_store import create, get, update
# from .orchestrator import run_pipeline
# from .orchestrator_phased import PHASE1, PHASE2, run_agents

# BASE_DIR = Path(__file__).resolve().parent

# app = FastAPI(title="FlowCraft AI - ETL/ELT Builder")

# # ---- Templates: disable cache to avoid your previous Jinja cache corruption
# jinja_env = Environment(
#     loader=FileSystemLoader(str(BASE_DIR / "templates")),
#     autoescape=select_autoescape(["html", "xml"]),
#     cache_size=0,
# )
# templates = Jinja2Templates(env=jinja_env)

# # ---- Static (CSS/JS)
# STATIC_DIR = BASE_DIR / "static"
# STATIC_DIR.mkdir(parents=True, exist_ok=True)
# app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# # ---- Uploads (user story + supporting docs)
# UPLOADS_DIR = BASE_DIR / "uploads"
# UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

# ALLOWED_EXTS = {
#     ".xlsx", ".xls", ".csv", ".json", ".pdf",
#     ".doc", ".docx", ".yml", ".yaml", ".sql", ".txt", ".md",
# }

# # ---- Generated artifacts root (adjust if your project uses another folder)
# GENERATED_ROOT = (BASE_DIR.parent / "generated").resolve()


# def _safe_filename(name: str) -> str:
#     name = os.path.basename((name or "").strip())
#     return name or "uploaded_file"


# def load_user_inputs(run_id: str) -> tuple[str, List[str]]:
#     run_dir = UPLOADS_DIR / run_id
#     story_path = run_dir / "story.txt"
#     files_dir = run_dir / "files"
#     user_story = story_path.read_text(encoding="utf-8") if story_path.exists() else ""
#     uploaded_files: List[str] = []
#     if files_dir.exists():
#         for fn in sorted(os.listdir(files_dir)):
#             uploaded_files.append(str(files_dir / fn))
#     return user_story, uploaded_files


# def _build_inputs(payload: Dict[str, Any], run_id: str) -> Dict[str, Any]:
#     story, uploaded_files = load_user_inputs(run_id)
#     inp = dict(payload or {})
#     if not inp.get("user_story"):
#         inp["user_story"] = (
#             story
#             or f"Build ETL pipeline for {inp.get('pipeline_name', 'the selected pipeline')}. "
#                f"{inp.get('comments', '')}".strip()
#         )
#     inp["uploaded_files"] = uploaded_files
#     return inp


# # -------------------- Pages --------------------
# @app.get("/", response_class=HTMLResponse)
# def home(request: Request):
#     print("request index html value::", request)
#     # CRITICAL: template name must be string, context must be dict
#     return templates.TemplateResponse("index.html", {"request": request})


# @app.get("/favicon.ico")
# def favicon():
#     return Response(status_code=204)


# @app.get("/health")
# def health():
#     return {"status": "ok"}


# # -------------------- Status --------------------
# @app.get("/api/status/{run_id}")
# def status(run_id: str):
#     job = get(run_id)
#     if not job:
#         return JSONResponse(status_code=404, content={"error": "not_found"})

#     user_story, uploaded_files = load_user_inputs(run_id)
#     return {
#         "run_id": job.run_id,
#         "status": job.status,
#         "current_agent": job.current_agent,
#         "outputs": job.outputs,
#         "artifacts_written": job.artifacts,
#         "error": job.error,
#         "user_story": user_story,
#         "uploaded_files": uploaded_files,
#     }


# # -------------------- Upload Inputs --------------------
# @app.post("/api/inputs/{run_id}")
# async def save_user_inputs(
#     run_id: str,
#     user_story: str = Form(""),
#     files: Optional[List[UploadFile]] = File(None),
# ):
#     run_dir = UPLOADS_DIR / run_id
#     files_dir = run_dir / "files"
#     run_dir.mkdir(parents=True, exist_ok=True)
#     files_dir.mkdir(parents=True, exist_ok=True)

#     (run_dir / "story.txt").write_text(user_story or "", encoding="utf-8")

#     saved: List[str] = []
#     if files:
#         for f in files:
#             fname = _safe_filename(f.filename)
#             ext = os.path.splitext(fname)[1].lower()
#             if ext and ext not in ALLOWED_EXTS:
#                 return JSONResponse(status_code=400, content={"ok": False, "error": f"File type not allowed: {fname}"})
#             dest = files_dir / fname
#             with dest.open("wb") as out:
#                 shutil.copyfileobj(f.file, out)
#             saved.append(str(dest))

#     return {"ok": True, "run_id": run_id, "saved_files": saved}


# # -------------------- Artifact preview/download --------------------
# @app.get("/api/artifact/preview")
# def artifact_preview(path: str):
#     target = Path(path).resolve()
#     if not str(target).startswith(str(GENERATED_ROOT)):
#         return JSONResponse(status_code=403, content={"error": "Access denied"})
#     if not target.exists():
#         return JSONResponse(status_code=404, content={"error": "File not found"})
#     try:
#         content = target.read_text(encoding="utf-8")
#     except Exception as e:
#         return JSONResponse(status_code=500, content={"error": str(e)})
#     return {"content": content, "type": target.suffix.lower()}


# @app.get("/api/artifact/download")
# def artifact_download(path: str):
#     target = Path(path).resolve()
#     if not str(target).startswith(str(GENERATED_ROOT)):
#         return JSONResponse(status_code=403, content={"error": "Access denied"})
#     if not target.exists():
#         return JSONResponse(status_code=404, content={"error": "File not found"})
#     return FileResponse(str(target), filename=target.name, media_type="application/octet-stream")


# # -------------------- Pipeline runs (NO Pydantic -> no 422) --------------------
# def _run_job(run_id: str, payload: Dict[str, Any]):
#     def cb(status: str, current_agent: Optional[str], outputs, artifacts):
#         update(run_id, status=status, current_agent=current_agent, outputs=outputs, artifacts=artifacts)

#     try:
#         update(run_id, status="running")
#         merged = _build_inputs(payload, run_id)
#         _rid, outputs, artifacts = run_pipeline(merged, progress_cb=cb)
#         update(run_id, status="done", current_agent=None, outputs=outputs, artifacts=artifacts)
#     except Exception as e:
#         update(run_id, status="error", error=str(e), current_agent=None)


# @app.post("/api/run_async")
# def run_async(payload: Dict[str, Any], bg: BackgroundTasks):
#     run_id = uuid4().hex
#     create(run_id)
#     bg.add_task(_run_job, run_id, payload)
#     return {"run_id": run_id}


# @app.post("/api/run_phase1")
# def run_phase1(payload: Dict[str, Any]):
#     run_id = uuid4().hex
#     inp = _build_inputs(payload, run_id)
#     rid, outputs, artifacts = run_agents(inp, PHASE1, run_id=run_id)
#     return {"run_id": rid, "outputs": outputs, "artifacts_written": artifacts}


# @app.post("/api/run_phase2/{run_id}")
# def run_phase2(run_id: str, payload: Dict[str, Any]):
#     inp = _build_inputs(payload, run_id)
#     rid, outputs, artifacts = run_agents(inp, PHASE2, run_id=run_id)
#     return {"run_id": rid, "outputs": outputs, "artifacts_written": artifacts}

from fastapi import FastAPI, Request, BackgroundTasks,UploadFile, File, Form
from fastapi.responses import HTMLResponse,Response,JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from dotenv import load_dotenv
from uuid import uuid4
from fastapi.staticfiles import StaticFiles
from typing import List, Optional
import shutil
import os

from .schemas import RunRequest
from .orchestrator import run_pipeline
from .job_store import create, get, update
from .orchestrator_phased import run_agents, PHASE1, PHASE2

BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTS = {".xlsx", ".xls", ".csv", ".json", ".pdf", ".doc", ".docx", ".yml", ".yaml", ".sql", ".txt"}

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.cache = {}  # prevents: cannot use 'tuple' as a dict key

app = FastAPI(title="FlowCraft AI")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(request, "index.html", {})

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/favicon.ico")
def favicon():
    return Response(status_code=204)

@app.get("/api/status/{run_id}")
def status(run_id: str):
    job = get(run_id)
    if not job:
        return {"error": "not_found"}
    return {
        "run_id": job.run_id,
        "status": job.status,
        "current_agent": job.current_agent,
        "outputs": job.outputs,
        "artifacts_written": job.artifacts,
        "error": job.error,
    }

def _run_job(run_id: str, inputs: dict):
    def progress_cb(status: str, current_agent: str | None, outputs, artifacts):
        update(run_id, status=status, current_agent=current_agent, outputs=outputs, artifacts=artifacts)

    try:
        update(run_id, status="running")
        rid, outputs, artifacts = run_pipeline(inputs, progress_cb=progress_cb)
        # keep original run_id stable in UI
        update(run_id, status="done", current_agent=None, outputs=outputs, artifacts=artifacts)
    except Exception as e:
        update(run_id, status="error", error=str(e), current_agent=None)

@app.post("/api/run_async")
def run_async(req: RunRequest, bg: BackgroundTasks):
    run_id = uuid4().hex
    create(run_id)
    bg.add_task(_run_job, run_id, req.model_dump())
    return {"run_id": run_id}

@app.get("/api/dial_ping")
def dial_ping():
    from .llm import run_llm
    return {"reply": run_llm("ping", "Reply with exactly 'pong'.", "pong?")}


@app.post("/api/run_phase1")
def run_phase1(payload: dict):
    try:
        run_id, outputs, artifacts = run_agents(payload, PHASE1)
        return {"run_id": run_id, "outputs": outputs, "artifacts_written": artifacts}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "where": "/api/run_phase1"}
        )

@app.post("/api/run_phase2/{run_id}")
def run_phase2(run_id: str, req: RunRequest):
    run_id, outputs, artifacts = run_agents(req.model_dump(), PHASE2, run_id=run_id)
    return {"run_id": run_id, "outputs": outputs, "artifacts_written": artifacts}


# Templates
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Static (CSS/JS/images)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/")
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/api/inputs/{run_id}")
async def save_user_inputs(
    run_id: str,
    user_story: str = Form(""),
    files: Optional[List[UploadFile]] = File(None),
):
    run_dir = UPLOADS_DIR / run_id
    files_dir = run_dir / "files"
    run_dir.mkdir(parents=True, exist_ok=True)
    files_dir.mkdir(parents=True, exist_ok=True)

    # Save user story
    story_path = run_dir / "story.txt"
    story_path.write_text(user_story or "", encoding="utf-8")

    saved_files = []
    if files:
      for f in files:
          filename = os.path.basename(f.filename or "uploaded_file")
          ext = os.path.splitext(filename)[1].lower()

          if ext and ext not in ALLOWED_EXTS:
              # skip or raise; here we raise to be strict
              return {"ok": False, "error": f"File type not allowed: {filename}"}

          dest = files_dir / filename
          with dest.open("wb") as out:
              shutil.copyfileobj(f.file, out)
          saved_files.append(str(dest))

    return {
        "ok": True,
        "run_id": run_id,
        "story_path": str(story_path),
        "saved_files": saved_files,
        "message": "Inputs saved successfully"
    }