from pydantic import BaseModel, Field
from typing import Optional, List, Literal

AgentName = Literal[
    "refinement",
    "source_discovery",
    "schema_drift",
    "transformation",
    "trust_quality",
    "deployment",
]

class RunRequest(BaseModel):
    # ✅ user_story is now OPTIONAL — prevents 422 when UI sends pipeline_name/comments only
    user_story:    Optional[str] = Field(default="")
    platform:      Optional[str] = "Databricks"
    constraints:   Optional[str] = None
    known_sources: Optional[str] = None
    drift_inputs:  Optional[str] = None
    target_style:  Optional[str] = Field(default="databricks_notebooks_and_sql")
    pipeline_name: Optional[str] = None
    comments:      Optional[str] = None

class AgentOutput(BaseModel):
    agent:   AgentName
    content: str

class RunResponse(BaseModel):
    run_id:            str
    outputs:           List[AgentOutput]
    artifacts_written: List[str]