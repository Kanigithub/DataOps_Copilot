from dataclasses import dataclass, field
from typing import Dict, List, Optional
from threading import Lock

@dataclass
class Job:
    run_id: str
    status: str = "queued"     # queued|running|done|error
    current_agent: Optional[str] = None
    outputs: List[dict] = field(default_factory=list)
    artifacts: List[str] = field(default_factory=list)
    error: Optional[str] = None

_STORE: Dict[str, Job] = {}
_LOCK = Lock()

def create(run_id: str) -> Job:
    with _LOCK:
        j = Job(run_id=run_id)
        _STORE[run_id] = j
        return j

def get(run_id: str) -> Optional[Job]:
    with _LOCK:
        return _STORE.get(run_id)

def update(run_id: str, **kwargs):
    with _LOCK:
        j = _STORE[run_id]
        for k, v in kwargs.items():
            setattr(j, k, v)