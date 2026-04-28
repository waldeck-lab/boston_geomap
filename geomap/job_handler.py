from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class JobState:
    job_id: str
    kind: str
    status: str = "queued"       # queued, running, done, failed, cancelled
    phase: str = "planning"
    current_step: str = ""
    total_steps: int = 0
    completed_steps: int = 0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    updated_at: Optional[float] = None
    eta_seconds: Optional[int] = None
    error: Optional[str] = None
    warnings: list[str] = field(default_factory=list)

    def touch(self):
        self.updated_at = time.time()
        

def update_progress(job: JobState, phase: str, step: str, done_inc: int = 0):
    job.phase = phase
    job.current_step = step
    job.completed_steps += done_inc
    job.touch()

    if job.completed_steps > 0 and job.started_at:
        elapsed = time.time() - job.started_at
        rate = elapsed / job.completed_steps
        remaining = max(job.total_steps - job.completed_steps, 0)
        job.eta_seconds = int(rate * remaining)

