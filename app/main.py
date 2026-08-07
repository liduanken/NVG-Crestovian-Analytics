from fastapi import FastAPI, HTTPException

from app.llm import ledger
from app.models import PipelineEvent, ResolveDecision, ResolveRequest, TriageDecision
from app.resolve import resolve_task
from app.triage import triage_event

app = FastAPI(title="Pipeline Triage Agent")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/budget")
def budget():
    """Current LLM spend against the daily ceiling."""
    return ledger.snapshot()


@app.post("/triage", response_model=TriageDecision)
def triage(event: PipelineEvent):
    try:
        return triage_event(event.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/resolve", response_model=ResolveDecision)
def resolve(request: ResolveRequest):
    """Investigate a failing DAG task on the live Airflow instance and act on it."""
    try:
        return resolve_task(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
