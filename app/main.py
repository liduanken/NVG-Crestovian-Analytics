from fastapi import FastAPI, HTTPException

from app.models import PipelineEvent, ResolveDecision, ResolveRequest, TriageDecision
from app.triage import triage_event

app = FastAPI(title="Pipeline Triage Agent")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/triage", response_model=TriageDecision)
def triage(event: PipelineEvent):
    try:
        return triage_event(event.model_dump())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/resolve", response_model=ResolveDecision)
def resolve(request: ResolveRequest):
    """
    Connect to a live Airflow instance, investigate the failing task,
    and take action (retry, skip, or escalate).

    Airflow is at request.airflow_url — basic auth via request.airflow_user /
    request.airflow_password. DAG sources are in airflow/dags/ and can be read
    or patched if the failure is a code bug.

    Include llm_calls, tokens_used, and tool_calls in the response for eval scoring.
    """
    raise NotImplementedError
