"""Central configuration. All tunables live here so cost/behaviour levers are visible in one place."""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
MOCK_DATA_DIR = REPO_ROOT / "mock_data"
DAGS_DIR = REPO_ROOT / "airflow" / "dags"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Settings:
    llm_api_key: str = os.environ.get("LLM_API_KEY", "")
    llm_base_url: str = os.environ.get(
        "LLM_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai"
    )
    llm_model: str = os.environ.get("LLM_MODEL", "gemini-3.5-flash")
    llm_timeout_seconds: float = _env_float("LLM_TIMEOUT_SECONDS", 30.0)

    # Per-million-token prices used for budget accounting. Defaults track
    # gemini-2.0-flash; override per provider so the budget guard stays honest.
    price_per_1m_input: float = _env_float("LLM_PRICE_PER_1M_INPUT", 0.10)
    price_per_1m_output: float = _env_float("LLM_PRICE_PER_1M_OUTPUT", 0.40)

    daily_budget_usd: float = _env_float("DAILY_BUDGET_USD", 3.00)

    # Bounds the /resolve tool loop. Each step is at most one LLM call.
    max_resolve_steps: int = _env_int("MAX_RESOLVE_STEPS", 8)
    max_log_chars: int = _env_int("MAX_LOG_CHARS", 4000)
    history_window: int = _env_int("HISTORY_WINDOW", 7)

    # Reasoning models spend most of this budget thinking before emitting any
    # visible output, so a tight cap returns an empty response, not a short one.
    max_triage_tokens: int = _env_int("MAX_TRIAGE_TOKENS", 3000)
    max_resolve_tokens: int = _env_int("MAX_RESOLVE_TOKENS", 4000)

    airflow_url: str = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
    airflow_user: str = os.environ.get("AIRFLOW_USER", "admin")
    airflow_password: str = os.environ.get("AIRFLOW_PASSWORD", "admin")

    # Guardrail: DAG source edits are irreversible in a way retries are not.
    allow_dag_patching: bool = os.environ.get("ALLOW_DAG_PATCHING", "true").lower() == "true"


settings = Settings()
