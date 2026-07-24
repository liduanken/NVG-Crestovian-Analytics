-- Eval scenario state — DAGs read this to determine their behavior.
-- bootstrap.sh seeds a row per scenario after the Airflow stack is up.
CREATE TABLE IF NOT EXISTS eval_scenario_state (
    dag_id                  TEXT PRIMARY KEY,
    failure_mode            TEXT NOT NULL DEFAULT 'none',
    -- Modes: none | transient | persistent | data_corruption | zero_rows
    retry_count_remaining   INT NOT NULL DEFAULT 0,
    error_code              TEXT,
    extra                   JSONB DEFAULT '{}'::jsonb,
    updated_at              TIMESTAMPTZ DEFAULT now()
);

-- web_events table used by the web_analytics DAG.
-- Note: the column is 'user_id', not 'user_uuid'. The DAG has a bug that references 'user_uuid'.
CREATE TABLE IF NOT EXISTS web_events (
    id              BIGSERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    session_id      TEXT,
    page_url        TEXT,
    properties      JSONB DEFAULT '{}'::jsonb
);

-- Seed web_events with enough data that a working query returns results.
INSERT INTO web_events (user_id, event_type, event_timestamp, page_url)
SELECT
    'user_' || (i % 500),
    CASE (i % 4)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'click'
        WHEN 2 THEN 'form_submit'
        ELSE 'session_start'
    END,
    now() - (random() * interval '48 hours'),
    '/page/' || (i % 20)
FROM generate_series(1, 10000) AS s(i)
ON CONFLICT DO NOTHING;
