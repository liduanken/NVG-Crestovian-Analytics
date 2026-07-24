-- Seed 7 days of historical successful runs and today's current failures.
-- Run via: docker compose exec -T postgres psql -U airflow -f /seed/seed_history.sql
-- (invoked automatically by bootstrap.sh after `airflow db migrate`).
--
-- Uses log_template_id from the existing record (set by Airflow on db init).
-- Inserts are idempotent via ON CONFLICT DO NOTHING.

DO $$
DECLARE
    tmpl_id INTEGER;
BEGIN
    SELECT id INTO tmpl_id FROM log_template ORDER BY id DESC LIMIT 1;
    IF tmpl_id IS NULL THEN
        tmpl_id := 1;
    END IF;

    -- ── crm_sync (high criticality, daily at 02:00 UTC) ─────────────────────
    -- 6 successful runs over the past 7 days, then today's failure is triggered by bootstrap.sh
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'crm_sync',
        'scheduled__' || to_char(d, 'YYYY-MM-DD') || 'T02:00:00+00:00',
        'scheduled',
        d + interval '2 hours',
        d + interval '2 hours',
        d + interval '1 day' + interval '2 hours',
        d + interval '2 hours' + interval '5 seconds',
        d + interval '2 hours' + interval '127 seconds',
        'success',
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '2 days',
        interval '1 day'
    ) AS d
    ON CONFLICT DO NOTHING;

    -- ── market_feed_legacy (deprecated, every 30 min) ────────────────────────
    -- Shows a pattern of repeated failures (it's been end-of-life since 2026-07-01)
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'market_feed_legacy',
        'scheduled__' || to_char(d, 'YYYY-MM-DD"T"HH24:MI:SS') || '+00:00',
        'scheduled',
        d,
        d,
        d + interval '30 minutes',
        d + interval '2 seconds',
        d + interval '5 seconds',
        'failed',
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day',
        interval '6 hours'
    ) AS d
    ON CONFLICT DO NOTHING;

    -- ── risk_scores (critical, daily at 01:00 UTC) ───────────────────────────
    -- 6 successful runs, today's run will be the persistent outage failure
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'risk_scores',
        'scheduled__' || to_char(d, 'YYYY-MM-DD') || 'T01:00:00+00:00',
        'scheduled',
        d + interval '1 hour',
        d + interval '1 hour',
        d + interval '1 day' + interval '1 hour',
        d + interval '1 hour' + interval '3 seconds',
        d + interval '1 hour' + interval '186 seconds',
        'success',
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '2 days',
        interval '1 day'
    ) AS d
    ON CONFLICT DO NOTHING;

    -- ── revenue_daily (critical, daily at 03:00 UTC) ─────────────────────────
    -- 6 successful runs, today's run will be the DATA_CORRUPTION failure
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'revenue_daily',
        'scheduled__' || to_char(d, 'YYYY-MM-DD') || 'T03:00:00+00:00',
        'scheduled',
        d + interval '3 hours',
        d + interval '3 hours',
        d + interval '1 day' + interval '3 hours',
        d + interval '3 hours' + interval '4 seconds',
        d + interval '3 hours' + interval '243 seconds',
        'success',
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '2 days',
        interval '1 day'
    ) AS d
    ON CONFLICT DO NOTHING;

    -- ── web_analytics (medium, every 2h) ─────────────────────────────────────
    -- Shows recent pattern of failures (bug was introduced ~2 days ago after a deploy)
    -- Older runs succeed; recent ones fail
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'web_analytics',
        'scheduled__' || to_char(d, 'YYYY-MM-DD"T"HH24:MI:SS') || '+00:00',
        'scheduled',
        d,
        d,
        d + interval '2 hours',
        d + interval '2 seconds',
        d + interval '8 seconds',
        CASE WHEN d < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '2 days'
             THEN 'success' ELSE 'failed' END,
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '4 hours',
        interval '2 hours'
    ) AS d
    ON CONFLICT DO NOTHING;

    -- ── report_export (low, weekdays at 06:00 UTC) ───────────────────────────
    -- 5 successful weekday runs; today's run succeeded but with 0 rows
    INSERT INTO dag_run (dag_id, run_id, run_type, execution_date, data_interval_start, data_interval_end, start_date, end_date, state, external_trigger, log_template_id)
    SELECT
        'report_export',
        'scheduled__' || to_char(d, 'YYYY-MM-DD') || 'T06:00:00+00:00',
        'scheduled',
        d + interval '6 hours',
        d + interval '6 hours',
        d + interval '1 day' + interval '6 hours',
        d + interval '6 hours' + interval '3 seconds',
        d + interval '6 hours' + interval '47 seconds',
        'success',
        false,
        tmpl_id
    FROM generate_series(
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '7 days',
        date_trunc('day', now() AT TIME ZONE 'UTC') - interval '2 days',
        interval '1 day'
    ) AS d
    WHERE extract(dow FROM d) BETWEEN 1 AND 5  -- weekdays only
    ON CONFLICT DO NOTHING;

END $$;
