-- dbt grants this group access only to the models used by the BI dashboard.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'olist_bi_reader') THEN
        CREATE ROLE olist_bi_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END $$;
GRANT USAGE ON SCHEMA analytics TO olist_bi_reader;

CREATE VIEW analytics.bi_publication_status AS
SELECT run_id, status, completed_at,
    status = 'SUCCESS' AND NOT EXISTS (
        SELECT 1 FROM audit.warehouse_load_runs load
        WHERE load.committed_at > latest.completed_at
    ) AS available
FROM (
    SELECT run_id, status, started_at, completed_at FROM audit.pipeline_runs
    ORDER BY COALESCE(completed_at, started_at) DESC, run_id DESC LIMIT 1
) latest;
GRANT SELECT ON analytics.bi_publication_status TO olist_bi_reader;

-- Existing local marts need the same grants as models created after migration.
DO $$ DECLARE model text; BEGIN
    FOREACH model IN ARRAY ARRAY['mart_revenue_daily', 'mart_average_order_value', 'fct_orders'] LOOP
        IF to_regclass(format('analytics.%I', model)) IS NOT NULL THEN
            EXECUTE format('GRANT SELECT ON analytics.%I TO olist_bi_reader', model);
        END IF;
    END LOOP;
END $$;
