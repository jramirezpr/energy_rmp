\connect energy

CREATE TABLE bronze_openmeteo_raw (
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    location_name TEXT NOT NULL,
    timezone TEXT,
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    hourly_json JSONB,
    daily_json JSONB,
    source TEXT NOT NULL,
    upsert_ts TIMESTAMP NOT NULL DEFAULT now(), 
    dag_run_id TEXT,  -- optional, links to the Airflow DAG run
    run_type TEXT,    -- 'initial', 'backfill', 'correction'
    PRIMARY KEY (location_name, start_dt, end_dt, source, upsert_ts)
);

CREATE INDEX idx_bronze_latest
ON bronze_openmeteo_raw(location_name, start_dt, end_dt, source, upsert_ts DESC);
