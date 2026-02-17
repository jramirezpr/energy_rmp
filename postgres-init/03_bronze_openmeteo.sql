\connect energy

CREATE TABLE IF NOT EXISTS bronze_openmeteo_raw (
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    location_name TEXT NOT NULL,
    timezone TEXT,
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    hourly_json JSONB,
    daily_json JSONB,
    source TEXT NOT NULL,
    ingestion_ts TIMESTAMP DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS bronze_openmeteo_raw_uk
ON bronze_openmeteo_raw (location_name, start_dt, end_dt, source);
