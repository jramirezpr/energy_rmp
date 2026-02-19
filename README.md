# Energy Weather Forecasting Pipeline

End-to-end data engineering and forecasting pipeline using Airflow, Postgres, dbt, and Python.

This project is a demo focusing on realistic data pipeline design: idempotent ingestion, backfills, schema validation, and reproducible transformations.

---

## Project Status

**Current state:**

- Dockerized Airflow + Postgres stack completed
- Bronze ingestion layer complete (Open-Meteo historical data)
- Chunked historical ingestion with backfill support
- Idempotent inserts with re-run safety
- dbt connected with Bronze schema tests
- Silver modeling, forecasting, and dashboards in progress

See the Roadmap section for planned next steps.

---

## Bronze Layer (Ingestion)

### Data Source

- Open-Meteo Archive API
- Historical + daily ingestion
- Location list driven by `locations.json`

### Airflow DAG

**DAG name:** `bronze_openmeteo_daily_json`

**Key features:**

- Chunked ingestion (7-day windows)
- Automatic resume from last ingested date
- Optional full backfill via DAG run config or Airflow Variable
- Structured logging for ingestion lifecycle
- Minimal retries and error handling

### Idempotency Strategy

- Composite primary key: `(location_name, start_dt, end_dt, source, upsert_ts)`
- `upsert_ts` allows safe re-runs, backfills, and corrections
- `ON CONFLICT DO NOTHING` prevents exact duplicate inserts
- Re-running the DAG does not corrupt or overwrite existing data

### Bronze Table

**Table:** `bronze_openmeteo_raw`

Stores raw JSON payloads (`hourly_json`, `daily_json`) along with metadata:

- location
- date range
- ingestion timestamp
- DAG run ID
- run type (initial / backfill)

Bronze is append-only and intentionally denormalized.

---

## Data Quality (dbt)

- dbt is connected to the `energy` Postgres database
- Bronze tables are defined as dbt sources
- Minimal schema tests validate ingestion correctness:
  - `not_null` on critical columns
  - existence checks

These tests ensure the ingestion pipeline is trustworthy before transformations begin.

---

## Tech Stack

- **Orchestration:** Airflow
- **Storage:** Postgres
- **Transformations:** dbt
- **Modeling:** Python (statsmodels, sklearn)
- **Visualization:** Streamlit / Plotly
- **Infrastructure:** Docker & Docker Compose

---

## Roadmap (High Level)

- [x] Environment & infrastructure
- [x] Bronze ingestion (Airflow)
- [x] Idempotency & backfill support
- [x] Bronze schema tests (dbt)
- [ ] Staging models
- [ ] Silver models
- [ ] Forecasting
- [ ] Dashboard
- [ ] Documentation & cleanup

---

## Notes

- Secrets are managed via environment variables (`.env` not committed)
- This project is intended as a demo and portfolio piece
- This repo is intentionally built step by step, with commits reflecting real pipeline evolution
