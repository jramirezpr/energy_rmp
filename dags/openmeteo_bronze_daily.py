import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta, date
import requests
import psycopg2
import os

logger = logging.getLogger(__name__)

def log_event(event_name, **kwargs):
    """Minimal structured logging for ingestion events."""
    logger.info("EVENT: %s | %s", event_name, kwargs)

# ----------------------------
# Config
# ----------------------------
CONFIG_FILE = '/opt/airflow/config/locations.json'
HIST_START = date(2022, 1, 1)
OPENMETEO_BASE = "https://archive-api.open-meteo.com/v1/archive"
CHUNK_DAYS = 7

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'bronze_openmeteo_daily_json',
    default_args=default_args,
    description='Daily ingestion of historical Open-Meteo data from JSON-config locations',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    def ingest_for_location(location, force_backfill=False, dag_run_id=None): 
        

        city_name = location.get("name")
        log_event("city_started", city=city_name)
        logger.info("Starting ingestion for location: %s", city_name)
        run_type = 'backfill' if force_backfill else 'initial' 
        dag_run_id_value = dag_run_id if dag_run_id else None
        run_type_value = 'backfill' if force_backfill else 'initial'

        today = date.today()
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=5432
        )
        cur = conn.cursor()

        # Ensure table exists (new schema with upsert_ts)
        cur.execute("""
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
                upsert_ts TIMESTAMP NOT NULL DEFAULT now(),
                dag_run_id TEXT,
                run_type TEXT,
                PRIMARY KEY (location_name, start_dt, end_dt, source, upsert_ts)
            );
        """)

        # Determine where to start
        if force_backfill:
            current = HIST_START
        else:
            cur.execute(
                """
                SELECT MAX(end_dt)
                FROM bronze_openmeteo_raw
                WHERE location_name = %s AND source = 'openmeteo_historical'
                """,
                (city_name,)
            )
            last_end = cur.fetchone()[0]
            current = (last_end + timedelta(days=1)) if last_end else HIST_START

        # Loop over date ranges
        while current <= today:
            start_date = current
            end_date = min(current + timedelta(days=CHUNK_DAYS - 1), today)

            url = (
                f"{OPENMETEO_BASE}?latitude={location['lat']}&longitude={location['lon']}"
                f"&start_date={start_date}&end_date={end_date}"
                "&hourly=temperature_2m,precipitation,windspeed_10m"
                "&daily=temperature_2m_max,precipitation_sum"
                "&timezone=auto"
            )

            try:
                r = requests.get(url)
                r.raise_for_status()
                payload = r.json()
            except requests.RequestException as e:
                logger.error("Error fetching %s: %s", city_name, e)
                current = end_date + timedelta(days=1)
                continue 

            if not payload.get('hourly'):
                logger.warning("No hourly data found for %s", city_name)
            if not payload.get('daily'):
                logger.warning("No daily data found for %s", city_name)

            try:
                cur.execute(
                    """
                    INSERT INTO bronze_openmeteo_raw
                    (latitude, longitude, location_name, timezone, start_dt, end_dt, hourly_json, daily_json, source, dag_run_id, run_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_name, start_dt, end_dt, source, upsert_ts) DO NOTHING;
                    """,
                    (
                        location['lat'],
                        location['lon'],
                        city_name,
                        payload.get('timezone'),
                        start_date,
                        end_date,
                        json.dumps(payload.get('hourly', {})),
                        json.dumps(payload.get('daily', {})),
                        'openmeteo_historical',
                        dag_run_id_value, 
                        run_type_value
                    )
                )
                conn.commit()
            except Exception as e:
                logger.error(
                    "DB insert failed for %s (%s - %s): %s",
                    city_name,
                    start_date,
                    end_date,
                    e
                )
                conn.rollback()

            current = end_date + timedelta(days=1)

        cur.close()
        conn.close()
        log_event("city_complete", city=city_name)


    def check_idempotency(**context):
        """Quick idempotency test: ensure no duplicate (location, start, end, source, upsert_ts)."""
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=5432
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT location_name, start_dt, end_dt, source, COUNT(*)
            FROM bronze_openmeteo_raw
            GROUP BY location_name, start_dt, end_dt, source, upsert_ts
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()
        if duplicates:
            logger.warning("Idempotency check FAILED, duplicates found: %s", duplicates)
        else:
            logger.info("Idempotency check passed, no duplicates found.")
        cur.close()
        conn.close()
    def run_ingest(**context):
        dag_run_id = context.get('dag_run').run_id if context.get('dag_run') else None
        log_event("ingestion_started", dag_run_id=dag_run_id)

        # Determine if this DAG run should backfill
        force_backfill = context.get('dag_run').conf.get('backfill', False) if context.get('dag_run') else False
        force_backfill = force_backfill or Variable.get("bronze_openmeteo_backfill", default_var=False, deserialize_json=True)

        with open(CONFIG_FILE, 'r') as f:
            locations = json.load(f)

        for loc in locations:
            ingest_for_location(loc, force_backfill=force_backfill, dag_run_id=dag_run_id)

        log_event("ingestion_complete", dag_run_id=dag_run_id)

    # PythonOperators

    ingest_task = PythonOperator(
        task_id='ingest_openmeteo_from_json',
        python_callable=run_ingest,
        provide_context=True
    )

    idempotency_task = PythonOperator(
        task_id='check_idempotency',
        python_callable=check_idempotency,
        provide_context=True
    )

    # Task dependencies
    ingest_task >> idempotency_task

