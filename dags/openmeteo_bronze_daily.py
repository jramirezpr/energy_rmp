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

    def ingest_for_location(location, force_backfill=False):
        city_name = location.get("name")
        log_event("city_started", city=city_name)

        logger.info("Starting ingestion for location: %s", city_name)
    

        today = date.today()
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=5432
        )
        cur = conn.cursor()

        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_openmeteo_raw (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                location_name TEXT,
                timezone TEXT,
                start_dt DATE,
                end_dt DATE,
                hourly_json JSONB,
                daily_json JSONB,
                source TEXT NOT NULL,
                ingestion_ts TIMESTAMP DEFAULT now()
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
                (location.get("name"),)
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
                logger.error("Error fetching %s: %s", location.get("name"), e)
                current = end_date + timedelta(days=1)
                continue 

            if not payload.get('hourly'):
                logger.warning("No hourly data found for %s", location.get("name"))
            if not payload.get('daily'):
                logger.warning("No daily data found for %s", location.get("name"))

            try:
                cur.execute(
                    """
                    INSERT INTO bronze_openmeteo_raw
                    (latitude, longitude, location_name, timezone, start_dt, end_dt, hourly_json, daily_json, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_name, start_dt, end_dt, source) DO NOTHING;
                    """,
                    (
                        location['lat'],
                        location['lon'],
                        location.get('name'),
                        payload.get('timezone'),
                        start_date,
                        end_date,
                        json.dumps(payload.get('hourly', {})),
                        json.dumps(payload.get('daily', {})),
                        'openmeteo_historical'
                    )
                )
                conn.commit()
            except Exception as e:
                logger.error(
                    "DB insert failed for %s (%s - %s): %s",
                    location.get("name"),
                    start_date,
                    end_date,
                    e
                )
                conn.rollback()

            current = end_date + timedelta(days=1)

        cur.close()
        conn.close()
        log_event("city_complete", city=city_name)


    def run_ingest(**context):
        """Run ingestion for all locations in CONFIG_FILE"""
        dag_run_id = context.get('dag_run').run_id if context.get('dag_run') else None
        log_event("ingestion_started", dag_run_id=dag_run_id)

        # Determine if this DAG run should backfill
        force_backfill = context.get('dag_run').conf.get('backfill', False) if context.get('dag_run') else False
        force_backfill = force_backfill or Variable.get("bronze_openmeteo_backfill", default_var=False, deserialize_json=True)

        with open(CONFIG_FILE, 'r') as f:
            locations = json.load(f)

        for loc in locations:
            ingest_for_location(loc, force_backfill=force_backfill)

        log_event("ingestion_complete", dag_run_id=dag_run_id)

