import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import requests
import psycopg2
import os

# ----------------------------
# Config
# ----------------------------
CONFIG_FILE = '/opt/airflow/config/locations.json'
HIST_START = date(2022, 1, 1)
OPENMETEO_BASE = "https://archive-api.open-meteo.com/v1/archive"

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

    def ingest_for_location(location):
        today = date.today()
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'postgres'),
            database=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            port=5432
        )
        cur = conn.cursor()

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

        current = HIST_START
        while current <= today:
            start_date = current
            end_date = min(current + timedelta(days=6), today)

            url = (
                f"{OPENMETEO_BASE}?latitude={location['lat']}&longitude={location['lon']}"
                f"&start_date={start_date}&end_date={end_date}"
                "&hourly=temperature_2m,precipitation,windspeed_10m"
                "&daily=temperature_2m_max,precipitation_sum"
                "&timezone=auto"
            )
            r = requests.get(url)
            r.raise_for_status()
            payload = r.json()

            cur.execute(
                """
                INSERT INTO bronze_openmeteo_raw
                (latitude, longitude, location_name, timezone, start_dt, end_dt, hourly_json, daily_json, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            current = end_date + timedelta(days=1)

        cur.close()
        conn.close()

    def run_ingest():
        """ run ingest for all locations in CONFIG_FILE"""
        with open(CONFIG_FILE, 'r') as f:
            locations = json.load(f)

        for loc in locations:
            ingest_for_location(loc)

    ingest_task = PythonOperator(
        task_id='ingest_openmeteo_from_json',
        python_callable=run_ingest
    )
