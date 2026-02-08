#!/bin/bash
set -e

# Wait for Postgres to be ready
until pg_isready -h postgres -p 5432; do
  echo "Waiting for Postgres..."
  sleep 2
done

# Wait until Airflow metadata DB is initialized
until airflow db check >/dev/null 2>&1; do
  echo "Waiting for Airflow metadata DB to be ready..."
  sleep 5
done

echo "Airflow DB is ready. Starting scheduler..."

# Start scheduler
exec airflow scheduler
