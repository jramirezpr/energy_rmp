#!/bin/bash
set -e

# Wait for Postgres to be ready
echo "Waiting for Postgres to be ready..."
until pg_isready -h postgres -p 5432 -U "$POSTGRES_USER"; do
  sleep 2
done
echo "Postgres is ready."

# Initialize Airflow metadata DB if needed
echo "Checking Airflow metadata DB..."
if ! airflow db check >/dev/null 2>&1; then
  echo "Airflow metadata DB not initialized. Running 'airflow db init'..."
  airflow db init
else
  echo "Airflow metadata DB already initialized."
fi

# Start the Airflow scheduler
echo "Starting Airflow scheduler..."
exec airflow scheduler
