#!/bin/bash
set -e

# Wait for Postgres to be ready
until pg_isready -h postgres -p 5432; do
  echo "Waiting for Postgres..."
  sleep 2
done

# Initialize Airflow DB if not already initialized
if ! airflow db check >/dev/null 2>&1; then
  echo "Initializing Airflow metadata DB..."
  airflow db init
fi

# Run migrations / upgrade DB
airflow db upgrade

# Create admin user (ignore if exists)
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Create Postgres connection (ignore if exists)
airflow connections add postgres_energy \
    --conn-type postgres \
    --conn-host ${POSTGRES_HOST:-postgres} \
    --conn-login ${POSTGRES_USER:-airflow} \
    --conn-password ${POSTGRES_PASSWORD:-airflow} \
    --conn-schema ${POSTGRES_DB:-energy} \
    --conn-port 5432 || true

# Optional: set minimal Airflow variables
airflow variables set example_var "Hello World" || true

# Start webserver
exec airflow webserver


