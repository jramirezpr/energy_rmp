#!/bin/bash
set -e

# Wait for Postgres to be ready
echo "Waiting for Postgres..."
until pg_isready -h postgres -p 5432 -U "$POSTGRES_USER"; do
  sleep 2
done
echo "Postgres is ready."

# Initialize Airflow metadata DB if needed
if ! airflow db check >/dev/null 2>&1; then
  echo "Initializing Airflow metadata DB..."
  airflow db init
else
  echo "Airflow metadata DB already initialized."
fi

# Run migrations / upgrade DB
echo "Upgrading Airflow metadata DB..."
airflow db upgrade

# Create admin user if not exists
echo "Ensuring admin user exists..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Create Postgres connection if not exists
echo "Ensuring Postgres connection exists..."
airflow connections add postgres_energy \
    --conn-type postgres \
    --conn-host ${POSTGRES_HOST:-postgres} \
    --conn-login ${POSTGRES_USER:-airflow} \
    --conn-password ${POSTGRES_PASSWORD:-airflow} \
    --conn-schema ${POSTGRES_DB:-energy} \
    --conn-port 5432 || true

# Optional: set minimal Airflow variables
echo "Setting example Airflow variables..."
airflow variables set example_var "Hello World" || true

# Start webserver
echo "Starting Airflow webserver..."
exec airflow webserver


