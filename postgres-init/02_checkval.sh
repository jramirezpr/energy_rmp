#!/bin/bash
echo "$AIRFLOW_DB Start"

set -e
echo "Start2"

# Wait until Postgres is ready via Unix socket (inside the container)
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB">/dev/null 2>&1; do
  echo "Waiting for Postgres to be ready..."
  sleep 7
done
echo "Start3"

# Wait until the project DB exists
until psql -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$POSTGRES_DB"; do
  echo "Waiting for project DB ($POSTGRES_DB) to exist..."
  sleep 2
done
echo "$AIRFLOW_DB !!!!!!"

# Wait until the Airflow metadata DB exists
until psql -U "$POSTGRES_USER" -lqt | cut -d \| -f 1 | grep -qw "$AIRFLOW_DB"; do
  echo "Waiting for Airflow DB ($AIRFLOW_DB) to exist..."
  sleep 2
done
echo "$AIRFLOW_DB 222"

echo "All databases are ready!"
exit 0

