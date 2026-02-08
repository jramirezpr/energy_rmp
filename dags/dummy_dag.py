from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("dummy_dag", start_date=datetime(2026, 2, 8), schedule_interval=None, catchup=False) as dag:
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Hello Airflow' && sleep 5"
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'Goodbye Airflow' && sleep 5"
    )

    start >> end
