from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'krzkos',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_dbt_flow',
    default_args=default_args,
    description='DBT transformation orchestration',
    schedule_interval=timedelta(minutes=10),
    catchup=False
) as dag:

    check_db = BashOperator(
        task_id='check_postgres',
        bash_command='nc -zv localhost 5432'
    )

    run_dbt = BashOperator(
        task_id='dbt_run_refresh',
        bash_command='cd /opt/airflow/dbt_ecommerce && python3 -m dbt run'
    )

    check_db >> run_dbt
