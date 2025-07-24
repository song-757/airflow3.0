from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id="dag_test_hello",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
)

with dag:
    t1 = BashOperator(
        task_id="print_hello",
        bash_command="echo Hello, Airflow!"
    )
