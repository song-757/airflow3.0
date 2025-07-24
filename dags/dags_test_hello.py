from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 정의
dag = DAG(
    dag_id="dag_test_hello",
    start_date=datetime(2024, 1, 1),
    schedule="@once",        # ✅ schedule_interval → schedule 로 수정
    catchup=False
)

# DAG에 태스크 추가
with dag:
    t1 = BashOperator(
        task_id="print_hello",
        bash_command="echo Hello, Airflow!"
    )
