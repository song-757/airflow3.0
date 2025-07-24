import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

def insert_postgress(ip, port, dbname, user, pwd, **kwargs):
    print('실행되나')

# ✅ 전역에 선언!
dag = DAG(
    dag_id="dag_python_postgres_insert",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)

with dag:
    insert_postgress_task = PythonOperator(
        task_id='insert_postgress_task',
        python_callable=insert_postgress,
        op_args=['172.28.0.3', '5432', 'song', 'song', 'song']
    )

    insert_postgress_task
