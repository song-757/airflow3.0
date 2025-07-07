import datetime
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    regist_t1 =  PythonOperator(
        task_id = 'regist_t1',
        python_callable=regist
    )


    regist_t1