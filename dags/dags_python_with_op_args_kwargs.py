import datetime
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_args_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    regist2_t1 =  PythonOperator(
        task_id = 'regist2_t1',
        python_callable=regist2,
        op_args=['kimsong','man','kr','seoul'],
        op_kwargs={'email':'hyoarang@naver.com','phone':'010'}
    )


    regist2_t1