import datetime
import pendulum
from airflow.sdk import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dag_python_bash_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    python_push =  PythonOperator(
        task_id = 'python_push',
        result_dict = {'status':'good','data':'[1,2,3]','options_cnt':'10'}
        return result_dict
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env ={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'
        },
        bash_command="echo $STATUS && echo $DATA && echo $OPTIONS "
    )

    python_push >> bash_pull