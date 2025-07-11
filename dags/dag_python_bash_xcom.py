import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task
with DAG(
    dag_id="dag_python_bash_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    @task(task_id='python_push_xcom')
    def python_push_xcom():
        result_dict = {'status':'good','data':[1,2,3],'options_cnt':100}
        return result_dict
    

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env ={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push_xcom")["status"]}}',
            'DATA':'{{ti.xcom_pull(task_ids="python_push_xcom")["data"]}}',
            'OPTIONS':'{{ti.xcom_pull(task_ids="python_push_xcom")["options_cnt"]}}'
        },
        bash_command="echo $STATUS && echo $DATA && echo $OPTIONS "
    )

    push_task = python_push_xcom()

    push_task >> bash_pull