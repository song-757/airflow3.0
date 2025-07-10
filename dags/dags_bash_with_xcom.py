import datetime
import pendulum
from airflow.sdk import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command="echo START && "
                        " echo XCOM_PUSHED &&"
                        #"{{ti.xcom_push(key='bash_push',value='first_bash_message')}} && "
                        " echo first_bash_message &&"
                        " echo COMPLATE ",
        do_xcom_push=True                
    )

    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {
            # 'PUSHED_VALUE':"{{ti.xcom_pull(key='bash_push')}}",
            'PUSHED_VALUE':"{{ti.xcom_pull(task_ids='bash_push')}}",
            'RETURN_VALUE':"{{ti.xcom_pull(task_ids='bash_push')}}"
        },
        bash_command="echo $PUSHED_VALUE && $RETURN_VALUE",
        do_xcom_push = False
    )
    
    bash_push >> bash_pull