import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task
with DAG(
    dag_id="dag_bash_python_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:

    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command="echo PUSH_START"
                        '{{ti.xcom_push(key="bash_pushed",value="200")}} && '
                        'echo PUSH_COMPLETED'
    )


    @task
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key="bash_pushed")
        return_value = ti.xcom_pull(task_ids='bash_push')
        result_dict = {'status':'good','data':[1,2,3],'options_cnt':100}
        print('status_value:'+str(status_value))
        print('return_value:'+str(return_value))
    
p   python_pull = python_pull_xcom()

    bash_push >> python_pull