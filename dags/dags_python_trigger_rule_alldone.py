import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from airflow.decorators import task
with DAG(
    dag_id="dags_python_trigger_rule_alldone",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    bash_upstream_1 = BashOperator(
        task_id = 'bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('python_upstream_1 exception')

    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상처리')

    @task(task_id='python_downStream' trigger_rule 'all_done')
    def python_downStream():
        print('정상처리')    

    [bash_upstream_1,python_upstream_1(),python_upstream_2()] >> python_downStream()