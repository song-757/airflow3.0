import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

from airflow.decorators import task
with DAG(
    dag_id="dags_python_grigger_rule_nskip",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    @task.branch(task_id = 'branching')
    def random_branch():
        import random

        item_list =['A','B','C']
        selected_item = random.choice(item_list)

        if selected_item  == 'A':
            return 'task_a'
        elif selected_item   == 'B':
            return 'task_b'
        elif selected_item   == 'C':
            return 'task_c'    

    task_a = BashOperator(
        task_id = 'task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('정상처리')

    @task(task_id='task_c')
    def task_c():
        print('정상처리')

    @task(task_id='task_d', trigger_rule = 'none_skipped')
    def task_d():
        print('정상처리')    

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()