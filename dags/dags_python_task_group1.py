import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from airflow.decorators import task
with DAG(
    dag_id="dags_python_task_group1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)    

    @task_group(group_id="first_group")
    def group_1():
        '''Task group 데커레이터를 이용한 첫번째 그룹입니다.'''
        
        
        @task(task_id = 'inner_task_1')
        def inner_task_1(**kwargs):
            print('첫번째 task_group내 첫번째 Task 입니다. ')

        inner_task_2 = PythonOperator(
            task_id = 'inner_task_2'
            python_callable =inner_func
            print('첫번째 task_group내 두번째 Task 입니다. ')
        )
        inner_task_1 >> inner_task_2
    with TaskGroup(group_id='',tooltip ='두번째그룹입니다') as group_2:    
        @task(task_id = 'inner_task_1')
        def inner_task_1(**kwargs):
            print('두번째 task_group내 첫번째 Task 입니다. ')

        inner_task_2 = PythonOperator(
            task_id = 'inner_task_2',
            python_callable =inner_func,
            op_kwargs={'msg','두번째 task_group내 두번째 Task 입니다.'}
        )
        inner_task_1 >> inner_task_2

    group_1 >> group_2