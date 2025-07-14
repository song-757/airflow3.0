import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator



with DAG(
    dag_id="dags_bash_variable",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def select_random():
        import random

        item_list =['A','B','C']
        selected_item = random.choice(item_list)

        if selected_item  == 'A':
            return 'task_a'
        elif selected_item  in ['B','C']:
            return ['task_b','task_c']
        
    def common_fuunc(**kwargs):
        print(kwargs['selected'])     

    task_branch_python_operator = BranchPythonOperator(
        task_id = 'task_branch_python_operator',
        python_callable = select_random 
    )

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'A'}
    )

    task_a = PythonOperator(
        task_id = 'task_b',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'B'}
    )

    task_a = PythonOperator(
        task_id = 'task_c',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'C'}
    )