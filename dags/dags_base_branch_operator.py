import datetime
import pendulum
from airflow.sdk import DAG

from airflow.decorators import task
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator



with DAG(
    dag_id="dags_base_branch_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def common_fuunc(**kwargs):
        print(kwargs['selected'])

    class CustomBaseBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):

            import random

            item_list =['A','B','C']
            selected_item = random.choice(item_list)

            if selected_item  == 'A':
                return 'task_a'
            elif selected_item  in ['B','C']:
                return ['task_b','task_c']

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'A'}
    )

    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'B'}
    )

    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable =common_fuunc,
        op_kwargs = {'selected' : 'C'}
    )

    custom_base_branch_operator = CustomBaseBranchOperator(task_id='base_branch_operator' )
    custom_base_branch_operator.choose_branch()