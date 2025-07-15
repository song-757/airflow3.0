import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label


from airflow.decorators import task
with DAG(
    dag_id="dags_python_label",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag: 

    empty_1 = EmptyOperatorOperator(
        task_id ='empty_1'
    )

    empty_2 = EmptyOperatorOperator(
        task_id ='empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperatorOperator(
        task_id ='empty_3'
    )

    empty_4 = EmptyOperatorOperator(
        task_id ='empty_4'
    )

    empty_4 = EmptyOperatorOperator(
        task_id ='empty_5'
    )

    empty_4 = EmptyOperatorOperator(
        task_id ='empty_6'
    )

    empty_2 >> Label('Start Branch') >> [empty_3,empty_4,empty_5] >> Label('End Branch') >> empty_6