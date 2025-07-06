import pendulum
from airflow import DAG
from airflow.decorators import task



with DAG(
    dag_id="dags_python_test_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 6, 1, tz="ASIA/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        """Print the Airflow context and ds variable from the context."""
        print(some_input)
        return "Whatever you return gets printed in the logs"

    python_task_1 = print_context('파이썬 데코레이터 실행')
    