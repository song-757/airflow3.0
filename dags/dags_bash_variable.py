import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_variable",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator (
        task_id ="bash_var_1",
        bash_command =f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator (
        task_id ="bash_var_2",
        bash_command ="echo variable:{{var.value.sample_key}}"
    )

    bash_var_1 >> bash_var_2