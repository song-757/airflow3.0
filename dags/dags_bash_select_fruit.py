import datetime
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command="/opt/airflow/plugins/test.sh ORANGE"
    )

    t1_avocado = BashOperator(
        task_id = "t1_avocado",
        bash_command="/opt/airflow/plugins/test.sh AVOCADO"
    )

    t1_orange >> t1_avocado
    