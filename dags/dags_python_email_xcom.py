import datetime
import pendulum
from airflow.sdk import DAG

from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    @task
    def some_lgoic(**kwargs):
        from random import choice
        return choice('Success','Fail')
        
    send_email = EmailOperator(
        task_id = 'send_email',
        to='hyoarang@naver.com',
        subject ='{{data_interval_end.intimezone("Asia/Seoul") | ds}} some_logic 처리결과',
        html_content ='{{data_interval_end.intimezone("Asia/Seoul") | ds}} some_logic 처리결과는 <br> \
                        {{ti.xcom_pull(task_id="some_lgoic")}}'

    )    

    python_push = some_lgoic();

    python_push >> send_email