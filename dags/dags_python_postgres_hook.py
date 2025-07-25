import datetime
import pendulum
from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_postgres_hook",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def insert_postgress(postgres_conn_id,**kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn())as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg='insert 수행 hook'
                sql='insert into py_opr_drct_insrt values(%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()
        

    insert_postgress_hook = PythonOperator(
        task_id ='insert_postgress_hook',
        python_callable= insert_postgress,
        op_kwargs=['postgres_conn_id': 'conn-db-postgres-custom'] 
    )


    insert_postgress_hook