import datetime
import pendulum
from airflow.sdk import DAG

from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dag_python_postgres_insert",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    def insert_postgress(ip,port,dbname,user,pwd,**kwargs):
        import psycopg2
        from contextlib import closing
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user,password=pwd,port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg='insert 수행'
                sql='insert into py_opr_drct_insrt values(%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()
        


    insert_postgress = PythonOperator(
        task_id ='insert_postgress',
        python_callable= insert_postgress,
        op_args=['172.28.0.3','5432','song','song'] 
    )


    insert_postgress