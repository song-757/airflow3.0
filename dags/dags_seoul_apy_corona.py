from operators.seoul_api_to_csv_oerator import SeoulApiToCsvOperatro
from airflow import DAG
import pendulum


with DAG(
    dag_id="dags_seoul_apy_corona",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 6, 24, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
   
) as dag:
    
    tb_corona19_count_status = SeoulApiToCsvOperatro(
        task_id = 'tb_corona19_count_status',
        dataset_nm = 'TbCorona19CountStatus',
        path='opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul")|ds_nodash}}',
        file_name = 'TbCorona19CountStatus.csv'
    )

    tb_corona19_vaccine_stat_new = SeoulApiToCsvOperatro(
        task_id = 'tb_corona19_vaccine_stat_new',
        dataset_nm = 'TbCorona19VaccineStartNew',
        path='opt/airflow/files/TbCorona19VaccineStartNew/{{data_interval_end.in_timezone("Asia/Seoul")|ds_nodash}}',
        file_name = 'TbCorona19VaccineStartNew.csv'
    )


    tb_corona19_count_status >> tb_corona19_vaccine_stat_new