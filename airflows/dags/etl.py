"""Airflow DAG script """
import os
import configparser
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from operators.local_to_s3_upload import LocalToS3Operator
from operators.data_quality import DataQualityOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from airflow.models import Variable

default_args = {
    'owner': 'JoshuaAcosta',
    'start_date': datetime(2020, 9, 20),
    'depends_on_past':False,
    'retries': 0,
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(dag_id='ETL_311complaint_dag',
          default_args=default_args,
          description='Extract, load and stage NYC 311 complaints, \
                       weather and city demographics data',
          schedule_interval=None)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

upload_demographics_data = LocalToS3Operator(
    task_id='upload_demographics_data',
    dag=dag,
    path=os.environ['DATA_DIR'],
    extention="xlsx",
    folder="demographics",
    s3_bucket=Variable.get('aws_bucket'))


upload_complaint_data = LocalToS3Operator(
    task_id='upload_complaint_data',
    dag=dag,
    path=os.environ['DATA_DIR'],
    extention="csv",
    folder="311_complaints",
    s3_bucket=Variable.get('aws_bucket'))

get_weather_data = BashOperator(
    task_id='get_NOAA_weather_data',
    bash_command='python3 "${AIRFLOW_HOME}/dags/scripts/get_weather_data.py"',
    dag=dag)

transform_demo_data = BashOperator(
    task_id='transform_demo_data',
    bash_command='python3 "${AIRFLOW_HOME}/dags/scripts/transform_demo_data.py"',
    dag=dag
)

transform_weather_data = BashOperator(
    task_id='transform_weather_data',
    bash_command='python3 "${AIRFLOW_HOME}/dags/scripts/transform_weather_data.py"',
    dag=dag)

transform_complaint_data = BashOperator(
    task_id='transform_complaint_data',
    bash_command='python3 "${AIRFLOW_HOME}/dags/scripts/transform_complaint_data.py"',
    dag=dag)

load_weather_data = S3ToRedshiftTransfer(
    task_id='load_weather_data',
    schema='public',
    table='dim_weather',
    s3_bucket=Variable.get('aws_bucket'),
    s3_key='processed',
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=['csv', "IGNOREHEADER 1"],
    dag=dag)

load_demo_data = S3ToRedshiftTransfer(
    task_id='load_demo_data',
    schema='public',
    table='dim_demographics',
    s3_bucket=Variable.get('aws_bucket'),
    s3_key='processed',
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=['csv', 'IGNOREHEADER 1'],
    dag=dag)

load_time_data = S3ToRedshiftTransfer(
    task_id='load_time_data',
    schema='public',
    table='dim_time',
    s3_bucket=Variable.get('aws_bucket'),
    s3_key='processed',
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=['csv', "IGNOREHEADER 1"],
    dag=dag)

load_complaint_data = S3ToRedshiftTransfer(
    task_id='load_complaint_data',
    schema='public',
    table='fact_complaints',
    s3_bucket=Variable.get('aws_bucket'),
    s3_key='processed',
    redshift_conn_id='redshift_conn',
    aws_conn_id='aws_credentials',
    copy_options=['csv', "IGNOREHEADER 1"],
    dag=dag)

data_quality_query_one = "SELECT COUNT(*) FROM fact_complaints WHERE created_date is null"

data_quality_check_one = DataQualityOperator(
    task_id='data_quality_check_one',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality_check=data_quality_query_one)

data_quality_query_two = "SELECT COUNT(*) FROM fact_complaints WHERE community_board is null"

data_quality_check_two = DataQualityOperator(
    task_id='data_quality_check_two',
    dag=dag,
    redshift_conn_id="redshift_conn",
    data_quality_check=data_quality_query_two)

end_operator = DummyOperator(task_id='execution_complete', dag=dag)

start_operator >> [get_weather_data, upload_demographics_data, upload_complaint_data]
get_weather_data >> transform_weather_data
upload_demographics_data >> transform_demo_data
upload_complaint_data >> transform_complaint_data
transform_weather_data >> load_weather_data
transform_demo_data >> load_demo_data
transform_complaint_data >> load_time_data
[load_weather_data, load_demo_data, load_time_data] >> load_complaint_data
load_complaint_data >> [data_quality_check_one, data_quality_check_two] >> end_operator
