from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue  import GlueJobOperator
from airflow.models import Variable
from project_2_utils import validate_streaming_files, prepare_files_for_job_run, archive_and_cleanup, cleanup_on_failure


default_args = {
    'owner': 'Marzuk',
    'email': ['marzuk.entsie@amalitech.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

now = datetime.now()
project_vars = Variable.get("project-two-vars", deserialize_json=True)
bucket = project_vars.get("bucket")
source_key = project_vars.get("source_path")

with DAG(
    'music_streaming_etl2',
    default_args=default_args,
    description='ETL pipeline for music streaming data analysis',
    schedule_interval=None,
    start_date=datetime(2025, 3, 18),
    catchup=False,
    tags=['music', 'streaming', 'aws', 'dynamodb'],
) as dag:

    start = S3KeySensor(
        task_id='wait_for_streaming_files_task',
        bucket_name=bucket,
        bucket_key=f"{source_key}*.csv",
        wildcard_match=True,
        deferrable=True,
        poke_interval=1800, # Check every 30 minutes
        timeout=86400, # Wait for 24 hours 
        dag=dag
    )

    validate = PythonOperator(
        task_id='validate_streaming_files_task',
        python_callable=validate_streaming_files,
        dag=dag
    )

    prepare = PythonOperator(
        task_id='prepare_files_for_job_run_task',
        python_callable=prepare_files_for_job_run,
        dag=dag
    )

    run_job = GlueJobOperator(
        task_id='run_glue_job_task',
        job_name='streaming_transformation',
        script_location="s3://aws-glue-assets-288761743948-eu-west-1/scripts/streaming_transformation.py",
        script_args={
        '--JOB_NAME': 'streaming_transformation',
        '--s3_songs_path': "s3://etl-airflow-glue-bucket-zuki/data/songs/songs.csv",
        '--s3_users_path': "s3://etl-airflow-glue-bucket-zuki/data/users/users.csv",
        '--s3_streams_path': "s3://etl-airflow-glue-bucket-zuki/data/temp/merged_stream.csv",
        '--target_dynamodb_table': "daily-genre-kpis"
        },
        aws_conn_id='aws_default',
        dag=dag
    )

    archive_cleanup = PythonOperator(
        task_id='archive_and_cleanup_task',
        python_callable=archive_and_cleanup,
        # perform retry
        retries=1,
        dag=dag
    )

    cleanup_on_failure = PythonOperator(
        task_id='cleanup_on_failure_task',
        python_callable=cleanup_on_failure,
        trigger_rule='one_failed',
        dag=dag
    )

    end = DummyOperator(
        task_id='end_pipeline',
        dag=dag
    )

    start >> validate >> prepare >> run_job >> archive_cleanup >> end
    [validate, prepare, run_job, archive_cleanup] >> cleanup_on_failure
    cleanup_on_failure >> end