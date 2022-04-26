from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from io import StringIO
from io import BytesIO
from time import sleep
import csv
import requests
import json
import boto3
import zipfile
import io
import sys
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False 
}

DAG_ID = os.path.basename(__file__).replace('.py', '')

s3_bucket_name = Variable.get("s3_bucket_name", default_var="undefined")
s3_key = Variable.get("s3_key", default_var="undefined")
check_s3 = s3_key+"readme.txt"

redshift_cluster = Variable.get("redshift_cluster", default_var="undefined")
redshift_db = Variable.get("redshift_db", default_var="undefined") 
redshift_dbuser = Variable.get("redshift_dbuser", default_var="undefined")
redshift_table_name = Variable.get("redshift_table_name", default_var="undefined")
redshift_iam_arn = Variable.get("redshift_iam_arn", default_var="undefined")
redshift_secret_arn = Variable.get("redshift_secret_arn", default_var="undefined")

athena_db = Variable.get("demo_athena_db", default_var="undefined")
athena_results = Variable.get("athena_results", default_var="undefined")

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['athena','redshift'],
) as dag:
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=s3_bucket_name,
        s3_key="athena-results/join_athena_tables/edfa1038-fbd1-4517-ab8c-0bb17204d05f_clean.csv",
        schema='PUBLIC',
        redshift_conn_id='redshift_default',
        table=redshift_table_name,
        copy_options=['csv IGNOREHEADER 1'],
        task_id='transfer_s3_to_redshift',
)


    task_transfer_s3_to_redshift