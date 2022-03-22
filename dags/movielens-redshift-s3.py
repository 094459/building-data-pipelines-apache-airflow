from airflow import DAG

from airflow.operators.sensors import S3KeySensor, HttpSensor
from airflow.models import Variable

from airflow.operators.redshift_to_s3_operator import RedshiftToS3Transfer


from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False 
}

DAG_ID = os.path.basename(__file__).replace('.py', '')

# You will need to create the two connection properties in the Airflow UI
# - redshift_conn_id is used to configure the redshift connection details
# If you are using Redshift VPC Endpoints make sure you configure that not the Redshift cluster endpoint
# - aws_conn_id is used to define the aws credentials you will use
# You can use a newly created iam role, make sure it has the right access levels
#  - iterate with no access (default) until it works to keep least privilege

s3_bucket_name = Variable.get("s3_bucket_name", default_var="undefined")
s3_key = Variable.get("s3_key", default_var="undefined")
redshift_table_name = Variable.get("redshift_table_name", default_var="undefined")
redshift_airflow_connection = Variable.get("redshift_airflow_connection", default_var="undefined")
aws_connection = Variable.get("aws_connection", default_var="undefined")


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['s3','redshift'],

) as dag:  
    unload_to_S3 = RedshiftToS3Transfer(
    task_id='unload_to_S3',
    schema='public',
    table=redshift_table_name,
    s3_bucket=s3_bucket_name,
    s3_key=s3_key,
    redshift_conn_id=redshift_airflow_connection,
    unload_options = ['CSV'],
    aws_conn_id = aws_connection
  )
    unload_to_S3