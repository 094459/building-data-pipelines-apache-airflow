from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

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

# This bucket should exist as the DAG will not create and the workflow will fail
# You will need to create the files folder within that bucket
# You will also need to ensure your MWAA execution policy has access
## configure these as variables in Airflow

test_http = Variable.get("test_http", default_var="undefined")
download_http = Variable.get("download_http", default_var="undefined")
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

create_athena_movie_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ML_Latest_Small_Movies (
  `movieId` int,
  `title` string,
  `genres` string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_bucket_name}/{s3_key}ml-latest-small/movies.csv/ml-latest-small/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_bucket_name=s3_bucket_name, s3_key=s3_key)

create_athena_ratings_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ML_Latest_Small_Ratings (
  `userId` int,
  `movieId` int,
  `rating` int,
  `timestamp` bigint 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_bucket_name}/{s3_key}ml-latest-small/ratings.csv/ml-latest-small/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_bucket_name=s3_bucket_name, s3_key=s3_key)

create_athena_tags_table_query="""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.ML_Latest_Small_Tags (
  `userId` int,
  `movieId` int,
  `tag` int,
  `timestamp` bigint 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://{s3_bucket_name}/{s3_key}ml-latest-small/tags.csv/ml-latest-small/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
""".format(database=athena_db, s3_bucket_name=s3_bucket_name, s3_key=s3_key)

join_tables_athena_query="""
SELECT REPLACE ( m.title , '"' , '' ) as title, r.rating
FROM  {database}.ML_Latest_Small_Movies m
INNER JOIN (SELECT rating, movieId FROM {database}.ML_Latest_Small_Ratings WHERE rating > 4) r on m.movieId = r.movieId
""".format(database=athena_db)


def download_zip():
    s3c = boto3.client('s3', region_name="eu-west-1")
    indata = requests.get(download_http)
    n=0
    with zipfile.ZipFile(io.BytesIO(indata.content)) as z:       
        zList=z.namelist()
        print(zList)
        for i in zList: 
            print(i) 
            zfiledata = BytesIO(z.read(i))
            n += 1
            s3c.put_object(Bucket=s3_bucket_name, Key=s3_key+i+'/'+i, Body=zfiledata)

def clean_up_csv_fn(**kwargs):    
    ti = kwargs['task_instance']
    queryId = ti.xcom_pull(key='return_value', task_ids='join_athena_tables' )
    print(queryId)
    athenaKey=athena_results+"join_athena_tables/"+queryId+".csv"
    print(athenaKey)
    cleanKey=athena_results+"join_athena_tables/"+queryId+"_clean.csv"
    print(cleanKey)
    s3c = boto3.client('s3', region_name="eu-west-1")
    obj = s3c.get_object(Bucket=s3_bucket_name, Key=athenaKey)
    infileStr=obj['Body'].read().decode('utf-8')
    outfileStr=infileStr.replace('e', '') 
    outfile = StringIO(outfileStr)
    s3c.put_object(Bucket=s3_bucket_name, Key=cleanKey, Body=outfile.getvalue())

def s3_to_redshift(**kwargs):    
    ti = kwargs['task_instance']
    queryId = ti.xcom_pull(key='return_value', task_ids='join_athena_tables' )
    print(queryId)
    athenaKey='s3://'+s3_bucket_name+"/"+athena_results+"join_athena_tables/"+queryId+"_clean.csv"
    print(athenaKey)
    sqlQuery="copy "+redshift_table_name+" from '"+athenaKey+"' iam_role '"+redshift_iam_arn+"' CSV IGNOREHEADER 1;"
    print(sqlQuery)
    rsd = boto3.client('redshift-data', region_name="eu-west-1")
    resp = rsd.execute_statement(
        ClusterIdentifier=redshift_cluster,
        Database=redshift_db,
        #DbUser=redshift_dbuser,
        SecretArn=redshift_secret_arn,
        Sql=sqlQuery,
    )
    print(resp)
    return "OK"

def create_redshift_table():
    rsd = boto3.client('redshift-data', region_name="eu-west-1")
    resp = rsd.execute_statement(
        ClusterIdentifier=redshift_cluster,
        Database=redshift_db,
        DbUser=redshift_dbuser,
        Sql="CREATE TABLE IF NOT EXISTS "+redshift_table_name+" (title	character varying, rating	int);"
    )
    print(resp)
    return "OK"

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval=None,
    tags=['athena','redshift'],
) as dag:
    check_s3_for_key = S3KeySensor(
        task_id='check_s3_for_key',
        bucket_key=check_s3,
        wildcard_match=True,
        bucket_name=s3_bucket_name,
        aws_conn_id='aws_default',
        timeout=20,
        poke_interval=5,
        dag=dag
    )

    files_to_s3 = PythonOperator(
        task_id="files_to_s3",
        python_callable=download_zip
    )
    
    create_athena_movie_table = AWSAthenaOperator(
        task_id="create_athena_movie_table",
        query=create_athena_movie_table_query,
        database=athena_db,
        output_location='s3://'+s3_bucket_name+"/"+athena_results+'create_athena_movie_table'
        )
    
    create_athena_ratings_table = AWSAthenaOperator(
        task_id="create_athena_ratings_table",
        query=create_athena_ratings_table_query,
        database=athena_db,
        output_location='s3://'+s3_bucket_name+"/"+athena_results+'create_athena_ratings_table'
        )
    
    create_athena_tags_table = AWSAthenaOperator(
        task_id="create_athena_tags_table",
        query=create_athena_tags_table_query,
        database=athena_db,
        output_location='s3://'+s3_bucket_name+"/"+athena_results+'create_athena_tags_table'
        )
    
    join_athena_tables = AWSAthenaOperator(
        task_id="join_athena_tables",
        query=join_tables_athena_query,
        database=athena_db, 
        output_location='s3://'+s3_bucket_name+"/"+athena_results+'join_athena_tables'
        )
    
    create_redshift_table_if_not_exists = PythonOperator(
        task_id="create_redshift_table_if_not_exists",
        python_callable=create_redshift_table
    )

    clean_up_csv = PythonOperator(
        task_id="clean_up_csv",
        python_callable=clean_up_csv_fn,
        provide_context=True     
    )

    transfer_to_redshift = PythonOperator(
        task_id="transfer_to_redshift",
        python_callable=s3_to_redshift,
        provide_context=True     
    )

    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=s3_bucket_name,
        s3_key="athena-results/join_athena_tables/*_clean.csv",
        schema='PUBLIC',
        table=redshift_table_name,
        copy_options=['csv'],
        task_id='transfer_s3_to_redshift',
)


    #check_s3_for_key >> files_to_s3 >> create_athena_movie_table >> join_athena_tables >> clean_up_csv >> transfer_to_redshift
    #files_to_s3 >> create_athena_ratings_table >> join_athena_tables
    #files_to_s3 >> create_athena_tags_table >> join_athena_tables
    #files_to_s3 >> create_redshift_table_if_not_exists >> transfer_to_redshift

    task_transfer_s3_to_redshift