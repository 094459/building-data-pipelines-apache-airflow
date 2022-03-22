import boto3
from io import StringIO


download_http = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
s3_bucket_name = "mwaa-redshift-094459"
s3_key = "files-new/ml-latest-small/"
cleanKey="_cleanedup.csv"

#repeat for each folder - ratings, movies, links, tags

s3c = boto3.client('s3')
obj = s3c.get_object(Bucket=s3_bucket_name, Key=s3_key+"movies.csv/ml-latest-small/movies.csv")
infileStr=obj['Body'].read().decode('utf-8')
outfileStr=infileStr.replace('e', '') 
outfile = StringIO(outfileStr)
s3c.put_object(Bucket=s3_bucket_name, Key=s3_key+"movies.csv/ml-latest-small/movies"+cleanKey, Body=outfile.getvalue())
