import requests
import boto3
import zipfile
import io
from io import BytesIO

download_http = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
s3_bucket_name = "mwaa-redshift-094459"
s3_key = "files-new/"

s3c = boto3.client('s3')
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