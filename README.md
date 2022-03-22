# Apache Airflow

This repo contains resources used during presentation and demos given at various events where I show you how to get started with Apache Airflow and build a data pipeline.

### Getting started with Apache Airflow

To get Apache Airflow up and running on your local developer environment, try the following:

> Note! Pre-reqs for this is that you have Docker Compose and a Docker Engine running in your developer environment. Check the pre-reqs [here](https://github.com/aws/aws-mwaa-local-runner#prerequisites)

```
git clone https://github.com/aws/aws-mwaa-local-runner.git
cd aws-mwaa-local-runner
./mwaa-local-env build-image
./mwaa-local-env start
```

>**Suggested configuration changes for demo purposes**
>
>Before building the image using the above steps, I made a change to the airflow.cfg file (in docker/config) to change the amount of time Apache Airflow refreshed the DAGS folder to pick up new DAGs. The default is 300 seconds (5 mins) and I changed this to 10 seconds. This does put extra load on my demo machine, but makes demos much easier.
>


The first time you run this, it might take some time as it downloads all the needed Docker images. Once it has finished, you should see this appear in the terminal

> After the first time, it should start straight away

```
local-runner_1  |   ____________       _____________
local-runner_1  |  ____    |__( )_________  __/__  /________      __
local-runner_1  | ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
local-runner_1  | ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
local-runner_1  |  _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
local-runner_1  | [2022-02-23 11:04:02,780] {{dagbag.py:451}} INFO - Filling up the DagBag from /dev/null
local-runner_1  | [2022-02-23 11:04:05 +0000] [187] [INFO] Starting gunicorn 19.10.0
local-runner_1  | [2022-02-23 11:04:05 +0000] [187] [INFO] Listening at: http://0.0.0.0:8080 (187)
local-runner_1  | [2022-02-23 11:04:05 +0000] [187] [INFO] Using worker: sync
local-runner_1  | [2022-02-23 11:04:05 +0000] [190] [INFO] Booting worker with pid: 190
local-runner_1  | [2022-02-23 11:04:05 +0000] [191] [INFO] Booting worker with pid: 191
local-runner_1  | [2022-02-23 11:04:05 +0000] [192] [INFO] Booting worker with pid: 192
local-runner_1  | [2022-02-23 11:04:05 +0000] [193] [INFO] Booting worker with pid: 193
local-runner_1  | [2022-02-23 11:04:35 +0000] [187] [INFO] Handling signal: ttin
local-runner_1  | [2022-02-23 11:04:35 +0000] [225] [INFO] Booting worker with pid: 225
```

You can now access the Apache Airflow UI by accessing http://localhost:8080 on your browser, and logging in using the admin/test username and password.


### Building our Data Pipeline

Follow the instructions in [this blog post](https://dev.to/aws/working-with-the-redshifttos3transfer-operator-and-amazon-managed-workflows-for-apache-airflow-56n9) to build the environment and create the DAG which will be our data pipeline.

The original post was done using Apache Airflow 1.12, and the code within this Repo is updated to use Apache Airflow 2.0.2, and fix a typo in the original clean script.






