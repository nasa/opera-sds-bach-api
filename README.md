# bach-api
API to support the Accountability UIs for OPERA with HySDS

# Getting started

## Prerequisites

1. Git.
2. Python (see .python-version).
4. A fork of the repo (for any contributions).
5. A clone of the `opera-sds-bach-api` repo.

## Installation

1. Create a python virtual environment.
    1. RECOMMENDED: move `pip.conf` into the resulting `venv/` directory.
1. Activate the virtual environment and run `pip install -e .` from the project root to install the workspace dependencies.

# Running locally

1. Run Elasticsearch (On Mozart. See [opera-sds-pcm project homepage](https://github.com/nasa/opera-sds-pcm)).
1Under the project root, create a `celeryconfig.py` file. See snippet below. Update placeholders as needed.

   ```python
   # celeryconfig.py
   
   JOBS_ES_URL = "http://127.0.0.1:9200"
   
   GRQ_AWS_ES = False
   GRQ_ES_HOST = "127.0.0.1"
   AWS_REGION = "us-west-2"
   GRQ_ES_URL = "https://<mozart_ip>/grq_es/"
   
   GRQ_ES_USER = "<user>"
   GRQ_ES_PWD = "<password>"
   ```
1. Run either the following commands to start the development server. 
    * Flask development mode:
      These commands guarantee development mode is fully enabled, in addition to the interactive debugger and reloader.
      See the official Flask documentation for how this works (https://flask.palletsprojects.com/en/2.0.x/cli/)
    ```bash
    export FLASK_ENV=development
    export FLASK_APP="accountability_api:create_app('accountability_api.settings.DevelopmentConfig')"
    flask run -p 8875
    ```
    * External debugger (IDE), run `python run_debug.py`.
      See Flask documentation (https://flask.palletsprojects.com/en/2.0.x/debugging/)
1. Make API calls to endpoints under `http://localhost:8875/`

## Files required to run in `docker`

The following files are required to run `opera-sds-bach-api` in docker. Refer to the `docker run` command in this document for where the app expects these files.

### Sample `app.conf.ini`
    [default]
    FLASK_HOST = 0.0.0.0
    FLASK_PORT = 8080
    FLASK_APP_NAME = ACCOUNTABILITY API
    DEBUG = False
    PDR_SERVER = http://137.78.112.40:5000
    LATENCY_STG_MAX_SECOND =10800
    PROD_ACC_PRODS_COUNT = 5
    RABIT_MQ_PROTOCOL = https
    RABIT_MQ_REQUIRED_AUTH = True
    VENUE = local
    JOB_CONTAINER_NAME = container-sds-accountability_accountability-sciflo:core-v3.0.1
    swagger_base = /api

    [LOGGING]
    LOG_LEVEL = INFO
    LOG_INTERVAL_HOUR = 12
    LOG_BACKUP_COUNT = 30

    [dev]
    DEBUG = True
    
    [production]
    DEBUG = False
    FLASK_HOST = 0.0.0.0
    FLASK_PORT = 8080
    FLASK_APP_NAME = ACCOUNTABILITY API
    PDR_SERVER = http://137.78.112.40:5000
    LOG_LEVEL = INFO

#### `app.conf.ini` Explanation
| Name | Description | Default Value |
|---|---|---|
| in `default` profile  |
| FLASK_HOST | [0.0.0.0] in DEV, used to launch Flask  | NA. mandatory|
| FLASK_PORT | [integer] in DEV, used to launch Flask | NA. mandatory|
| FLASK_APP_NAME | NOT IN USED |NA|
| DEBUG | [boolean] in DEV, flag when launching Flask |NA. mandatory|
| PDR_SERVER | PDR [protocol://server:port] to get the PDR config page which is displayed in UI & capture the DAAC names and other metadata in API |NA. mandatory|
| LATENCY_STG_MAX_SECOND | [integer] seconds used in calculating "%-under-max-time" in Latency Report |seconds for 3 hour|
| PROD_ACC_PRODS_COUNT | [integer] in Half-Orbit-Workflow, replace total number of products in a workflow. When count total products from the index |count from the ES index|
| RABIT_MQ_PROTOCOL | [http or https] protocol used when querying rabbit-mq |https|
| RABIT_MQ_REQUIRED_AUTH | [boolean] whether rabbit-mq needs auth info from `.netrc` |True|
| VENUE | [string] to display in UI |NA. mandatory|
| JOB_CONTAINER_NAME | [string] to filter workflow rows in "Workflow Monitor"|NA. mandatory|
| swagger_base | [string] to prepend swagger-ui files|empty string|
| in `LOGGING` profile  |
| LOG_LEVEL | [Python Log Levels in Uppercase] |INFO|
| LOG_INTERVAL_HOUR | [integer] |12|
| LOG_BACKUP_COUNT | [integer] |30|
| in `dev` profile  |
| DEBUG | [boolean] |NA. mandatory|
| in `production` profile  |
| DEBUG | [boolean] |NA. mandatory|
| FLASK_HOST | [0.0.0.0] |NA. mandatory|
| FLASK_PORT | [integer] |NA. mandatory|

### `.netrc`
    machine 127.0.0.1 login <rabbit-mq-username> password <rabbit-mq-password>
    macdef init
- the permissions of `.netrc` needs to be `400`

### `celeryconfig.py`
- using the existing config from factotum machine. 
- keys in use

| Name | Usage | Note |
|---|---|---|
|GRQ_ES_URL|to retrieve GRQ Elasticsearch URL|
|JOBS_ES_URL|to retrieve Mozart / Jobs Elasticsearch URL|
|PYMONITOREDRUNNER_CFG|to retrieve IP and port of Rabbit-MQ|Current port is shown as 5672 while it should be 15672|
|MOZART_URL|to extract Mozart IP|
|TOSCA_URL|to extract GRQ IP|
|REDIS_INSTANCE_METRICS_URL|to extract Kibana IP|

#### Assumptions
External Rabbit-MQ port is internal Rabbit-MQ port retrieved from `celeryconfig.py` + 1

### Directory `logs` to keep log files

#### `docker` command to start the server
        docker run --rm --name <docker_ps_name> \
          -v /absolute/path/to/.netrc:/home/ops/.netrc:z  \
          -v /absolute/path/to/celeryconfig.py:/home/ops/accountability-ui-api/accountability_api/celeryconfig.py:ro  \
          -v /absolute/path/to/app.conf.ini:/home/ops/accountability-ui-api/accountability_api/app.conf.ini:ro  \
          -v /absolute/path/to/logs:/home/ops/accountability-ui-api/logs:z  \
          -d --net="host" <image-name>:<tag> ;

### NOTES
- This project requires `boto3` and `botocore`. But they are not part of the installation instruction as `hysds/pge-base:v3.0.0-rc.4` already has them installed.
- This project assumes aws key and secret are set in default location. 

### Sample Deployment Script
- in `factotum` machine home directory `/export/home/hysdsops`, create `accountability-ui-api`. `mkdir accountability-ui-api`
- go to `accountability-ui-api`: `cd /export/home/hysdsops/accountability-ui-api`
- create `deploy-script.sh` with the follwing content. Don't forget to change permissions `chmod 755 deploy-script.sh`

        #! /bin/bash
        
        docker_ps_name=accountability-ui-api-instance
        cd /export/home/hysdsops/accountability-ui-api ;
        rm -f accountability-ui-api-release-nrt-bug-fix-1.tar ;  # s3 filename
        aws s3 cp s3://accountability-dev-code-bucket-2/api/release-nrt-bug-fix-1/accountability-ui-api-release-nrt-bug-fix-1.tar . ;  # s3 url
        docker load -i accountability-ui-api-release-nrt-bug-fix-1.tar ;
        
        docker stop ${docker_ps_name} && docker rm ${docker_ps_name} ;  # stopping the current process. NOTE: docker rm may print an error
        sleep 5 ;  # making sure that the process is stopeed
        docker run --rm --name ${docker_ps_name} \  # running the script from the above section
          -v /export/home/hysdsops/.netrc:/home/ops/.netrc:z  \
          -v /export/home/hysdsops/accountability-ui-api/sample_celeryconfig.py:/home/ops/accountability-ui-api/accountability_api/celeryconfig.py:ro  \
          -v /export/home/hysdsops/accountability-ui-api/app.conf.ini:/home/ops/accountability-ui-api/accountability_api/app.conf.ini:ro  \
          -v /export/home/hysdsops/accountability-ui-api/logs:/home/ops/accountability-ui-api/logs:z  \
          -d --net="host" accountability-ui-api:release-nrt-bug-fix-1 ;
- NOTE: the script may need some updates if the S3 URL is updated or docker name & tag is changed
- run the script 
