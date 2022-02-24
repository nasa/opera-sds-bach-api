#!/bin/bash
# source $HOME/jpl/env/bin/activate
export PYTHONPATH=.:$PYTHONPATH

# deploy ES docker container
docker-compose down
sleep 2
docker-compose up -d
cd mozart_docker_compose
docker-compose down
sleep 2
docker-compose up -d
cd ..
# sleep 40
if [ $(uname -s) == "Darwin" ]; then
    echo "Make sure you have coreutils installed via 'brew install coreutils'"
    # brew install coreutils
    waiting="Waiting for Elasticsearch container to spin up"
    gtimeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9200/_cluster/health?wait_for_status=green&timeout=50s)" != "200" ]]; do sleep 5; echo "Waiting for Elasticsearch container to spin up"; done' || false
else
    timeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9200/_cluster/health?wait_for_status=green&timeout=50s)" != "200" ]]; do sleep 5; echo "Waiting for Elasticsearch container to spin up"; done' || false
fi
if [ $(uname -s) == "Darwin" ]; then
    echo "Make sure you have coreutils installed via 'brew install coreutils'"
    # brew install coreutils
    waiting="Waiting for Elasticsearch container to spin up"
    gtimeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9300/_cluster/health?wait_for_status=green&timeout=50s)" != "200" ]]; do sleep 5; echo "Waiting for Elasticsearch container to spin up"; done' || false
else
    timeout 300 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9300/_cluster/health?wait_for_status=green&timeout=50s)" != "200" ]]; do sleep 5; echo "Waiting for Elasticsearch container to spin up"; done' || false
fi
# load data into elasticsearch
pip install --no-deps -e .
# adding comment
python accountability_api/load_es_data.py $(pwd)/tests/grq_es_data/ grq
python accountability_api/load_es_data.py $(pwd)/tests/mozart_es_data/ mozart

# run linting and pep8 style check (configured by ../.flake8)
flake8 --output-file=/tmp/flake8.log

# run unit tests
pytest --ignore=lambda --junit-xml=/tmp/pytest_unit.xml

# run code coverage
# pytest --ignore=lambda --cov=accountability_api tests/ --cov-report=html:/tmp/coverage.html

cp /tmp/pytest_unit.xml .
cp /tmp/flake8.log .
# cp /tmp/coverage.html .

# shutting down docker images
docker-compose down
cd mozart_docker_compose
docker-compose down
cd ..