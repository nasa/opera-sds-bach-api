#!/usr/bin/env bash


docker run -v /export/home/hysdsops/verdi/ops/hysds/celeryconfig.py:/home/ops/accountability-ui-api/accountability_api/celeryconfig.py:ro -d --net="host" accountability-ui-api:master
