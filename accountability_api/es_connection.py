from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from elasticsearch import RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from hysds.celery import app

MOZART_ES = None
GRQ_ES = None


def get_mozart_es(es_url, logger=None):
    global MOZART_ES
    if MOZART_ES is None:
        MOZART_ES = ElasticsearchUtility(es_url, logger)
    return MOZART_ES


def get_grq_es(logger=None) -> ElasticsearchUtility:
    global GRQ_ES

    if GRQ_ES is None:
        aws_es = app.conf.get("GRQ_AWS_ES", False)

        if aws_es is True:
            es_host = app.conf["GRQ_ES_HOST"]
            es_url = app.conf["GRQ_ES_URL"]
            region = app.conf["AWS_REGION"]

            aws_auth = BotoAWSRequestsAuth(
                aws_host=es_host, aws_region=region, aws_service="es"
            )
            GRQ_ES = ElasticsearchUtility(
                es_url=es_url,
                logger=logger,
                http_auth=aws_auth,
                connection_class=RequestsHttpConnection,
                use_ssl=True,
                verify_certs=False,
                ssl_show_warn=False,
            )
        else:
            es_url = app.conf["GRQ_ES_URL"]
            GRQ_ES = ElasticsearchUtility(
                es_url=es_url,
                logger= logger,
                # NOTE: devs adjust this locally to connect to own Elasticsearch.
                # http_auth=(app.conf["GRQ_ES_USER"], app.conf["GRQ_ES_PWD"]),
                # connection_class=RequestsHttpConnection,
                # use_ssl=True,
                # verify_certs=False,
                # ssl_show_warn=False,
            )
    return GRQ_ES
