import traceback
from elasticsearch import Elasticsearch

# from accountability_api.es_connection import get_grq_es, get_mozart_es
# GRQ_ES_URL = app.conf.get("GRQ_ES_URL", "http://localhost:9200") # "https://100.65.9.250/es/"
# JOBS_ES_URL = app.conf.get("JOBS_ES_URL", "http://localhost:9300") # "https://100.65.9.37/es/"

GRQ_ES = None
JOBS_ES = None


def connect_to_es(url):
    try:
        return Elasticsearch(url)
        # return Elasticsearch(url, http_auth=(user, pw), connection_class=RequestsHttpConnection, verify_certs=False)
    except Exception as e:
        print("Cannot connect to ElasticSearch at {}: {}".format(url, e))
        print(traceback.format_exc())


try:
    from hysds.celery import app
    from accountability_api.es_connection import get_grq_es, get_mozart_es

    GRQ_ES = get_grq_es()
    JOBS_ES = get_mozart_es(app.conf.get("JOBS_ES_URL"))
except Exception:
    print("Currently being run locally")

    GRQ_ES_URL = "localhost:9200"
    JOBS_ES_URL = "localhost:9300"

    GRQ_ES = connect_to_es(GRQ_ES_URL)
    JOBS_ES = connect_to_es(JOBS_ES_URL)
