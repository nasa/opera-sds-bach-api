from hysds.celery import app
from accountability_api.es_connection import get_grq_es, get_mozart_es

GRQ_ES = get_grq_es()
JOBS_ES = get_mozart_es(app.conf.get("JOBS_ES_URL"))