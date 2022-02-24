from builtins import object
import os


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class Config(object):
    SECRET_KEY = "secret key"
    ES_URL = "http://127.0.0.1:9200"  # default port is 9200


class ProductionConfig(Config):
    CACHE_TYPE = "null"


class DevelopmentConfig(Config):
    DEBUG = True
    DEBUG_TB_INTERCEPT_REDIRECTS = False

    CACHE_TYPE = "null"

    # This allows us to test the forms from WTForm
    WTF_CSRF_ENABLED = False
