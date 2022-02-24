import base64
import logging
import netrc
import os
from urllib.parse import urlparse

from configparser import ConfigParser

from accountability_api.singleton_base import Singleton

try:
    from hysds.celery import app
except Exception:
    print("can't find app")

LOGGER = logging.getLogger()


class ConfigurationObj(metaclass=Singleton):
    def __init__(self):
        config_file = "app.conf.ini"
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_file = os.path.join(current_path, config_file)
        if not os.path.isfile(config_file):
            print("Config file {} not found".format(config_file))
            exit(1)
        temp_configs = ConfigParser()
        temp_configs.read(config_file)
        self.__config = temp_configs

    def get_item(self, key, profile="default", default=None):
        return self.__config.get(profile, key, fallback=default)


class RabbitMQConfig(metaclass=Singleton):
    def __init__(self):
        self._base_domain = None
        self._port = None
        self._external_port = None
        self._protocol = None
        self._is_secure = True
        self._base_url = self.__get_rabbit_mq_base_url()
        self._base64_auth = self.__get_rabbit_mq_auth()

    @property
    def domain(self):
        return self._base_domain

    @property
    def external_port(self):
        return self._external_port

    def is_secure(self):
        return self._is_secure

    def get_base64_auth(self):
        return self._base64_auth

    def get_base_url(self):
        return self._base_url

    def __get_rabbit_mq_base_url(self):
        config = ConfigurationObj()
        self._protocol = config.get_item("RABIT_MQ_PROTOCOL", default="https")
        self._is_secure = (
            config.get_item("RABIT_MQ_REQUIRED_AUTH", default="True").strip().lower()
            == "true"
        )
        base_rabbit_mq = app.conf.get("PYMONITOREDRUNNER_CFG")
        if base_rabbit_mq is None:
            LOGGER.info("PYMONITOREDRUNNER_CFG not in celery config")
            return None
        if "rabbitmq" not in base_rabbit_mq:
            LOGGER.info("rabbitmq not in PYMONITOREDRUNNER_CFG")
            return None
        base_rabbit_mq = base_rabbit_mq["rabbitmq"]
        if "hostname" not in base_rabbit_mq or "port" not in base_rabbit_mq:
            LOGGER.info("hostname or port not in rabbitmq")
            return None
        self._base_domain = base_rabbit_mq["hostname"]
        self._port = int(base_rabbit_mq["port"])
        self._external_port = int(base_rabbit_mq["port"]) + 1  # TODO confirm the logic
        return "{}://{}:{}".format(
            self._protocol, base_rabbit_mq["hostname"], base_rabbit_mq["port"]
        )

    @staticmethod
    def __get_rabbit_mq_auth():
        try:
            simple_net_rc = netrc.netrc()
            plain_auth = simple_net_rc.authenticators("127.0.0.1")
            if len(plain_auth) < 3:
                LOGGER.info("127.0.0.1 not in netrc.netrc")
                return None
            return base64.standard_b64encode(
                "{}:{}".format(plain_auth[0], plain_auth[2]).encode()
            ).decode()
        except:
            LOGGER.exception(
                "error while attempting to get authenticator for rabbit-mq"
            )
            return None


class CeleryConfigObj(metaclass=Singleton):
    def __init__(self):
        self._mozart = None
        self._grq = None
        self._kibana = None
        self._venue = ConfigurationObj().get_item("VENUE", default="unknown")
        self.__set_mozart()
        self.__set_grq()
        self.__set_kibana()

    @staticmethod
    def __get_domain(input_url, split_port=True):
        try:
            parsed_uri = urlparse(input_url)
            parsed_domain = parsed_uri.netloc
            if not split_port or ":" not in parsed_domain:
                return parsed_domain
            return parsed_domain.split(":")[0]
        except:
            return ""

    def __set_mozart(self):
        raw_mozart = app.conf.get("MOZART_URL", "")
        if raw_mozart == "":
            self._mozart = "UNDEFINED"
            return
        attempted_domain = self.__get_domain(raw_mozart)
        self._mozart = attempted_domain if attempted_domain != "" else "UNDEFINED"
        return

    def __set_grq(self):
        raw_grq = app.conf.get("TOSCA_URL", "")
        if raw_grq == "":
            self._grq = "UNDEFINED"
            return
        attempted_domain = self.__get_domain(raw_grq)
        self._grq = attempted_domain if attempted_domain != "" else "UNDEFINED"
        return

    def __set_kibana(self):
        raw_kibana = app.conf.get("REDIS_INSTANCE_METRICS_URL", "")
        if raw_kibana == "":
            self._kibana = "UNDEFINED"
            return
        parsed_kibana = self.__get_domain(raw_kibana, False)
        if "@" in parsed_kibana:
            parsed_kibana = parsed_kibana.split("@")[1]
        self._kibana = parsed_kibana if parsed_kibana != "" else "UNDEFINED"
        return

    def get_setting_json(self):
        return {
            "mozart": self._mozart,
            "grq": self._grq,
            "kibana": self._kibana,
            "venue": self._venue,
        }
