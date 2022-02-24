import logging

from accountability_api.configuration_obj import CeleryConfigObj, RabbitMQConfig
from flask_restx import Namespace, Resource

api = Namespace(
    "Configuration",
    path="/basicConfig",
    description="Underlying configuration mainly for frontend",
)
LOGGER = logging.getLogger(__name__)


@api.route("/")
class BasicConfig(Resource):
    def get(self):
        """
        Get configurations of Product Delivery Server
        """
        basic_config = CeleryConfigObj().get_setting_json()
        rabbit_mq_config = RabbitMQConfig()
        basic_config["rabbit_mq_domain"] = "{}:{}".format(
            rabbit_mq_config.domain, rabbit_mq_config.external_port
        )
        return basic_config, 200

    pass
