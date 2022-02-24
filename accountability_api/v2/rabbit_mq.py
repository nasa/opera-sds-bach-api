import json
import logging

import requests
from accountability_api.configuration_obj import RabbitMQConfig
from flask_restx import Namespace, Resource, reqparse

api = Namespace("Rabbit-MQ", path="/rabbitMQ", description="Rabbit MQ queries")

parser = reqparse.RequestParser()
parser.add_argument(
    "regex",
    type=str,
    location="args",
    default="true",
    help="boolean in a string form to allow upper/lowercase",
)
parser.add_argument(
    "name",
    type=str,
    location="args",
    required=True,
    help="text to query the name of rabbit MQ names. it allows REGEX",
)

LOGGER = logging.getLogger()
response_model = api.schema_model(
    name="ancillaryDataset_response",
    schema={
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "state": {"type": "string"},
                "type": {"type": "string"},
                "durable": {"type": "boolean"},
                "auto_delete": {"type": "boolean"},
                "expires": {"type": "integer"},
                "ttl": {"type": "integer"},
                "priority": {"type": "integer"},
            },
            "required": ["name", "state"],
        },
    },
)


@api.route("/")
class RabbitMQ(Resource):
    @staticmethod
    def __restructure_result(each):
        state = each["state"]
        if state == "running" and "idle_since" in each:
            state = "idle"
        arguments = each["arguments"] if "arguments" in each else None
        return {
            "name": each["name"],
            "state": state,
            "type": each["type"] if "type" in each else None,
            "durable": each["durable"],
            "auto_delete": each["auto_delete"],
            "expires": None
            if arguments is None or "x-expires" not in arguments
            else arguments["x-expires"],
            "ttl": None
            if arguments is None or "x-message-ttl" not in arguments
            else arguments["x-message-ttl"],
            "priority": None
            if arguments is None or "x-max-priority" not in arguments
            else arguments["x-max-priority"],
        }

    def get_rabbit_data(self, name, is_regex=False):
        """
        https://100.65.8.176:15673/api/queues?page=1&page_size=100&name=state&use_regex=false&pagination=true

        :return:
        """
        rabbit_mq_config = RabbitMQConfig()
        rabbit_mq_domain = rabbit_mq_config.get_base_url()
        if rabbit_mq_domain is None:
            raise ValueError("missing domain or base64 for rabbit mq")
        headers = {
            "Content-Type": "application/json",
        }
        if rabbit_mq_config.is_secure():
            rabbit_mq_base64 = rabbit_mq_config.get_base64_auth()
            if rabbit_mq_base64 is None:
                raise ValueError("missing domain or base64 for rabbit mq")
            headers["Authorization"] = "Basic {}".format(rabbit_mq_base64)
        current_pg = 1
        page_size = 500
        restructured_json = []

        while True:
            rabbit_mq_url = "{}/api/queues?page={}&page_size={}&pagination=true&use_regex={}&name={}".format(
                rabbit_mq_domain, current_pg, page_size, str(is_regex).lower(), name
            )
            res = requests.get(rabbit_mq_url, headers=headers, verify=False)
            if res.status_code != 200:
                raise ValueError(
                    "rabbit mq response is not 200. code: {}. message: {}".format(
                        res.status_code, res.content.decode()
                    )
                )
            res_json = json.loads(res.content.decode())
            for each in res_json["items"]:
                restructured_json.append(self.__restructure_result(each))
                pass
            if len(restructured_json) >= res_json["filtered_count"]:
                break
            current_pg += 1
            pass
        return restructured_json

    @api.expect(parser)
    @api.response(
        code=200,
        description="Rabbit MQ results in JSON dictionary",
        model=response_model,
    )
    @api.response(code=500, description="Some error while retrieving Rabbit MQ")
    def get(self):
        """
        Rabbit MQ names and states query

        """
        args = parser.parse_args()
        try:
            return (
                self.get_rabbit_data(
                    args.get("name"), args.get("regex").strip().lower() == "true"
                ),
                200,
            )
        except Exception as e:
            LOGGER.exception("error while getting rabbit-mq data")
            return {"message": "cannot retrieve rabbit mq. {}".format(str(e))}, 500
