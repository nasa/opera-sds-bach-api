from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query

api = Namespace("Ancillary-Dataset", path="/index", description="General Index")

response_model = api.schema_model(
    name="index_response",
    schema={
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "dataset_type": {
                    "type": "string",
                },
                "count": {"type": "integer"},
                "children": {
                    "type": "array",
                    "minItems": 0,
                    "maxItems": 0,
                },
            },
            "required": ["dataset_type", "count", "children"],
        },
    },
)

parser = reqparse.RequestParser()
parser.add_argument(
    "startDateTime", type=str, location="args", help="ISO-8601 style date time string"
)
parser.add_argument(
    "endDateTime", type=str, location="args", help="ISO-8601 style date time string"
)


@api.route("/", defaults={"path": "ancillary"})
@api.route("/<path:path>")
class Index(Resource):
    @api.expect(parser)
    def get(self, path):
        """
        Created to return all docs in an index. Implemented to return the aggregates of ancillary dataset types
        """
        args = parser.parse_args()
        start_time, end_time = args["startDateTime"], args["endDateTime"]
        body = {
            "query": {},
            "aggs": {
                "dataset_types": {"terms": {"field": "dataset.raw", "size": 10000}}
            },
        }
        if not (query.is_null(start_time) and query.is_null(end_time)):
            body["query"]["bool"] = {
                "must": [
                    {
                        "range": query.construct_range_object(
                            "ProductReceivedTime", start_time, end_time
                        )
                    }
                ]
            }
        else:
            body["query"]["match_all"] = {}
        result = query.run_query(index=path, size=0, body=body)
        return [
            {"dataset_type": i["key"], "count": i["doc_count"], "children": []}
            for i in result["aggregations"]["dataset_types"]["buckets"]
        ]
