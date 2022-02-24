from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query, metadata, processing

api = Namespace(
    "Observation", path="/observation", description="Listing Observation Details"
)

parser = reqparse.RequestParser()
response_model = api.schema_model(
    name="observation_response",
    schema={
        "type": "array",
        "items": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
)

parser.add_argument(
    "id",
    dest="product_id",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid product ID.",
)
parser.add_argument(
    "start",
    dest="start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "end",
    dest="end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "obs_start",
    dest="obs_start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "obs_end",
    dest="obs_end_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "observation_id",
    dest="obs_id",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid Observation ID.",
)
parser.add_argument(
    "datatake_id",
    dest="dt_id",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid Datatake ID.",
)
parser.add_argument(
    "processing_type",
    dest="processing_type",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid processing_type.",
)
parser.add_argument(
    "cycle",
    dest="cycle",
    type=str,
    location="args",
    required=False,
    help="Please provide an existing cycle",
)
parser.add_argument(
    "size",
    default=40,
    dest="size",
    type=int,
    location="args",
    required=False,
    help="Please provide a valid size.",
)


@api.route("/")
class Observation(Resource):
    @api.expect(parser)
    @api.response(
        code=200,
        description="LDF results in JSON dictionary List",
        model=response_model,
    )
    @api.response(code=500, description="Some error while retrieving Half Orbits")
    def get(self):
        """
        List all half orbits

        Provide one of following. (Listed by Priority)

        """
        args = parser.parse_args()

        # datetime args
        start_datetime = args.get("start_datetime", None)
        end_datetime = args.get("end_datetime", None)
        obs_start = args.get("obs_start_datetime", None)
        obs_end = args.get("obs_end_datetime", None)

        # other args
        obs_id = args.get("obs_id", None)
        dt_id = args.get("dt_id", None)
        processing_type = args.get("processing_type", None)
        cycle = args.get("cycle", None)

        time_key = "created_at"
        # args = parser.parse_args()
        docs = query.get_obs_data(
            obs_start=obs_start,
            obs_end=obs_end,
            start=start_datetime,
            end=end_datetime,
            time_key=time_key,
            obs_id=obs_id,
            dt_id=dt_id,
            processing_type=processing_type,
            cycle=cycle,
        )

        # grab the L0B doc and add all relevent info.
        for doc in docs:
            l0b_product = {}
            if "L0B_L_RRSD_id" in doc:
                l0b_product = query.get_product(
                    doc["L0B_L_RRSD_id"], index=metadata.PRODUCT_INDEXES["L0B_L_RRSD"]
                )
            important_product_data = processing.get_l0b_info(l0b_product)
            doc.update(important_product_data)
            if doc["ProcessingType"] == "not found":
                if "processing_type" in doc and doc["processing_type"]:
                    if doc["processing_type"] == "nominal":
                        doc["ProcessingType"] = "PR"
                    elif doc["processing_type"] == "urgent":
                        doc["ProcessingType"] = "PU"
            if len(doc["L0A_L_RRST_ids"]) > 0:
                for l0a in doc["L0A_L_RRST_ids"]:
                    downlink_query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "_index": metadata.ACCOUNTABILITY_INDEXES[
                                                "DOWNLINK"
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    downlink_query = query.add_query_match(
                        query=downlink_query, field_name="L0A_L_RRST_id", value=l0a
                    )
                    downlink_entry = query.run_query(
                        index=metadata.ACCOUNTABILITY_INDEXES["DOWNLINK"],
                        sort="last_modified:desc",
                        size=40,
                        body=downlink_query,
                    )
                    hits = downlink_entry["hits"]["hits"]
                    if len(hits) > 0:
                        l0a_status = hits[0]["_source"]["L0A_L_RRST_status"]
                        if "L0A_L_RRST_status" not in doc:
                            doc["L0A_L_RRST_status"] = l0a_status
                        elif l0a_status != "job-completed":
                            doc["L0A_L_RRST_status"] = l0a_status

        # seperate observations
        docs = processing.seperate_observations(docs)

        return docs
