from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query

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

        time_key = "creation_time"
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

        return docs
