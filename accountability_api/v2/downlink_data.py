from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query

api = Namespace("Downlink", path="/downlink", description="Listing Downlink Details")

parser = reqparse.RequestParser()
response_model = api.schema_model(
    name="downlink_response",
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
    "workflow_start",
    dest="workflow_start_datetime",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "workflow_end",
    dest="workflow_end_datetime",
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
    "vcid",
    dest="vcid",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid vcid.",
)
parser.add_argument(
    "ldf_filename",
    dest="ldf_filename",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid LDF filename.",
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
class Downlink(Resource):
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
        workflow_start = args.get("workflow_start_datetime", None)
        workflow_end = args.get("workflow_end_datetime", None)

        # other args
        obs_id = args.get("obs_id", None)
        vcid = args.get("vcid", None)
        ldf_filename = args.get("ldf_filename", None)
        size = args.get("size", 40)

        docs = query.get_downlink_data(
            work_start=workflow_start,
            work_end=workflow_end,
            start=start_datetime,
            end=end_datetime,
            size=size,
            time_key="last_modified",
            obs_id=obs_id,
            vcid=vcid,
            ldf_filename=ldf_filename,
        )
        return docs
