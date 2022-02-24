from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query

api = Namespace(
    "TrackFrame", path="/track-frame", description="Listing Track Frame Details"
)

parser = reqparse.RequestParser()
response_model = api.schema_model(
    name="track_frame_response",
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
    "cycle_sec",
    dest="cycle_sec",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "cycle_ref",
    dest="cycle_ref",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "track",
    dest="track",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid ISO UTC datetime.",
)
parser.add_argument(
    "frame",
    dest="frame",
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
    "l0b_rrsd_id",
    dest="l0b_rrsd_id",
    type=str,
    location="args",
    required=False,
    help="Please provide a valid Datatake ID.",
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
class TrackFrame(Resource):
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

        # other args
        cycle_sec = args.get("cycle_sec", None)
        cycle_ref = args.get("cycle_ref", None)
        track = args.get("track", None)
        frame = args.get("frame", None)
        obs_id = args.get("obs_id", None)
        l0b_rrsd_id = args.get("l0b_rrsd_id", None)

        # time_key = "created_at"
        # args = parser.parse_args()
        docs = query.get_track_frame_data(
            cycle_sec=cycle_sec,
            cycle_ref=cycle_ref,
            track=track,
            frame=frame,
            obs_id=obs_id,
            l0b_rrsd_id=l0b_rrsd_id,
        )

        return docs
