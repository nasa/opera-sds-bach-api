from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query

api = Namespace("Job", path="/job", description="Job related operations")

response_model = api.schema_model(
    name="Job_response",
    schema={
        "type": "object",
        "properties": {
            "metadata": {
                "type": "object",
                "properties": {
                    "payload_id": {"type": "string"},
                    "job_id": {"type": "string"},
                    "job_info.duration": {"type": "number"},
                    "job_info.execute_node": {"type": "string"},
                    "job_info.time_start": {"type": "string"},
                    "job_info.cmd_duration": {"type": "number"},
                    "job_info.job_queue": {"type": "string"},
                    "job_info.job_dir": {"type": "string"},
                    "status": {"type": "string"},
                    "workdir": {"type": "string"},
                    "error": {"type": "string"},
                    "short_error": {"type": "string"},
                    "traceback": {"type": "string"},
                },
                "required": ["payload_id", "job_id"],
            },
            "dataset": {"type": "string"},
        },
        "required": ["metadata", "dataset"],
    },
)


parser = reqparse.RequestParser()
parser.add_argument(
    "id",
    dest="job_id",
    type=str,
    location="args",
    required=False,
    help="Search by Job Payload ID.",
)
parser.add_argument(
    "uuid",
    dest="uuid",
    type=str,
    location="args",
    required=False,
    help="Search by Job UUID.",
)
parser.add_argument(
    "job_status",
    dest="job_status",
    type=str,
    location="args",
    required=False,
    help="Search by Job Status.",
)
parser.add_argument(
    "flatten",
    type=bool,
    default=True,
    help="Please provide if the result needs to be flattened.",
)


@api.route("/")
class Job(Resource):
    @api.expect(parser)
    @api.response(code=200, description="Job details dictionary", model=response_model)
    @api.response(code=400, description="Job not found")
    @api.response(code=500, description="Some error while retrieving Job")
    def get(self):
        """
        Get a job based on provided ID.
        Provide one of following. (Listed by Priority)
        <b>id</b>: Job Payload ID
        <b>uuid</b>: Job UUID
        <b>job_status</b>: Job Status

        <b>Constraint</b>: Querying all jobs is intentionally not implemented to prevent load on ES
        """
        args = parser.parse_args()
        if args.get("job_id"):
            doc = query.get_job(args.get("job_id"))
        elif args.get("uuid"):
            doc = query.get_job_by_uuid(args.get("uuid"))
        elif args.get("job_status"):
            doc = query.get_jobs_by_status(args.get("job_status"))
            return doc
        else:
            return {"message": "Provide a job_id or uuid"}, 400

        if args.get("flatten", True):
            result = query.flatten_doc(doc, skip_keys=["params"])
            if result.get("job.params", {}).get("runconfig"):
                result["runconfig"] = result.get("job.params").get("runconfig")
                del result["job.params"]
            return result

        return doc
