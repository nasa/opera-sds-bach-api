import logging

from accountability_api.api_utils.aws.s3 import AWSS3
from flask_restx import Namespace, Resource, reqparse
from flask import stream_with_context, Response

api = Namespace(
    "Product-Text-File", path="/FileContent", description="Product Text File Content"
)

parser = reqparse.RequestParser()
parser.add_argument("S3HttpUrl", type=str, location="args", help="http url of S3")
LOGGER = logging.getLogger()


@api.route("/")
class LogFileContent(Resource):
    @api.expect(parser)
    @api.response(code=200, description="File stream")
    @api.response(code=500, description="Some error while retrieving file stream")
    def get(self):
        """
        Get all records of a specific ancillary dataset type

        <b>Constraint</b>: Size is maxed at 100k. Use date range to get all results

        <b>Possible enhancement</b>: pagination / scrolling
        """
        args = parser.parse_args()
        s3_http_url = args["S3HttpUrl"]
        try:
            s3_client = AWSS3().from_http_url(s3_http_url)
            return Response(stream_with_context(s3_client.get_stream()))
        except Exception as e:
            logging.exception(
                "error while getting the stream of log file {}".format(s3_http_url)
            )
            return {"message": "cannot get contents. cause: {}".format(str(e))}, 500
