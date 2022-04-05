from __future__ import division

import logging

from flask import request, make_response
from flask_restx import Namespace, Resource, reqparse, fields

from accountability_api.api_utils.reporting.reports_generator import ReportsGenerator

api = Namespace("Reports", path="/reports", description="Report related operations")
LOGGER = logging.getLogger(__name__)

report_types = {
    "ObservationAccountabilityReport": "NISAR Observation Accountability Report for MS",
    "IncomingFiles": "NISAR Incoming Files report",
    "GeneratedSdsProducts": "NISAR SDS Generated Products",
    "DaacOutgoingProducts": "Daac Outgoing Products Report",
    "DataAccountabilityReport": "NISAR report combining GeneratedSdsProducts, DaacOutgoingProducts, and DaacOutgoingProducts reports",
}


@api.route("/")
class Reports(Resource):
    def get(self):
        """
        Getting Report Names
        """
        return {
            "status": "OK",
            "code": 200,
            "message": "Success!",
            "result": {"Report Types": list(report_types.keys())},
        }, 200


reportModel = api.model(
    "Report",
    {
        "name": fields.String,
        "startDateTime": fields.DateTime,
        "endDateTime": fields.DateTime,
        "mime": fields.String,
        "reportType": fields.String,
        "daac": fields.String,
        "crid": fields.String,
        "processingMode": fields.String,
        "venue": fields.String,
    },
)

parser = reqparse.RequestParser()
parser.add_argument("startDateTime", type=str, location="args")
parser.add_argument("endDateTime", type=str, location="args")
parser.add_argument("mime", type=str, location="args")
parser.add_argument("reportType", type=str, location="args")
parser.add_argument("daac", type=str, default="all", location="args")
parser.add_argument("crid", type=str, default="", location="args")
parser.add_argument("processingMode", type=str, default="", location="args")
parser.add_argument("venue", type=str, default="local", location="args")


def makeResponse(data, status="OK", code=200, message="Success!", result_json=None):
    return {
        "status": status,
        "code": code,
        "message": message,
        "result": data,
        "result_json": result_json,
        "headers": {},
    }, code


@api.route("/<reportName>")
class CreateReport(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)
        self._report_type = None
        self._start = None
        self._end = None
        self._prod_type = None
        self._processing_mode = None
        self._venue = None
        self._crid = None

    @api.expect(parser)
    def get(self, reportName):
        """
        Get detailed Reports
        """
        args = parser.parse_args()

        self._report_type = ""

        if "reportType" in args:
            self._report_type = args["reportType"]

        if "processingMode" in args:
            self._processing_mode = args["processingMode"]

        if "venue" in args:
            self._venue = args["venue"]

        if "crid" in args:
            self._crid = args["crid"]

        self._start = args["startDateTime"]
        self._end = args["endDateTime"]
        self._mimetype = args["mime"]

        try:
            reports_generator = ReportsGenerator(self._start, self._end, mime=self._mimetype)
            report = reports_generator.generate_report(
                reportName,
                report_type=self._report_type,
                output_format=self._mimetype,
                processing_mode=self._processing_mode,
                venue=self._venue,
                crid=self._crid
            )
            filename = reports_generator.filename
            response = make_response(report)

            response.headers["content-type"] = self._mimetype
            response.headers["filename"] = filename
            LOGGER.info(f"return report: {filename}")
            return response
        except Exception as e:
            LOGGER.exception(f"error while generating report: {reportName}")
            return {
                "message": f"cannot generate {reportName}",
                "details": str(e),
            }, 500

    @api.expect(reportModel)
    @api.response(202, "Accepted: Report has been accepted for processing.")
    @api.response(400, "Bad Request: Malformed post body.")
    @api.response(404, "Not Found: Report does not exist.")
    def post(self, reportType):
        """
        Get detailed Reports
        """
        payload = request.get_json()
        self._report_type = reportType
        self._start = payload["startDateTime"]
        self._end = payload["endDateTime"]
        self._prod_type = payload.get("productType", None)
        try:
            gen_report, json_data = self.__get_report()
        except Exception as e:
            LOGGER.exception(f"error while generating report: {reportType}")
            return {
                "message": f"cannot generate {reportType}",
                "details": str(e),
            }, 500
        if gen_report is None:
            return makeResponse(
                None, status="Not Found", message="Report does not exist", code=404
            )
        makeResponse(
            gen_report,
            "Accepted",
            202,
            "Report has been accepted for processing.",
            result_json=json_data,
        )
