from __future__ import division

import traceback
from dataclasses import dataclass

from flask import request, make_response, current_app, send_file
from flask_restx import Namespace, Resource, reqparse, fields

from accountability_api.api_utils.reporting.reports_generator import ReportsGenerator

api = Namespace("Reports", path="/reports", description="Report related operations")

report_types = {
    "ObservationAccountabilityReport": "NISAR Observation Accountability Report for MS",
    "IncomingFiles": "NISAR Incoming Files report",
    "GeneratedSdsProducts": "NISAR SDS Generated Products",
    "DaacOutgoingProducts": "Daac Outgoing Products Report",
    "DataAccountabilityReport": "NISAR report combining GeneratedSdsProducts, DaacOutgoingProducts, and DaacOutgoingProducts reports",
}


@dataclass
class BachApiException(Exception):
    request: dict


@dataclass
class ReportGenerationException(BachApiException):
    pass


@dataclass
class NamedReportGenerationException(ReportGenerationException):
    report_name: str


@dataclass
class TypedReportGenerationException(ReportGenerationException):
    report_type: str


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
        "enableHistograms": fields.String
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
parser.add_argument("enableHistograms", type=str, default="false", choices=["false", "true"], location="args")


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
        current_app.logger.info(f"Report requested. {args=}")

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

        self._report_options = {
            "generate_histograms": args["enableHistograms"] == "true"
        }

        try:
            reports_generator = ReportsGenerator(self._start, self._end, mime=self._mimetype)
            report = reports_generator.generate_report(
                reportName,
                report_type=self._report_type,
                output_format=self._mimetype,
                processing_mode=self._processing_mode,
                venue=self._venue,
                crid=self._crid,
                report_options=self._report_options
            )

            current_app.logger.info(f"{self._mimetype=}")

            if self._mimetype == "application/zip":
                if not report:
                    return make_response('', 204)
                return send_file(report.name, as_attachment=True, download_name=reports_generator.filename)
            if self._mimetype == "text/csv":
                return send_file(report.name, as_attachment=True, download_name=reports_generator.filename)
            if self._mimetype == "image/png":
                if not report:
                    return make_response('', 204)
                return send_file(report.name, as_attachment=False)

            return make_response(report)
        except Exception as e:
            current_app.logger.exception(f"error while generating report: {reportName}")
            raise NamedReportGenerationException(
                report_name=reportName,
                request={"reportName": reportName, "params": {**args}}
            ) from e

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
            current_app.logger.exception(f"error while generating report: {reportType}")
            raise TypedReportGenerationException(
                report_type=reportType,
                request={"reportType": reportType, "payload": payload}
            ) from e
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


@api.errorhandler(NamedReportGenerationException)
def handle_named_report_generation_exception(error: NamedReportGenerationException):
    return {
        "type": "https://opera.jpl.nasa.gov/probs/named-report",
        "title": f"Cannot generate {error.report_name}",
        "status": 500,
        "detail": "Please try again.",

        "traceback": traceback.format_exc(),

        "request": error.request
    }, 500


@api.errorhandler(TypedReportGenerationException)
def handle_typed_report_generation_exception(error: TypedReportGenerationException):
    return {
        "type": "https://opera.jpl.nasa.gov/probs/typed-report",
        "title": f"Cannot generate {error.report_type}",
        "status": 500,
        "detail": "Please try again.",

        "traceback": traceback.format_exc(),

        "request": error.request
    }, 500


@api.errorhandler(BachApiException)
def handle_bach_api_exception(error: BachApiException):
    return {
        "type": "https://opera.jpl.nasa.gov/probs/bach-api",
        "title": "Something went unexpectedly wrong",
        "status": 500,
        "detail": "Please try again.",

        "traceback": traceback.format_exc(),

        "request": error.request
    }, 500


@api.errorhandler(Exception)
def handle_root_exception(error: Exception):
    return {
        # Problem Details (RFC 7807)
        "type": "https://opera.jpl.nasa.gov/probs/root",
        "title": "Something went unexpectedly wrong",
        "status": 500,
        "detail": "Please try again.",

        "traceback": traceback.format_exc(),
    }, 500