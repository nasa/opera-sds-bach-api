import re
from datetime import datetime
from importlib import import_module

from accountability_api.api_utils.reporting.report import Report

# from .observation_accountability_report import ObservationAccountabilityReport


class ReportsGenerator:
    def __init__(self, start_date, end_date, mime="json"):
        self._start = start_date
        self._end = end_date
        self._output_format = mime

    def generate_report(
        self, report_name, report_type="", output_format=None, **kwargs
    ):
        # first, we need to convert this report_name to the proper module
        # assuming its the same as the class name
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module("accountability_api.api_utils.reporting." + module_path)
        cls = None

        try:
            cls = getattr(module, report_name)
        except Exception:
            raise Exception("Could not find %s" % report_name)

        if not issubclass(cls, Report):
            raise Exception("%s is not of subclass Report" % report_name)

        detailed = False
        if report_type == "brief" or report_type == "detailed":
            detailed = report_type == "detailed"

        report = cls(
            report_name,
            self._start,
            self._end,
            datetime.utcnow().isoformat(),
            report_type=report_type,
            detailed=detailed,
            **kwargs
        )
        if output_format is not None and output_format != self._output_format:
            self._output_format = output_format

        result = report.generate_report(output_format=self._output_format)
        self.filename = report.get_filename(self._output_format)
        return result
