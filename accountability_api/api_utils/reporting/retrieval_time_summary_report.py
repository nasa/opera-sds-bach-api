from accountability_api.api_utils.reporting.retrieval_time_report import RetrievalTimeReport


class RetrievalTimeSummaryReport(RetrievalTimeReport):
    def generate_report(self, output_format=None, report_type=None):
        return RetrievalTimeReport.generate_report(self, output_format=output_format, report_type="summary")
