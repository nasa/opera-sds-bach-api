from accountability_api.api_utils.reporting.production_time_report import ProductionTimeReport


class ProductionTimeSummaryReport(ProductionTimeReport):
    def generate_report(self, output_format=None, report_type=None):
        return ProductionTimeReport.generate_report(self, output_format=output_format, report_type="summary")
