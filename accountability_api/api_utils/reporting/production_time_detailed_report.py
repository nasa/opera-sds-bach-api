from accountability_api.api_utils.reporting.production_time_report import ProductionTimeReport


class ProductionTimeDetailedReport(ProductionTimeReport):
    def generate_report(self, output_format=None, report_type=None):
        return ProductionTimeReport.generate_report(self, output_format=output_format, report_type="detailed")

    def get_filename(self, output_format):
        return ProductionTimeReport.get_filename_by_report_type(self, output_format, report_type="detailed")
