import base64
import io
import statistics
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import current_app
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandas import DataFrame, Timedelta

from accountability_api.api_utils import query
from accountability_api.api_utils.reporting.report import Report

# Pandas options
pd.set_option('display.max_rows', None)  # control the number of rows printed
pd.set_option('display.max_columns', None)  # Breakpoint for truncate view. `None` value means unlimited.
pd.set_option('display.width', None)   # control the printed line length. `None` value will auto-detect the width.
pd.set_option('display.max_colwidth', 10)  # Number of characters to print per column.


class ProductionTimeReport(Report):
    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super(ProductionTimeReport, self).__init__(title, start_date, end_date, timestamp, **kwargs)

    def generate_report(self, output_format=None, report_type=None):
        current_app.logger.info(f"Generating report. {output_format=}, {self.__dict__=}")

        generated_products = query.get_docs(indexes=["grq_1_l3_dswx_hls"], start=self.start_datetime, end=self.end_datetime)

        if output_format == "application/zip":
            report_df = ProductionTimeReport.es_to_report_df(generated_products, report_type)

            tmp_report_zip = tempfile.NamedTemporaryFile(suffix=".zip", dir=".", delete=True)
            with tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True) as tmp_report_csv:
                current_app.logger.info(f"{tmp_report_csv.name=}")

                tmp_report_csv.write(report_df.to_csv().encode("utf-8"))
                tmp_report_csv.flush()

                # create zip. send zip.
                with zipfile.ZipFile(tmp_report_zip.name, "w") as report_zipfile:
                    report_zipfile.write(Path(tmp_report_csv.name).name, arcname=self.get_filename("text/csv"))

                    # write histogram files, convert columns to filenames
                    for i, row in report_df.iterrows():
                        tmp_histogram = tempfile.NamedTemporaryFile(suffix=".png", dir=".", delete=True)
                        histogram_b64: str = report_df.at[i, 'histogram']
                        tmp_histogram.write(base64.b64decode(histogram_b64))
                        tmp_histogram.flush()
                        report_zipfile.write(Path(tmp_histogram.name).name, arcname=self.get_filename("image/png"))
                        report_df.at[i, 'histogram'] = Path(tmp_histogram.name).name
            return tmp_report_zip

        report_df = ProductionTimeReport.es_to_report_df(generated_products, report_type)

        if output_format == "text/csv":
            tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
            tmp_report_csv.write(report_df.to_csv().encode("utf-8"))
            tmp_report_csv.flush()
            return tmp_report_csv
        elif output_format == "application/json" or output_format == "json":
            return report_df.to_json(orient='records', date_format='epoch', lines=False, index=True)
        elif output_format == "text/xml":
            return report_df.to_xml()
        elif output_format == "text/html":
            return report_df.to_html()
        else:
            raise Exception(f"output format ({output_format}) is not supported.")

    @staticmethod
    def es_to_report_df(generated_products_es: list[dict], report_type: str) -> DataFrame:
        current_app.logger.info(f"Total generated products for report {len(generated_products_es)}")
        if not generated_products_es:
            return pd.DataFrame(generated_products_es)

        # create initial data frame with raw report data
        production_times: list[dict] = []
        for product in generated_products_es:
            if not product.get("daac_CNM_S_timestamp"):
                current_app.logger.info(f"No CNM-S data. skipping product. {product=}")
                continue

            product_received_dt = datetime.fromisoformat(product["metadata"]["ProductReceivedTime"].removesuffix("Z"))
            product_received_ts = product_received_dt.timestamp()
            input_received_ts = product_received_ts

            daac_alerted_ts = datetime.fromisoformat(product["daac_CNM_S_timestamp"].removesuffix("Z")).timestamp()
            production_time_duration: float = daac_alerted_ts - input_received_ts

            production_time = {
                "OPERA Product File Name": product["metadata"]["FileName"],
                "OPERA Product Short Name": product["metadata"]["ProductType"],
            }
            if report_type == "detailed":
                production_time.update({
                    "InputReceivedDateTime": datetime.fromtimestamp(input_received_ts).isoformat(),
                    "DaacAlertedDateTime": datetime.fromtimestamp(daac_alerted_ts).isoformat(),
                    "ProductionTime": ProductionTimeReport.to_duration_isoformat(production_time_duration)
                })
            elif report_type == "summary":
                production_time.update({
                    "InputReceivedDateTime": input_received_ts,
                    "DaacAlertedDateTime": daac_alerted_ts,
                    "ProductionTime": production_time_duration
                })
            else:
                raise Exception(f"Unsupported report type. {report_type=}")
            production_times.append(production_time)

        if report_type == "detailed":
            # create data frame of raw data (log report)
            df_production_times_log = pd.DataFrame(production_times)
            return df_production_times_log
        elif report_type == "summary":
            # create data frame of aggregate data (summary report)
            df_production_times_summary = pd.DataFrame(production_times)
            production_time_durations = [x["ProductionTime"] for x in production_times]
            histogram = ProductionTimeReport.create_histogram(production_time_durations, df_production_times_summary["OPERA Product Short Name"].iloc[0])

            df_production_times_summary = pd.DataFrame([{
                "OPERA Product Short Name": df_production_times_summary["OPERA Product Short Name"].iloc[0],
                "ProductionTime (count)": df_production_times_summary.size,
                "ProductionTime (min)": ProductionTimeReport.to_duration_isoformat(df_production_times_summary["ProductionTime"].min()),
                "ProductionTime (max)": ProductionTimeReport.to_duration_isoformat(df_production_times_summary["ProductionTime"].max()),
                "ProductionTime (mean)": ProductionTimeReport.to_duration_isoformat(df_production_times_summary["ProductionTime"].mean()),
                "ProductionTime (median)": ProductionTimeReport.to_duration_isoformat(df_production_times_summary["ProductionTime"].median()),
                "histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")
            }])

            current_app.logger.info("Generated report")
            return df_production_times_summary
        else:
            raise Exception(f"Unsupported report type. {report_type=}")

    @staticmethod
    def create_histogram(times_seconds: list[float], title: str) -> io.BytesIO:
        current_app.logger.info(f"{len(times_seconds)=}")
        times_hours = [sec / 60 / 60 for sec in times_seconds]

        fig = Figure(layout='tight')
        ax: Axes = fig.subplots()
        ax.hist(times_hours, bins=len(times_hours))

        xticks = [
                # 0,
                # numpy.percentile(a=times_hours, q=90),
                statistics.mean(times_hours),
                # 24
            ]

        # extreme edge case where only 1 product has been generated
        if len(times_hours) >= 2:
            xticks = xticks + [min(*times_hours), max(*times_hours)]

        ax.set(
            title=f"{title} Production Times",
            xlabel="Production Time (hours)", xticks=xticks, xticklabels=[f"{x:.2f}" for x in xticks],
            yticks=[], yticklabels=[])

        ax.axvline(statistics.mean(times_hours), color='k', linestyle='dashed', linewidth=1, alpha=0.5)
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                     ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize('xx-small')
        histogram_img = io.BytesIO()
        fig.savefig(histogram_img, format="png")

        current_app.logger.info("Generated histogram")
        return histogram_img

    @staticmethod
    def to_duration_isoformat(duration_seconds: float):
        td: Timedelta = pd.Timedelta(f'{int(duration_seconds)} s')
        hh = 24 * td.components.days + td.components.hours
        hhmmss_format = f"{hh:02d}:{td.components.minutes:02d}:{td.components.seconds:02d}"
        return hhmmss_format

    def get_filename(self, output_format):
        if output_format == "text/csv":
            return f"production-time - {self.start_datetime} to {self.end_datetime}.csv"
        elif output_format == "text/html":
            return f"production-time - {self.start_datetime} to {self.end_datetime}.html"
        elif output_format == "application/json":
            return f"production-time - {self.start_datetime} to {self.end_datetime}.json"
        elif output_format == "image/png":
            return f"production-time - {self.start_datetime} to {self.end_datetime}.png"
        elif output_format == "application/zip":
            return f"production-time - {self.start_datetime} to {self.end_datetime}.zip"
        else:
            raise Exception(f"Output format not supported. {output_format=}")

    def populate_data(self):
        raise Exception

    def get_data(self):
        raise Exception

    def to_json(self):
        raise Exception

    def get_dict_format(self):
        raise Exception

    def to_xml(self):
        raise Exception

    def to_csv(self):
        raise Exception
