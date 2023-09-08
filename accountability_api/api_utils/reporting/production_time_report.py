import base64
import json
import operator
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime
from functools import reduce
from pathlib import Path

import elasticsearch.exceptions
import pandas as pd
from flask import current_app
from pandas import DataFrame

from accountability_api.api_utils import query, metadata
from accountability_api.api_utils.reporting.report import Report
from accountability_api.api_utils.reporting.report_util import to_duration_isoformat, create_histogram

# Pandas options
pd.set_option("display.max_rows", None)  # control the number of rows printed
pd.set_option("display.max_columns", None)  # Breakpoint for truncate view. `None` value means unlimited.
pd.set_option("display.width", None)   # control the printed line length. `None` value will auto-detect the width.
pd.set_option("display.max_colwidth", 10)  # Number of characters to print per column.


class ProductionTimeReport(Report):
    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super().__init__(title, start_date, end_date, timestamp, **kwargs)
        self._report_options = kwargs["report_options"]

    def generate_report(self, output_format=None, report_type=None):
        current_app.logger.info(f"Generating report. {output_format=}, {self.__dict__=}")

        product_docs = []
        sds_product_indexes = reduce(operator.add, metadata.PRODUCT_TYPE_TO_INDEX.values())
        for sdp_product_index in sds_product_indexes:
            current_app.logger.info(f"Querying index {sdp_product_index} for products")

            try:
                product_docs += query.get_docs(indexes=[sdp_product_index], start=self.start_datetime, end=self.end_datetime)
            except elasticsearch.exceptions.NotFoundError as e:
                current_app.logger.warning(f"An exception {type(e)} occurred while querying indexes {sds_product_indexes} for products. Do the indexes exists?")

        if output_format == "application/zip":
            report_df = ProductionTimeReport.to_report_df(product_docs, report_type, self._report_options)

            # create zip. send zip.
            tmp_report_zip = tempfile.NamedTemporaryFile(suffix=".zip", dir=".", delete=True)
            with zipfile.ZipFile(tmp_report_zip.name, "w") as report_zipfile:
                # write histogram files, convert histogram column to filenames
                if self._report_options["generate_histograms"]:
                    for i in range(len(report_df)):
                        tmp_histogram = tempfile.NamedTemporaryFile(suffix=".png", dir=".", delete=True)
                        histogram_b64: str = report_df["histogram"].values[i]
                        tmp_histogram.write(base64.b64decode(histogram_b64))
                        tmp_histogram.flush()
                        histogram_filename = self.get_histogram_filename(
                            sds_product_name=report_df["opera_product_short_name"].values[i],
                            report_type=report_type)
                        report_zipfile.write(Path(tmp_histogram.name).name, arcname=histogram_filename)
                        report_df["histogram"].values[i] = histogram_filename

                ProductionTimeReport.rename_columns(report_df, report_type)
                report_csv = report_df.to_csv(index=False)
                report_csv = self.add_header_to_csv(report_csv, report_type)

                tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
                current_app.logger.info(f"{tmp_report_csv.name=}")
                tmp_report_csv.write(report_csv.encode("utf-8"))
                tmp_report_csv.flush()

                report_zipfile.write(Path(tmp_report_csv.name).name, arcname=self.get_filename_by_report_type("text/csv", report_type))
            return tmp_report_zip

        report_df = ProductionTimeReport.to_report_df(product_docs, report_type, self._report_options)

        if output_format == "text/csv":
            if self._report_options["generate_histograms"]:
                ProductionTimeReport.drop_column(report_df, "histogram")
            ProductionTimeReport.rename_columns(report_df, report_type)

            report_csv = report_df.to_csv(index=False)
            report_csv = self.add_header_to_csv(report_csv, report_type)

            tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
            tmp_report_csv.write(report_csv.encode("utf-8"))
            tmp_report_csv.flush()
            return tmp_report_csv
        elif output_format == "application/json" or output_format == "json":
            report_json = report_df.to_json(orient="records", date_format="epoch", lines=False, index=True)
            report_obj: list[dict] = json.loads(report_json)
            header = self.get_header(report_type)

            return json.dumps({
                "header": header,
                "payload": report_obj

            })
        elif output_format == "text/xml":
            return report_df.to_xml()
        elif output_format == "text/html":
            return report_df.to_html()
        else:
            raise Exception(f"output format ({output_format}) is not supported.")

    @staticmethod
    def to_report_df(product_docs: list[dict], report_type: str, report_options: dict) -> DataFrame:
        current_app.logger.info(f"Total generated products for report {len(product_docs)}")
        if not product_docs:
            return pd.DataFrame()

        # create initial data frame with raw report data
        product_type_to_production_times = defaultdict(list[dict])
        for product in product_docs:
            product_received_dt = datetime.fromisoformat(product["metadata"]["ProductReceivedTime"].removesuffix("Z"))
            product_received_ts = product_received_dt.timestamp()
            input_received_ts = product_received_ts

            daac_cnm_s_timestamp = product.get("daac_CNM_S_timestamp")
            if not daac_cnm_s_timestamp:
                daac_alerted_ts = None
                production_time_duration = None
            else:
                daac_alerted_ts = datetime.fromisoformat(daac_cnm_s_timestamp.removesuffix("Z")).timestamp()
                production_time_duration: float = daac_alerted_ts - input_received_ts

            if report_type == "detailed":
                production_time_dict = {
                    "opera_product_name": product["metadata"]["FileName"],
                    "opera_product_short_name": product["metadata"]["ProductType"],
                    "input_received_datetime": datetime.fromtimestamp(input_received_ts).isoformat(),
                    "daac_alerted_datetime": datetime.fromtimestamp(daac_alerted_ts).isoformat() if daac_alerted_ts else daac_alerted_ts,
                    "production_time": to_duration_isoformat(production_time_duration) if production_time_duration else production_time_duration
                }
            elif report_type == "summary":
                production_time_dict = {
                    "opera_product_name": product["metadata"]["FileName"],
                    "opera_product_short_name": product["metadata"]["ProductType"],
                    "input_received_datetime": input_received_ts,
                    "daac_alerted_datetime": daac_alerted_ts,
                    "production_time": production_time_duration
                }
            else:
                raise Exception(f"Unsupported report type. {report_type=}")
            product_type_to_production_times[product["metadata"]["ProductType"]].append(production_time_dict)
        if not product_type_to_production_times:
            return pd.DataFrame()

        if report_type == "detailed":
            # create data frame of raw data (log report)
            df_production_times_log = pd.DataFrame(reduce(operator.add, product_type_to_production_times.values()))
            return df_production_times_log
        elif report_type == "summary":
            # create data frame of aggregate data (summary report)
            production_time_summary_rows = []
            for product_type, production_times in product_type_to_production_times.items():
                # filter out NULL production times when aggregating
                df_production_times_summary_row = pd.DataFrame(production_times)
                df_production_times_summary_row = df_production_times_summary_row[
                    (
                        df_production_times_summary_row["production_time"].apply(lambda x: x is not None)
                    )
                ]

                # ignore NULL production times for histogram generation
                production_time_durations_hours = [t["production_time"] / 60 / 60 for t in production_times if t is not None]

                production_time_summary_row = {
                    "opera_product_short_name": df_production_times_summary_row["opera_product_short_name"].iloc[0],
                    "production_time_count": len(df_production_times_summary_row),
                    "production_time_min": to_duration_isoformat(df_production_times_summary_row["production_time"].min()),
                    "production_time_max": to_duration_isoformat(df_production_times_summary_row["production_time"].max()),
                    "production_time_mean": to_duration_isoformat(df_production_times_summary_row["production_time"].mean()),
                    "production_time_median": to_duration_isoformat(df_production_times_summary_row["production_time"].median())
                }
                if report_options["generate_histograms"]:
                    histogram = create_histogram(
                        series=production_time_durations_hours,
                        title=f'{df_production_times_summary_row["opera_product_short_name"].iloc[0]} Production Times',
                        metric="Production Time",
                        unit="hours")
                    production_time_summary_row.update({"histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")})

                production_time_summary_rows.append(production_time_summary_row)
            df_production_times_summary = pd.DataFrame(production_time_summary_rows)

            current_app.logger.info("Generated report")
            return df_production_times_summary
        else:
            raise Exception(f"Unsupported report type. {report_type=}")

    def add_header_to_csv(self, report_csv, report_type):
        header = self.get_header(report_type)
        header_str = ""
        for line in header:
            for k, v in line.items():
                header_str += f"{k}: {v}\n"
        report_csv = header_str + report_csv
        return report_csv

    def get_header(self, report_type):
        if report_type == "summary":
            header = self.get_header_summary()
        elif report_type == "detailed":
            header = self.get_header_detailed()
        else:
            raise Exception(f"{report_type=}")
        return header

    def get_header_detailed(self) -> list[dict[str, str]]:
        header = [
            {"Title": "OPERA Production Time Log"},
            {"Date of Report": datetime.fromisoformat(self._creation_time).strftime("%Y-%m-%dT%H:%M:%SZ")},
            {"Period of Coverage (AcquisitionTime)": f'{datetime.fromisoformat(self.start_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")}-{datetime.fromisoformat(self.end_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")}'},
        ]
        return header

    def get_header_summary(self) -> list[dict[str, str]]:
        header = [
            {"Title": "OPERA Production Time Summary"},
            {"Date of Report": datetime.fromisoformat(self._creation_time).strftime("%Y-%m-%dT%H:%M:%SZ")},
            {"Period of Coverage (AcquisitionTime)": f'{datetime.fromisoformat(self.start_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")} - {datetime.fromisoformat(self.end_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")}'}
        ]
        return header

    def get_filename_by_report_type(self, output_format, report_type):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        if output_format == "text/csv":
            return f"production-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.csv"
        elif output_format == "text/html":
            return f"production-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.html"
        elif output_format == "application/json":
            return f"production-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.json"
        elif output_format == "application/zip":
            return f"production-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.zip"
        else:
            raise Exception(f"Output format not supported. {output_format=}")

    def get_histogram_filename(self, sds_product_name, report_type):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        return f"production-time-{report_type} - {sds_product_name} - {start_datetime_normalized} to {end_datetime_normalized}.png"

    @staticmethod
    def rename_columns(report_df: DataFrame, report_type: str):
        if report_type == "summary":
            return ProductionTimeReport.rename_summary_columns(report_df)
        elif report_type == "detailed":
            return ProductionTimeReport.rename_detailed_columns(report_df)
        else:
            raise Exception(f"Unrecognized report type. {report_type=}")

    @staticmethod
    def rename_detailed_columns(report_df: DataFrame):
        report_df.rename(
            columns={
                "opera_product_name": "OPERA Product File Name",
                "opera_product_short_name": "OPERA Product Short Name",
                "input_received_datetime": "Input Received Datetime",
                "daac_alerted_datetime": "DAAC Alerted Datetime",
                "production_time": "Production Time"
            },
            inplace=True)

    @staticmethod
    def rename_summary_columns(report_df: DataFrame):
        report_df.rename(
            columns={
                "opera_product_short_name": "OPERA Product Short Name",
                "production_time_count": "Production Time (count)",
                "production_time_min": "Production Time (min)",
                "production_time_max": "Production Time (max)",
                "production_time_mean": "Production Time (mean)",
                "production_time_median": "Production Time (median)",
            },
            inplace=True)

    @staticmethod
    def drop_column(df: DataFrame, column):
        if column in df.columns:
            df.drop(columns=[column], inplace=True)

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


