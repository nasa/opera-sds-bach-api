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
from accountability_api.api_utils.metadata import SDS_PRODUCT_TYPE_TO_INPUT_PRODUCT_TYPES
from accountability_api.api_utils.reporting.report import Report

# Pandas options
pd.set_option('display.max_rows', None)  # control the number of rows printed
pd.set_option('display.max_columns', None)  # Breakpoint for truncate view. `None` value means unlimited.
pd.set_option('display.width', None)   # control the printed line length. `None` value will auto-detect the width.
pd.set_option('display.max_colwidth', 10)  # Number of characters to print per column.


class RetrievalTimeReport(Report):
    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super(RetrievalTimeReport, self).__init__(title, start_date, end_date, timestamp, **kwargs)

    def generate_report(self, output_format=None, report_type=None):
        current_app.logger.info(f"Generating report. {output_format=}, {self.__dict__=}")

        try:
            input_products_l30 = query.get_docs(indexes=["grq_1_l2_hls_l30"], start=self.start_datetime, end=self.end_datetime)
        except:  # index not found
            input_products_l30 = []
        try:
            input_products_s30 = query.get_docs(indexes=["grq_1_l2_hls_s30"], start=self.start_datetime, end=self.end_datetime)
        except:  # index not found
            input_products_s30 = []
        input_products_all = input_products_l30 + input_products_s30

        if output_format == "application/zip":
            report_df = RetrievalTimeReport.es_to_report_df(input_products_all, report_type, start=self.start_datetime, end=self.end_datetime)

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

        report_df = RetrievalTimeReport.es_to_report_df(input_products_all, report_type, start=self.start_datetime, end=self.end_datetime)

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
    def es_to_report_df(generated_products_es: list[dict], report_type: str, start, end) -> DataFrame:
        current_app.logger.info(f"Total generated products for report {len(generated_products_es)}")
        if not generated_products_es:
            return pd.DataFrame(generated_products_es)

        # group products by filename, group products by granule
        product_name_to_product_map = RetrievalTimeReport.map_by_name(generated_products_es)
        granule_to_products_map = RetrievalTimeReport.map_by_granule(generated_products_es)

        RetrievalTimeReport.augment_with_hls_spatial_info(granule_to_products_map, start, end)
        RetrievalTimeReport.augment_with_hls_info(product_name_to_product_map, start, end)

        # create initial data frame with raw report data
        retrieval_times: list[dict] = []
        for product in generated_products_es:
            current_app.logger.debug(f'{product["_id"]=}')

            if not product.get("hls") or not product.get("hls_spatial"):
                current_app.logger.warning("HLS info unavailable. Did you skip query + download jobs?")

            if not product.get("hls"):  # possible in dev when skipping download job by direct file upload
                product_received_dt = datetime.fromisoformat(product["metadata"]["ProductReceivedTime"].removesuffix("Z"))
                product_received_ts = product_received_dt.timestamp()
            else:
                product_received_dt = datetime.fromisoformat(product["hls"]["download_datetime"].removesuffix("Z"))
                product_received_ts = product_received_dt.timestamp()
            current_app.logger.debug(f"{product_received_dt=!s}")

            if not product.get("hls"):  # possible in dev when skipping download job by direct file upload
                opera_detect_dt = product_received_dt
                opera_detect_ts = opera_detect_dt.timestamp()
            else:
                # add PublicAvailableDateTime information
                opera_detect_dt = datetime.fromisoformat(product["hls"]["query_datetime"].removesuffix("Z"))
                opera_detect_ts = opera_detect_dt.timestamp()
            current_app.logger.debug(f"{opera_detect_dt=!s}")

            if not product.get("hls_spatial"):  # possible in dev when skipping download job by direct file upload
                public_available_dt = opera_detect_dt
                public_available_ts = public_available_dt.timestamp()
            else:
                public_available_dt = datetime.fromisoformat(product["hls_spatial"]["production_datetime"].removesuffix("Z"))
                public_available_ts = public_available_dt.timestamp()
            current_app.logger.debug(f"{public_available_dt=!s}")

            retrieval_time = product_received_ts - public_available_ts
            current_app.logger.debug(f"{retrieval_time=:,.0f} (seconds)")

            retrieval_time_dict = {
                "OPERA Product File Name": product["metadata"]["FileName"],
                "ProductType": product["metadata"]["ProductType"],
            }
            if report_type == "detailed":
                retrieval_time_dict.update({
                    "PublicAvailableDateTime": datetime.fromtimestamp(public_available_ts).isoformat(),
                    "OperaDetectDateTime": datetime.fromtimestamp(opera_detect_ts).isoformat(),
                    "ProductReceivedDateTime": datetime.fromtimestamp(product_received_ts).isoformat(),
                    "RetrievalTime": RetrievalTimeReport.to_duration_isoformat(retrieval_time)
                })
            elif report_type == "summary":
                retrieval_time_dict.update({
                    "RetrievalTime": retrieval_time
                })
            else:
                raise Exception(f"Unsupported report type. {report_type=}")
            retrieval_times.append(retrieval_time_dict)
            current_app.logger.debug("---")

        if report_type == "detailed":
            # create data frame of raw data (log report)
            df_retrieval_times_log = pd.DataFrame(retrieval_times)
            return df_retrieval_times_log
        elif report_type == "summary":
            # create data frame of aggregate data (summary report)
            df_retrieval_times_summary = pd.DataFrame(retrieval_times)
            product_types = df_retrieval_times_summary["ProductType"].unique()
            current_app.logger.debug(f"{product_types=}")

            current_app.logger.info("Processing recognized product types")
            df_retrieval_times_summary_entries = []
            for sds_product_type, input_product_types in SDS_PRODUCT_TYPE_TO_INPUT_PRODUCT_TYPES.items():
                current_app.logger.info(f"{sds_product_type=}")

                input_product_types_processed = []
                for input_product_type in input_product_types:
                    current_app.logger.debug(f"{input_product_type=}")

                    df_retrieval_times_summary_input_product_type = df_retrieval_times_summary[df_retrieval_times_summary['ProductType'].apply(lambda x: x == input_product_type)]
                    current_app.logger.debug(f"Found {len(df_retrieval_times_summary_input_product_type)} {input_product_type} products")

                    if not len(df_retrieval_times_summary_input_product_type):
                        current_app.logger.debug("0 products. Skipping to next input product type")
                        continue
                    input_product_types_processed.append(input_product_type)

                    retrieval_times = df_retrieval_times_summary_input_product_type["RetrievalTime"].to_numpy()
                    histogram = RetrievalTimeReport.create_histogram(retrieval_times, input_product_type)

                    df_retrieval_times_summary_input_product_type = pd.DataFrame([{
                        "OPERA Product Short Name": sds_product_type,  # e.g. L3_DSWX_HLS
                        "Input Product Short Name": input_product_type,  # e.g. L2_HLS_L30
                        "RetrievalTime (count)": len(df_retrieval_times_summary_input_product_type),
                        "RetrievalTime (P90)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_product_type["RetrievalTime"].quantile(q=0.9)),
                        "RetrievalTime (min)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_product_type["RetrievalTime"].min()),
                        "RetrievalTime (max)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_product_type["RetrievalTime"].max()),
                        "RetrievalTime (mean)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_product_type["RetrievalTime"].mean()),
                        "RetrievalTime (median)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_product_type["RetrievalTime"].median()),
                        "histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")
                    }])
                    df_retrieval_times_summary_entries.append(df_retrieval_times_summary_input_product_type)

                # filter by SDS product type to combine aggregations for their respective input product types
                # prevent redundant ALL row when only 1 input product type was processed
                if len(input_product_types_processed) > 1:
                    current_app.logger.info(f"Creating ALL entry")

                    df_retrieval_times_summary_input_all_product_types = df_retrieval_times_summary[df_retrieval_times_summary['ProductType'].apply(lambda x: x in input_product_types)]
                    current_app.logger.debug(f"Found {len(df_retrieval_times_summary_input_product_type)} {input_product_types} products")

                    retrieval_times = df_retrieval_times_summary_input_all_product_types["RetrievalTime"].to_numpy()
                    histogram = RetrievalTimeReport.create_histogram(retrieval_times, input_product_type)

                    df_retrieval_times_summary_input_all_product_types = pd.DataFrame([{
                        "OPERA Product Short Name": sds_product_type,  # e.g. L3_DSWX_HLS
                        "Input Product Short Name": "ALL",  # e.g. ALL = L2_HLS_L30 + L2_HLS_L30
                        "RetrievalTime (count)": len(df_retrieval_times_summary_input_all_product_types),
                        "RetrievalTime (P90)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_all_product_types["RetrievalTime"].quantile(q=0.9)),
                        "RetrievalTime (min)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_all_product_types["RetrievalTime"].min()),
                        "RetrievalTime (max)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_all_product_types["RetrievalTime"].max()),
                        "RetrievalTime (mean)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_all_product_types["RetrievalTime"].mean()),
                        "RetrievalTime (median)": RetrievalTimeReport.to_duration_isoformat(df_retrieval_times_summary_input_all_product_types["RetrievalTime"].median()),
                        "histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")
                    }])
                    df_retrieval_times_summary_entries.append(df_retrieval_times_summary_input_all_product_types)

            df_retrieval_times_summary = pd.concat(df_retrieval_times_summary_entries)

            current_app.logger.info("Generated report")
            return df_retrieval_times_summary
        else:
            raise Exception(f"Unsupported report type. {report_type=}")

    @staticmethod
    def augment_with_hls_info(product_name_to_product_map, start, end):
        current_app.logger.info("Adding HLS information to products")

        hls_docs: list[dict] = query.get_docs(indexes=["hls_catalog"], start=start, end=end)
        for hls_doc in hls_docs:
            hls_doc_id = hls_doc["_id"]  # filename
            product_name = hls_doc_id[0:len(hls_doc_id) - 1 - hls_doc_id[::-1].index(".")]  # strip extension to get product name
            product = product_name_to_product_map.get(product_name, {})
            product["hls"] = hls_doc

    @staticmethod
    def augment_with_hls_spatial_info(granule_to_products_map, start, end):
        current_app.logger.info("Adding HLS spatial information to products")

        hls_spatial_docs: list[dict] = query.get_docs(indexes=["hls_spatial_catalog"], start=start, end=end)
        for hls_spatial_doc in hls_spatial_docs:
            granule_id = hls_spatial_doc_id = hls_spatial_doc["_id"]  # filename minus extension minus band (i.e. granule)
            for product in granule_to_products_map.get(granule_id, []):
                product["hls_spatial"] = hls_spatial_doc

    @staticmethod
    def map_by_granule(generated_products_es):
        granule_to_products_map = {}
        for product in generated_products_es:
            product_id: str = product["_id"]  # filename minus extension
            granule_id = product_id[0:len(product_id) - 1 - product_id[::-1].index(".")]  # strip band to get granule

            if not granule_to_products_map.get(granule_id):
                granule_to_products_map[granule_id] = []
            granule_to_products_map[granule_id].append(product)
        return granule_to_products_map

    @staticmethod
    def map_by_name(generated_products_es):
        product_name_to_product_map = {}
        for product in generated_products_es:
            product_id: str = product["_id"]  # filename minus extension
            product_name_to_product_map[product_id] = product
        return product_name_to_product_map

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

        # extreme edge case where only 1 product type has been generated
        if len(times_hours) >= 2:
            xticks = xticks + [min(*times_hours), max(*times_hours)]

        ax.set(
            title=f"{title} Retrieval Times",
            xlabel="Retrieval Time (hours)", xticks=xticks, xticklabels=[f"{x:.2f}" for x in xticks],
            yticks=[], yticklabels=[]
        )

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


class RetrievalTimeDetailedReport(RetrievalTimeReport):
    def generate_report(self, output_format=None, report_type=None):
        return RetrievalTimeReport.generate_report(self, output_format=output_format, report_type="detailed")


class RetrievalTimeSummaryReport(RetrievalTimeReport):
    def generate_report(self, output_format=None, report_type=None):
        return RetrievalTimeReport.generate_report(self, output_format=output_format, report_type="summary")
