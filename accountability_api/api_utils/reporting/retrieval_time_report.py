import base64
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import elasticsearch.exceptions
import pandas as pd
from flask import current_app
from pandas import DataFrame

from accountability_api.api_utils import query, metadata
from accountability_api.api_utils.reporting.report import Report
# Pandas options
from accountability_api.api_utils.reporting.report_util import to_duration_isoformat, create_histogram

pd.set_option('display.max_rows', None)  # control the number of rows printed
pd.set_option('display.max_columns', None)  # Breakpoint for truncate view. `None` value means unlimited.
pd.set_option('display.width', None)   # control the printed line length. `None` value will auto-detect the width.
pd.set_option('display.max_colwidth', 10)  # Number of characters to print per column.


class RetrievalTimeReport(Report):
    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super().__init__(title, start_date, end_date, timestamp, **kwargs)

    def generate_report(self, output_format=None, report_type=None):
        current_app.logger.info(f"Generating report. {output_format=}, {self.__dict__=}")

        input_products = []
        for incoming_sdp_product_index in metadata.INCOMING_SDP_PRODUCTS.values():
            current_app.logger.info(f"Querying index {incoming_sdp_product_index} for products")

            try:
                input_products += query.get_docs(indexes=[incoming_sdp_product_index], start=self.start_datetime, end=self.end_datetime)
            except elasticsearch.exceptions.NotFoundError as e:
                current_app.logger.warning(f"An exception {type(e)} occurred while querying index {incoming_sdp_product_index} for products. Does the index exists?")

        if output_format == "application/zip":
            report_df = RetrievalTimeReport.to_report_df(input_products, report_type, start=self.start_datetime, end=self.end_datetime)

            # create zip. send zip.
            tmp_report_zip = tempfile.NamedTemporaryFile(suffix=".zip", dir=".", delete=True)
            with zipfile.ZipFile(tmp_report_zip.name, "w") as report_zipfile:
                # write histogram files, convert histogram column to filenames
                for i, row in report_df.iterrows():
                    tmp_histogram = tempfile.NamedTemporaryFile(suffix=".png", dir=".", delete=True)
                    histogram_b64: str = report_df.at[i, 'histogram']
                    tmp_histogram.write(base64.b64decode(histogram_b64))
                    tmp_histogram.flush()
                    histogram_filename = self.get_histogram_filename(
                        sds_product_name=report_df.at[i, "OPERA Product Short Name"],
                        input_product_name=report_df.at[i, "Input Product Short Name"])
                    report_zipfile.write(Path(tmp_histogram.name).name, arcname=histogram_filename)
                    report_df.at[i, 'histogram'] = histogram_filename

                tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
                current_app.logger.info(f"{tmp_report_csv.name=}")

                tmp_report_csv.write(report_df.to_csv().encode("utf-8"))
                tmp_report_csv.flush()

                report_zipfile.write(Path(tmp_report_csv.name).name, arcname=self.get_filename("text/csv"))
            return tmp_report_zip

        report_df = RetrievalTimeReport.to_report_df(input_products, report_type, start=self.start_datetime, end=self.end_datetime)

        if output_format == "text/csv":
            report_df.drop(columns=["histogram"], inplace=True)

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
    def to_report_df(product_docs: list[dict], report_type: str, start, end) -> DataFrame:
        current_app.logger.info(f"Total generated products for report {len(product_docs)}")
        if not product_docs:
            return pd.DataFrame(product_docs)

        # group products by filename, group products by granule
        product_name_to_product_map = RetrievalTimeReport.map_by_name(product_docs)
        granule_to_products_map = RetrievalTimeReport.map_by_granule(product_docs)

        RetrievalTimeReport.augment_with_hls_spatial_info(granule_to_products_map, start, end)
        RetrievalTimeReport.augment_with_hls_info(product_name_to_product_map, start, end)

        # create initial data frame with raw report data
        retrieval_times_seconds: list[dict] = []
        for product in product_docs:
            current_app.logger.debug(f'{product["_id"]=}')

            if not product.get("hls") or not product.get("hls_spatial"):
                current_app.logger.warning("HLS info unavailable. Did you skip query + download jobs?")

            if not product.get("hls"):  # possible in dev when skipping download job by direct file upload
                product_received_dt = datetime.fromisoformat(product["metadata"]["ProductReceivedTime"].removesuffix("Z"))
            else:
                product_received_dt = datetime.fromisoformat(product["hls"]["download_datetime"].removesuffix("Z"))
            product_received_ts = product_received_dt.timestamp()
            current_app.logger.debug(f"{product_received_dt=!s}")

            if not product.get("hls"):  # possible in dev when skipping download job by direct file upload
                opera_detect_dt = product_received_dt
            else:
                # add PublicAvailableDateTime information
                opera_detect_dt = datetime.fromisoformat(product["hls"]["query_datetime"].removesuffix("Z"))
            opera_detect_ts = opera_detect_dt.timestamp()
            current_app.logger.debug(f"{opera_detect_dt=!s}")

            if not product.get("hls_spatial"):  # possible in dev when skipping download job by direct file upload
                public_available_dt = opera_detect_dt
            else:
                public_available_dt = datetime.fromisoformat(product["hls_spatial"]["production_datetime"].removesuffix("Z"))
            public_available_ts = public_available_dt.timestamp()
            current_app.logger.debug(f"{public_available_dt=!s}")

            retrieval_time = product_received_ts - public_available_ts
            current_app.logger.debug(f"{retrieval_time=:,.0f} (seconds)")  # add commas. remove decimals

            if report_type == "detailed":
                retrieval_time_dict = {
                    "OPERA Product File Name": product["metadata"]["FileName"],
                    "ProductType": product["metadata"]["ProductType"],
                    "PublicAvailableDateTime": datetime.fromtimestamp(public_available_ts).isoformat(),
                    "OperaDetectDateTime": datetime.fromtimestamp(opera_detect_ts).isoformat(),
                    "ProductReceivedDateTime": datetime.fromtimestamp(product_received_ts).isoformat(),
                    "RetrievalTime": to_duration_isoformat(retrieval_time)
                }
            elif report_type == "summary":
                retrieval_time_dict = {
                    "OPERA Product File Name": product["metadata"]["FileName"],
                    "ProductType": product["metadata"]["ProductType"],
                    "RetrievalTime": retrieval_time
                }
            else:
                raise Exception(f"Unsupported report type. {report_type=}")
            retrieval_times_seconds.append(retrieval_time_dict)
            current_app.logger.debug("---")

        if report_type == "detailed":
            # create data frame of raw data (log report)
            df_retrieval_times_log = pd.DataFrame(retrieval_times_seconds)
            return df_retrieval_times_log
        elif report_type == "summary":
            # create data frame of aggregate data (summary report)
            df_summary = pd.DataFrame(retrieval_times_seconds)
            product_types = df_summary["ProductType"].unique()
            current_app.logger.debug(f"{product_types=}")

            current_app.logger.info("Processing recognized product types")
            df_retrieval_times_summary_entries = []
            for sds_product_type, input_product_types in metadata.SDS_PRODUCT_TYPE_TO_INPUT_PRODUCT_TYPES.items():
                current_app.logger.info(f"{sds_product_type=}")

                input_product_types_processed = []
                for input_product_type in input_product_types:
                    current_app.logger.debug(f"{input_product_type=}")

                    # filter by current input product type
                    df_summary_input_product_type = df_summary[df_summary['ProductType'].apply(lambda x: x == input_product_type)]
                    current_app.logger.debug(f"Found {len(df_summary_input_product_type)} {input_product_type} products")

                    if not len(df_summary_input_product_type):
                        current_app.logger.debug("0 products. Skipping to next input product type")
                        continue
                    input_product_types_processed.append(input_product_type)

                    retrieval_times_seconds: list[float] = df_summary_input_product_type["RetrievalTime"].to_numpy()
                    retrieval_times_hours = [secs / 60 / 60 for secs in retrieval_times_seconds]
                    histogram = create_histogram(
                        series=retrieval_times_hours,
                        title=f"{input_product_type} Retrieval Times",
                        metric="Retrieval Time",
                        unit="hours")

                    df_summary_input_product_type = pd.DataFrame([{
                        "OPERA Product Short Name": sds_product_type,  # e.g. L3_DSWX_HLS
                        "Input Product Short Name": input_product_type,  # e.g. L2_HLS_L30
                        "RetrievalTime (count)": len(df_summary_input_product_type),
                        "RetrievalTime (P90)": to_duration_isoformat(df_summary_input_product_type["RetrievalTime"].quantile(q=0.9)),
                        "RetrievalTime (min)": to_duration_isoformat(df_summary_input_product_type["RetrievalTime"].min()),
                        "RetrievalTime (max)": to_duration_isoformat(df_summary_input_product_type["RetrievalTime"].max()),
                        "RetrievalTime (mean)": to_duration_isoformat(df_summary_input_product_type["RetrievalTime"].mean()),
                        "RetrievalTime (median)": to_duration_isoformat(df_summary_input_product_type["RetrievalTime"].median()),
                        "histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")
                    }])
                    df_retrieval_times_summary_entries.append(df_summary_input_product_type)

                # filter by SDS product type to combine aggregations for their respective input product types
                # prevent redundant ALL row when only 1 input product type was processed
                if len(input_product_types_processed) > 1:
                    current_app.logger.info(f"Creating ALL entry")

                    df_summary_input_product_type_all = df_summary[df_summary['ProductType'].apply(lambda x: x in input_product_types)]
                    current_app.logger.debug(f"Found {len(df_summary_input_product_type_all)} {input_product_types} products")

                    retrieval_times_seconds: list[float] = df_summary_input_product_type_all["RetrievalTime"].to_numpy()
                    retrieval_times_hours: list[float] = [secs / 60 / 60 for secs in retrieval_times_seconds]
                    histogram = create_histogram(
                        series=retrieval_times_hours,
                        title=f"{sds_product_type} Retrieval Times",
                        metric="Retrieval Time",
                        unit="hours")

                    df_summary_input_product_type_all = pd.DataFrame([{
                        "OPERA Product Short Name": sds_product_type,  # e.g. L3_DSWX_HLS
                        "Input Product Short Name": "ALL",  # e.g. ALL = L2_HLS_L30 + L2_HLS_L30
                        "RetrievalTime (count)": len(df_summary_input_product_type_all),
                        "RetrievalTime (P90)": to_duration_isoformat(df_summary_input_product_type_all["RetrievalTime"].quantile(q=0.9)),
                        "RetrievalTime (min)": to_duration_isoformat(df_summary_input_product_type_all["RetrievalTime"].min()),
                        "RetrievalTime (max)": to_duration_isoformat(df_summary_input_product_type_all["RetrievalTime"].max()),
                        "RetrievalTime (mean)": to_duration_isoformat(df_summary_input_product_type_all["RetrievalTime"].mean()),
                        "RetrievalTime (median)": to_duration_isoformat(df_summary_input_product_type_all["RetrievalTime"].median()),
                        "histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")
                    }])
                    df_retrieval_times_summary_entries.append(df_summary_input_product_type_all)

            df_summary = pd.concat(df_retrieval_times_summary_entries)

            current_app.logger.info("Generated report")
            return df_summary
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
    def map_by_granule(product_docs: list[dict]):
        granule_to_products_map = {}
        for product in product_docs:
            product_id: str = product["_id"]  # filename minus extension
            granule_id = product_id[0:len(product_id) - 1 - product_id[::-1].index(".")]  # strip band to get granule

            if not granule_to_products_map.get(granule_id):
                granule_to_products_map[granule_id] = []
            granule_to_products_map[granule_id].append(product)
        return granule_to_products_map

    @staticmethod
    def map_by_name(product_docs: list[dict]):
        # filename minus extension
        product_name_to_product_map = {product["_id"]: product for product in product_docs}
        return product_name_to_product_map

    def get_filename(self, output_format):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        if output_format == "text/csv":
            return f"retrieval-time - {start_datetime_normalized} to {end_datetime_normalized}.csv"
        elif output_format == "text/html":
            return f"retrieval-time - {start_datetime_normalized} to {end_datetime_normalized}.html"
        elif output_format == "application/json":
            return f"retrieval-time - {start_datetime_normalized} to {end_datetime_normalized}.json"
        elif output_format == "image/png":
            return f"retrieval-time - {start_datetime_normalized} to {end_datetime_normalized}.png"
        elif output_format == "application/zip":
            return f"retrieval-time - {start_datetime_normalized} to {end_datetime_normalized}.zip"
        else:
            raise Exception(f"Output format not supported. {output_format=}")

    def get_histogram_filename(self, sds_product_name, input_product_name):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        return f"production-time - {sds_product_name} - {input_product_name} - {start_datetime_normalized} to {end_datetime_normalized}.png"

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


