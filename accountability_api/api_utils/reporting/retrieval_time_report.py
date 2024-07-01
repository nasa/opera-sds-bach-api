import base64
import json
import operator
import re
import tempfile
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from pathlib import Path

import elasticsearch.exceptions
import pandas as pd
from flask import current_app
from pandas import DataFrame

from accountability_api.api_utils import query, metadata, utils
from accountability_api.api_utils.reporting.report import Report
from accountability_api.api_utils.reporting.report_util import to_duration_isoformat, create_histogram

# Pandas options
pd.set_option("display.max_rows", None)  # control the number of rows printed
pd.set_option("display.max_columns", None)  # Breakpoint for truncate view. `None` value means unlimited.
pd.set_option("display.width", None)   # control the printed line length. `None` value will auto-detect the width.
pd.set_option("display.max_colwidth", 10)  # Number of characters to print per column.


def datetime_fromisoformat(date_string: str):
    return datetime.fromisoformat(date_string.removesuffix("Z"))


class RetrievalTimeReport(Report):
    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super().__init__(title, start_date, end_date, timestamp, **kwargs)
        self._report_options = kwargs["report_options"]

    def generate_report(self, output_format=None, report_type=None):
        current_app.logger.info(f"Generating report. {output_format=}, {self.__dict__=}")

        product_docs = []
        input_product_indexes = reduce(operator.add, metadata.INCOMING_SDP_PRODUCTS.values())
        for incoming_sdp_product_index in input_product_indexes:
            current_app.logger.info(f"Querying index {incoming_sdp_product_index} for products")

            try:
                product_docs += query.get_docs(indexes=[incoming_sdp_product_index], start=self.start_datetime, end=self.end_datetime)
            except elasticsearch.exceptions.NotFoundError as e:
                current_app.logger.warning(f"An exception {type(e)} occurred while querying indexes {incoming_sdp_product_index} for products. Do the indexes exists?")

        if output_format == "application/zip":
            report_df = RetrievalTimeReport.to_report_df(product_docs, report_type, start=self.start_datetime, end=self.end_datetime, report_options=self._report_options)

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
                            input_product_name=report_df["input_product_short_name"].values[i],
                            report_type=report_type)
                        report_zipfile.write(Path(tmp_histogram.name).name, arcname=histogram_filename)
                        report_df["histogram"].values[i] = histogram_filename

                RetrievalTimeReport.rename_columns(report_df, report_type)
                report_csv = report_df.to_csv(index=False)
                report_csv = self.add_header_to_csv(report_csv, report_type)

                tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
                current_app.logger.info(f"{tmp_report_csv.name=}")
                tmp_report_csv.write(report_csv.encode("utf-8"))
                tmp_report_csv.flush()

                report_zipfile.write(Path(tmp_report_csv.name).name, arcname=self.get_filename_by_report_type("text/csv", report_type))
            return tmp_report_zip

        report_df = RetrievalTimeReport.to_report_df(product_docs, report_type, start=self.start_datetime, end=self.end_datetime, report_options=self._report_options)

        if output_format == "text/csv":
            if self._report_options["generate_histograms"]:
                RetrievalTimeReport.drop_column(report_df, "histogram")
            RetrievalTimeReport.rename_columns(report_df, report_type)

            report_csv = report_df.to_csv(index=False)
            report_csv = self.add_header_to_csv(report_csv, report_type)

            tmp_report_csv = tempfile.NamedTemporaryFile(suffix=".csv", dir=".", delete=True)
            tmp_report_csv.write(report_csv.encode("utf-8"))
            tmp_report_csv.flush()
            return tmp_report_csv
        elif output_format == "application/json" or output_format == "json":
            report_json = report_df.to_json(orient="records", date_format="epoch", lines=False)
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
    def to_report_df(dataset_docs: list[dict], report_type: str, start, end, report_options: dict) -> DataFrame:
        current_app.logger.info(f"Total generated datasets for report {len(dataset_docs)}")
        if not dataset_docs:  # EDGE CASE: no products in data store
            return pd.DataFrame()

        dataset_id_to_dataset_map = RetrievalTimeReport.map_by_id(dataset_docs)
        dataset_base_id_to_dataset_map = RetrievalTimeReport.map_by_base_id(dataset_docs)

        sds_product_type_to_input_datasets_map = defaultdict(list)
        for dataset in dataset_docs:
            if dataset["_id"].startswith("OPERA_L2_RTC-S1"):
                for sds_product_type in metadata.INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE["L2_RTC_S1"]:
                    sds_product_type_to_input_datasets_map[sds_product_type].append(dataset)
            elif dataset["_index"].startswith("cslc_catalog") and dataset["granule_id"].startswith("OPERA_L2_CSLC-S1"):
                for sds_product_type in metadata.INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE["L2_CSLC_S1"]:
                    sds_product_type_to_input_datasets_map[sds_product_type].append(dataset)
            else:
                for sds_product_type in metadata.INPUT_PRODUCT_TYPE_TO_SDS_PRODUCT_TYPE[dataset["dataset_type"]]:
                    sds_product_type_to_input_datasets_map[sds_product_type].append(dataset)

        # map L3_DSWX_HLS input products with ancillary information needed for report
        if sds_product_type_to_input_datasets_map.get("L3_DSWX_HLS"):
            RetrievalTimeReport.augment_hls_products_with_hls_spatial_info(
                dataset_base_id_to_dataset_map,
                utils.from_dt_to_iso(utils.from_iso_to_dt(start) - timedelta(hours=24)),
                end
            )
            RetrievalTimeReport.augment_hls_products_with_hls_info(
                dataset_id_to_dataset_map,
                utils.from_dt_to_iso(utils.from_iso_to_dt(start) - timedelta(hours=24)),
                end
            )

        # map L2_CSLC_S1 and L2_RTC_S1 input products with ancillary information needed for report
        l2_cslc_s1_input_product_docs = sds_product_type_to_input_datasets_map.get("L2_CSLC_S1")
        l2_rtc_s1_input_product_docs = sds_product_type_to_input_datasets_map.get("L2_RTC_S1")
        if l2_cslc_s1_input_product_docs or l2_rtc_s1_input_product_docs:
            RetrievalTimeReport.augment_slc_products_with_slc_info(
                dataset_base_id_to_dataset_map,
                utils.from_dt_to_iso(utils.from_iso_to_dt(start) - timedelta(hours=24)),
                end
            )

        l3_dswx_s1_input_product_docs = sds_product_type_to_input_datasets_map.get("L3_DSWX_S1")
        if l3_dswx_s1_input_product_docs:
            # TODO chrisjrd: augment with something
            pass

        # TODO chrisjrd: group DSWx-S1 input RTC into burst sets
        pass
        burst_cycle_index_to_burst_map = {}
        for dataset in dataset_docs:
            product = dataset
            if product["_id"].startswith("OPERA_L2_RTC-S1"):
                if not burst_cycle_index_to_burst_map.get("mgrs_set_id_acquisition_ts_cycle_index"):
                    burst_cycle_index_to_burst_map["mgrs_set_id_acquisition_ts_cycle_index"] = []
                burst_cycle_index_to_burst_map["mgrs_set_id_acquisition_ts_cycle_index"].append(dataset)
        for k in burst_cycle_index_to_burst_map:
            burst_cycle_index_to_burst_map[k] = sorted(burst_cycle_index_to_burst_map[k], key=lambda burst: burst["creation_timestamp"], reverse=True)
            burst_cycle_index_to_burst_map[k] = max(burst_cycle_index_to_burst_map[k], key=lambda burst: burst["creation_timestamp"])

        # create initial data frame with raw report data
        dataset_docs = list(dataset_id_to_dataset_map.values())
        retrieval_times_seconds: list[dict] = []
        for dataset in dataset_docs:
            current_app.logger.debug(f'{dataset["_id"]=}')
            products = []

            if dataset.get("metadata", {}).get("Files"):
                for product in dataset["metadata"]["Files"]:
                    nested_product = {"metadata": product}

                    if product.get("hls"):
                        nested_product["hls"] = product["hls"]

                    if product.get("hls_spatial"):
                        nested_product["hls_spatial"] = product["hls_spatial"]

                    nested_product["metadata"]["ProductReceivedTime"] = dataset["metadata"]["ProductReceivedTime"]
                    nested_product["metadata"]["ProductType"] = dataset["metadata"]["ProductType"]
                    nested_product["dataset_type"] = dataset["dataset_type"]

                    products.append(nested_product)
            else:
                products.append(dataset)

            for product in products:
                # gather important timestamps for subsequent aggregations
                product_id = product.get("metadata", {}).get("id")
                if not product_id:
                    if product.get("granule_id", "").startswith("OPERA_L2_CSLC-S1"):
                        product_id = product["granule_id"]
                    else:
                        product_id = product["id"]

                if product_id.startswith("OPERA_L2_RTC-S1"):
                    # may or may not have been submitted for download
                    if product.get("latest_creation_timestamp"):
                        product_received_dt = datetime_fromisoformat(product["latest_creation_timestamp"])
                    else:
                        product_received_dt = datetime_fromisoformat(product["creation_timestamp"])
                elif product_id.startswith("OPERA_L2_CSLC-S1"):
                    product_received_dt = datetime_fromisoformat(product["creation_timestamp"])
                    if product.get("download_datetime"):
                        product_received_dt = datetime_fromisoformat(product["download_datetime"])
                    if product.get("latest_download_job_ts"):
                        product_received_dt = datetime_fromisoformat(product["latest_download_job_ts"])
                else:
                    product_received_dt = datetime_fromisoformat(product["metadata"]["ProductReceivedTime"])
                product_received_ts = product_received_dt.timestamp()
                current_app.logger.debug(f"{product_received_dt=!s}")

                # get timestamps from catalog
                if product.get("hls"):
                    opera_detect_dt = datetime_fromisoformat(product["hls"]["query_datetime"])
                elif product.get("slc"):
                    opera_detect_dt = datetime_fromisoformat(product["slc"]["query_datetime"])
                elif product_id.startswith("OPERA_L2_RTC-S1"):
                    opera_detect_dt = datetime_fromisoformat(product["query_datetime"])
                elif product_id.startswith("OPERA_L2_CSLC-S1"):
                    opera_detect_dt = datetime_fromisoformat(product["creation_timestamp"])
                else:  # possible in local dev
                    opera_detect_dt = product_received_dt

                opera_detect_ts = opera_detect_dt.timestamp()
                current_app.logger.debug(f"{opera_detect_dt=!s}")

                if product.get("hls_spatial"):
                    if product["hls_spatial"].get("provider_date"):
                        public_available_dt = datetime_fromisoformat(product["hls_spatial"]["provider_date"])
                    else:
                        public_available_dt = datetime_fromisoformat(product["hls_spatial"]["production_datetime"])
                elif product.get("slc_spatial"):
                    if product["slc_spatial"].get("provider_date"):
                        public_available_dt = datetime_fromisoformat(product["slc_spatial"]["provider_date"])
                    else:
                        public_available_dt = datetime_fromisoformat(product["slc_spatial"]["production_datetime"])
                elif product_id.startswith("OPERA_L2_RTC-S1"):
                    if product.get("latest_production_datetime"):
                        public_available_dt = datetime_fromisoformat(product["production_datetime"])
                        latest_public_available_dt = datetime_fromisoformat(product["latest_production_datetime"])
                    else:
                        public_available_dt = datetime_fromisoformat(product["production_datetime"])
                else:  # possible in dev when skipping download job by direct file upload
                    public_available_dt = opera_detect_dt
                public_available_ts = public_available_dt.timestamp()
                current_app.logger.debug(f"{public_available_dt=!s}")
                if product_id.startswith("OPERA_L2_RTC-S1"):
                    if product.get("latest_production_datetime"):
                        latest_public_available_ts = latest_public_available_dt.timestamp()

                retrieval_time = product_received_ts - public_available_ts
                current_app.logger.debug(f"{retrieval_time=:,.0f} (seconds)")  # add commas. remove decimals

                # create report data frame record depending on report type

                retrieval_time_dicts = []
                if report_type == "detailed":
                    if product_id.startswith("OPERA_L2_RTC-S1"):
                        input_product_name = product["_id"]
                        input_product_type = "OPERA_L2_RTC-S1"
                    elif product_id.startswith("OPERA_L2_CSLC-S1"):
                        input_product_name = product_id
                        input_product_type = "OPERA_L2_CSLC-S1"
                    else:
                        input_product_name = product["metadata"]["FileName"]
                        input_product_type = product["metadata"]["ProductType"]
                    retrieval_time_dict = {
                        "input_product_name": input_product_name,
                        "input_product_type": input_product_type,
                        "public_available_datetime": datetime.fromtimestamp(public_available_ts).isoformat(),
                        "opera_detect_datetime": datetime.fromtimestamp(opera_detect_ts).isoformat(),
                        "product_received_datetime": datetime.fromtimestamp(product_received_ts).isoformat(),
                        "retrieval_time": to_duration_isoformat(retrieval_time),
                    }

                    if product_id.startswith("OPERA_L2_RTC-S1"):
                        if product.get("latest_production_datetime"):
                            retrieval_time_dict.update({"latest_public_available_datetime": datetime.fromtimestamp(latest_public_available_ts).isoformat()})

                    retrieval_time_dicts.append(retrieval_time_dict)
                elif report_type == "summary":
                    if product_id.startswith("OPERA_L2_RTC-S1"):
                        input_product_name = product["_id"]
                        input_product_type = "OPERA_L2_RTC-S1"
                    elif product_id.startswith("OPERA_L2_CSLC-S1"):
                        input_product_name = product_id
                        input_product_type = "OPERA_L2_CSLC-S1"
                    else:
                        input_product_name = product["metadata"]["FileName"]
                        input_product_type = product["metadata"]["ProductType"]
                    retrieval_time_dict = {
                        "opera_product_name": input_product_name,
                        "input_product_type": input_product_type,
                        "retrieval_time": retrieval_time
                    }
                    retrieval_time_dicts.append(retrieval_time_dict)
                else:
                    raise Exception(f"Unsupported report type. {report_type=}")
                for retrieval_time_dict in retrieval_time_dicts:
                    retrieval_times_seconds.append(retrieval_time_dict)
                current_app.logger.debug("---")

        if not retrieval_times_seconds:
            # EDGE CASE: input products exist, but output products do not
            return pd.DataFrame()

        if report_type == "detailed":
            # create data frame of raw data (log report)
            df_retrieval_times_log = pd.DataFrame(retrieval_times_seconds)
            return df_retrieval_times_log
        elif report_type == "summary":
            # create data frame of aggregate data (summary report)
            df_retrieval_times_summary = pd.DataFrame(retrieval_times_seconds)
            product_types = df_retrieval_times_summary["input_product_type"].unique()
            current_app.logger.debug(f"{product_types=}")

            # loop through output/input product type combinations and aggregate statistics into dataframe rows

            df_retrieval_times_summary_rows = []
            for input_product_type in product_types:
                current_app.logger.debug(f"{input_product_type=}")

                # filter by current input product type
                df_retrieval_time_summary_row = df_retrieval_times_summary[
                    (
                        df_retrieval_times_summary["input_product_type"].apply(lambda x: x == input_product_type)
                    )
                ]
                current_app.logger.debug(f"Found {len(df_retrieval_time_summary_row)} {input_product_type} products")

                if not len(df_retrieval_time_summary_row):
                    current_app.logger.debug("0 products. Skipping to next input product type")
                    continue

                retrieval_times_seconds: list[float] = df_retrieval_time_summary_row["retrieval_time"].to_numpy()
                retrieval_times_hours = [secs / 60 / 60 for secs in retrieval_times_seconds]

                retrieval_time_summary_row = {
                    "input_product_short_name": input_product_type,  # e.g. L2_HLS_L30
                    "retrieval_time_count": len(df_retrieval_time_summary_row),
                    "retrieval_time_p90": to_duration_isoformat(df_retrieval_time_summary_row["retrieval_time"].quantile(q=0.9)),
                    "retrieval_time_min": to_duration_isoformat(df_retrieval_time_summary_row["retrieval_time"].min()),
                    "retrieval_time_max": to_duration_isoformat(df_retrieval_time_summary_row["retrieval_time"].max()),
                    "retrieval_time_median": to_duration_isoformat(df_retrieval_time_summary_row["retrieval_time"].median()),
                    "retrieval_time_mean": to_duration_isoformat(df_retrieval_time_summary_row["retrieval_time"].mean()),
                }
                if report_options["generate_histograms"]:
                    histogram = create_histogram(
                        series=retrieval_times_hours,
                        title=f"{input_product_type} Retrieval Times",
                        metric="Retrieval Time",
                        unit="hours")
                    retrieval_time_summary_row.update({"histogram": str(base64.b64encode(histogram.getbuffer().tobytes()), "utf-8")})
                df_retrieval_time_summary_row = pd.DataFrame([retrieval_time_summary_row])
                df_retrieval_times_summary_rows.append(df_retrieval_time_summary_row)

            df_retrieval_times_summary = pd.concat(df_retrieval_times_summary_rows)

            current_app.logger.info("Generated report")
            return df_retrieval_times_summary
        else:
            raise Exception(f"Unsupported report type. {report_type=}")

    @staticmethod
    def augment_hls_products_with_hls_info(dataset_id_to_dataset_map: dict[str, list[dict]], start, end):
        current_app.logger.info("Adding HLS information to products")

        hls_docs: list[dict] = query.get_docs(indexes=["hls_catalog-*"], start=start, end=end)
        for hls_doc in hls_docs:
            hls_doc_id = hls_doc["_id"]  # filename
            product_name = hls_doc_id[0:len(hls_doc_id) - 1 - hls_doc_id[::-1].index(".")]  # strip extension to get product name
            dataset_id = granule_id = product_name[0:len(product_name) - 1 - product_name[::-1].index(".")]  # strip band/QA mask to get granule ID
            granule = dataset = dataset_id_to_dataset_map.get(dataset_id, {})
            if not granule:
                continue
            for input_product in granule["metadata"]["Files"]:
                input_product["hls"] = hls_doc

    @staticmethod
    def augment_hls_products_with_hls_spatial_info(dataset_id_to_datasets_map: dict[str, list[dict]], start, end):
        current_app.logger.info("Adding HLS spatial information to products")

        hls_spatial_docs: list[dict] = query.get_docs(indexes=["hls_spatial_catalog-*"], start=start, end=end)
        for hls_spatial_doc in hls_spatial_docs:
            dataset_id = granule_id = hls_spatial_doc_id = hls_spatial_doc["_id"]  # filename minus extension minus band (i.e. granule)
            granule = dataset = dataset_id_to_datasets_map.get(dataset_id, {})
            if not granule:
                continue
            granule["hls_spatial"] = hls_spatial_doc
            for input_product in granule["metadata"]["Files"]:
                input_product["hls_spatial"] = hls_spatial_doc

    @staticmethod
    def augment_slc_products_with_slc_info(dataset_id_to_dataset_map: dict[str, list[dict]], start, end):
        current_app.logger.info("Adding SLC information to products")

        slc_docs: list[dict] = query.get_docs(indexes=["slc_catalog-*"], start=start, end=end)
        for slc_doc in slc_docs:
            slc_doc_id = slc_doc["_id"]  # filename
            product_name = slc_doc_id[0:len(slc_doc_id) - 1 - slc_doc_id[::-1].index(".")]  # strip extension to get product name
            dataset_id = granule_id = product_name
            granule = dataset = dataset_id_to_dataset_map.get(dataset_id, {})
            if not granule:
                continue
            granule["slc"] = slc_doc

        slc_spatial_docs: list[dict] = query.get_docs(indexes=["slc_spatial_catalog-*"], start=start, end=end)
        for slc_spatial_doc in slc_spatial_docs:
            slc_doc_id: str
            slc_doc_id = slc_spatial_doc["_id"]  # filename
            product_name = slc_doc_id.rsplit("-", maxsplit=1)[0]  # remove `-SLC` suffix
            dataset_id = granule_id = product_name
            granule = dataset = dataset_id_to_dataset_map.get(dataset_id, {})
            if not granule:
                continue
            granule["slc_spatial"] = slc_spatial_doc

    @staticmethod
    def map_by_granule(product_docs: list[dict]):
        granule_to_products_map = {}
        for product in product_docs:
            granule_id = product_id = product["_id"]  # filename minus extension

            if not granule_to_products_map.get(granule_id):
                granule_to_products_map[granule_id] = []
            granule_to_products_map[granule_id].append(product)
        return granule_to_products_map

    @staticmethod
    def map_by_id(dataset_docs: list[dict]):
        dataset_id_to_datasets_map = {dataset["_id"]: dataset for dataset in dataset_docs}
        return dataset_id_to_datasets_map

    @staticmethod
    def map_by_base_id(dataset_docs: list[dict]):
        dataset_id_to_datasets_map = {}
        for dataset in dataset_docs:
            id_: str = dataset["_id"]
            if re.search(r"-r\d+$", id_):  # remove revision number suffix
                base_id = id_.rsplit("-", maxsplit=1)[0]
                dataset_id_to_datasets_map[base_id] = dataset
        return dataset_id_to_datasets_map

    def get_filename_by_report_type(self, output_format, report_type):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        if output_format == "text/csv":
            return f"retrieval-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.csv"
        elif output_format == "text/html":
            return f"retrieval-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.html"
        elif output_format == "application/json":
            return f"retrieval-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.json"
        elif output_format == "application/zip":
            return f"retrieval-time-{report_type} - {start_datetime_normalized} to {end_datetime_normalized}.zip"
        else:
            raise Exception(f"Output format not supported. {output_format=}")

    def get_header(self, report_type):
        if report_type == "summary":
            header = self.get_header_summary()
        elif report_type == "detailed":
            header = self.get_header_detailed()
        else:
            raise Exception(f"{report_type=}")
        return header

    def add_header_to_csv(self, report_csv, report_type):
        header = self.get_header(report_type)
        header_str = ""
        for line in header:
            for k, v in line.items():
                header_str += f"{k}: {v}\n"
        report_csv = header_str + report_csv
        return report_csv

    def get_header_detailed(self) -> list[dict[str, str]]:
        header = [
            {"Title": "OPERA Retrieval Time Log"},
            {"Date of Report": datetime.fromisoformat(self._creation_time).strftime("%Y-%m-%dT%H:%M:%SZ")},
            {"Period of Coverage (AcquisitionTime)": f'{datetime.fromisoformat(self.start_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")} - {datetime.fromisoformat(self.end_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")}'},
            {"PublicAvailableDateTime": "datetime when the product was first made available to the public by the DAAC."},
            {"OperaDetectDateTime": "datetime when the OPERA system first became aware of the product."},
            {"ProductReceivedDateTime": "datetime when the product arrived in our system"}
        ]
        return header

    def get_header_summary(self) -> list[dict[str, str]]:
        header = [
            {"Title": "OPERA Retrieval Time Summary"},
            {"Date of Report": datetime.fromisoformat(self._creation_time).strftime("%Y-%m-%dT%H:%M:%SZ")},
            {"Period of Coverage (AcquisitionTime)": f'{datetime.fromisoformat(self.start_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")} - {datetime.fromisoformat(self.end_datetime).strftime("%Y-%m-%dT%H:%M:%SZ")}'}
        ]
        return header

    def get_histogram_filename(self, input_product_name, report_type):
        start_datetime_normalized = self.start_datetime.replace(":", "")
        end_datetime_normalized = self.end_datetime.replace(":", "")

        return f"retrieval-time-{report_type} - {input_product_name} - {start_datetime_normalized} to {end_datetime_normalized}.png"

    @staticmethod
    def rename_columns(report_df: DataFrame, report_type: str):
        if report_type == "summary":
            return RetrievalTimeReport.rename_summary_columns(report_df)
        elif report_type == "detailed":
            return RetrievalTimeReport.rename_detailed_columns(report_df)
        else:
            raise Exception(f"Unrecognized report type. {report_type=}")

    @staticmethod
    def rename_detailed_columns(report_df: DataFrame):
        report_df.rename(
            columns={
                "input_product_name": "Input Product Name",
                "input_product_type": "Input Product Type",
                "public_available_datetime": "Public Available Datetime",
                "opera_detect_datetime": "OPERA Detect Datetime",
                "product_received_datetime": "Received Datetime",
                "retrieval_time": "Retrieval Time"
            },
            inplace=True)

    @staticmethod
    def rename_summary_columns(report_df: DataFrame):
        report_df.rename(
            columns={
                "input_product_short_name": "Input Product Short Name",
                "retrieval_time_count": "Retrieval Time (count)",
                "retrieval_time_p90": "Retrieval Time (P90)",
                "retrieval_time_min": "Retrieval Time (min)",
                "retrieval_time_max": "Retrieval Time (max)",
                "retrieval_time_median": "Retrieval Time (median)",
                "retrieval_time_mean": "Retrieval Time (mean)"
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


