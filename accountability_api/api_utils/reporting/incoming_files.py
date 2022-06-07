import traceback

from .report import Report
from accountability_api.api_utils import utils, query, processing
from accountability_api.api_utils import metadata as consts


class IncomingFiles(Report):
    """
    https://docs.google.com/presentation/d/1Qpk2sEkltm2rJJI-GvKiSChl2ljVQ7LDFbWURviFc7I/edit?usp=sharing

    """

    def __init__(self, title, start_date, end_date, timestamp, report_type="sdp", **kwargs):
        super(IncomingFiles, self).__init__(title, start_date, end_date, timestamp, **kwargs)
        self._report_type = report_type
        self._total_incoming_data_file_num = 0
        self._total_incoming_data_file_volume = 0
        self._total_products_produced_num = 0
        self._total_products_produced_size = 0

        self._products = {}

    def populate_data(self):
        products = {}
        products = self._get_incoming_products()

        self._products = products

    def _get_incoming_products(self):

        indexes = {}

        if self._report_type == "sdp":
            indexes = consts.INCOMING_SDP_PRODUCTS
        elif self._report_type == "ancillary":
            indexes = consts.INCOMING_ANCILLARY_FILES

        products = []

        total_products_produced = 0
        total_volume = 0

        for index in indexes:
            product_creation = query.construct_range_object(
                "creation_timestamp",
                start_value=self._start_datetime,
                stop_value=self._end_datetime,
            )
            source_includes = [
                "metadata.FileSize",
                "metadata.ProcessingType",
            ]

            body = {
                "query": {"bool": {"must": [{"range": product_creation}]}},
                "_source": source_includes,
            }
            body = self.add_universal_query_params(body)

            try:
                results = query.run_query_with_scroll(
                    index=indexes[index], body=body, doc_type="_doc"
                )

                volume = processing._get_processed_volume(
                    results.get("hits", {}).get("hits", [])
                )
                num_products = len(results.get("hits", {}).get("hits", []))

                total_products_produced += num_products
                total_volume += volume

                products.append(
                    {"name": index, "num_ingested": num_products, "volume": volume}
                )
            except Exception:
                traceback.print_exc()
                print(f"An exception has occurred. Returning 0 results for index {index}")
                products.append({"name": index, "num_ingested": 0, "volume": 0})

        self._total_incoming_data_file_num = total_products_produced
        self._total_incoming_data_file_volume = total_volume

        return products

    def get_dict_format(self):
        root_name = ""
        if self._report_type == "sdp":
            root_name = "INCOMING_SDP_PRODUCTS_REPORT"
        elif self._report_type == "ancillary":
            root_name = "INCOMING_ANCILLARY_PRODUCTS_REPORT"

        return {
            "root_name": root_name,
            "header": {
                "time_of_report": self._creation_time,
                "data_received_time_range": "{}-{}".format(
                    utils.to_iso_format_truncated(self._start_datetime),
                    utils.to_iso_format_truncated(self._end_datetime),
                ),
                "crid": self._crid,
                "venue": self._venue,
                "processing_mode": self._processing_mode,
                "total_products_produced": self._total_incoming_data_file_num,
                "total_data_volume": self._total_incoming_data_file_volume,
            },
            "products": self._products,
        }

    def get_data(self):
        return self._products

    def to_xml(self):
        data = self.get_dict_format()
        root_name = "root"
        if "root_name" in data:
            root_name = data["root_name"]
            del data["root_name"]

        return utils.convert_to_xml_str(root_name, data)

    def to_json(self):
        return super().to_json()

    def to_csv(self):
        return super().to_csv()

    def get_filename(self, output_format):
        """
        Constructs a filename in the format: incoming_<report_type>_files_<start>_<end>.<ext>.
        For example, "incoming_sdp_19700101T000000_20220101T000000.csv"
        """
        return f"incoming_{self._report_type}_files_{self._start_datetime}_{self._end_datetime}.{output_format}"

    def generate_report(self, output_format=None):
        return super().generate_report(output_format=output_format)
