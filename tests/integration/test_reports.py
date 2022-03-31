import json
import os
import re
from importlib import import_module

from lxml import etree
from pytest_mock import MockerFixture

from accountability_api.api_utils.reporting.daac_outgoing_products import DaacOutgoingProducts
from accountability_api.api_utils.reporting.data_accountability_report import DataAccountabilityReport
from accountability_api.api_utils.reporting.generated_sds_products import GeneratedSdsProducts
from accountability_api.api_utils.reporting.incoming_files import IncomingFiles
from accountability_api.api_utils.reporting.report import Report
from accountability_api.api_utils.reporting.reports_generator import ReportsGenerator


def sort_observations(observations):
    return sorted(observations, key=lambda child: child.find("OBS_ID").text)


def sort_products(products):
    return sorted(products, key=lambda child: child.find("name").text)


class ElasticsearchUtilityStub:
    def search(self, **kwargs):
        return {}

class TestReports:
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setup_method(self, method):
        if not os.path.isdir("generated_reports"):
            os.mkdir("generated_reports")

    def teardown_method(self, method):
        pass

    def test_incoming_nen_files_report(self, mocker: MockerFixture):
        mocker.patch("accountability_api.api_utils.get_grq_es", return_value=ElasticsearchUtilityStub())

        assert issubclass(IncomingFiles, Report)

        # make sure all reports are subclasses of the Report class when getting importing via the import_module

        report_name = "IncomingFiles"
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module(f"accountability_api.api_utils.reporting.{module_path}")

        cls = getattr(module, report_name)

        assert issubclass(cls, Report)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        incoming_nen_files_report = generator.generate_report(
            report_name=report_name, report_type="sdp", output_format="json"
        )

        expected_result = None
        with open(
            "tests/expected_reports/incoming_files_reports/incoming_sdp_files.json", "r"
        ) as f:
            expected_result = json.loads(f.read())

        incoming_nen_files_report = json.loads(incoming_nen_files_report)

        assert incoming_nen_files_report["root_name"] == expected_result["root_name"]
        assert incoming_nen_files_report["header"]["data_received_time_range"] == expected_result["header"]["data_received_time_range"]
        assert incoming_nen_files_report["header"]["total_products_produced"] == expected_result["header"]["total_products_produced"]
        assert incoming_nen_files_report["header"]["total_data_volume"] == expected_result["header"]["total_data_volume"]

        expected_result_products = {}

        for product in expected_result["products"]:
            expected_result_products[product["name"]] = {
                "num_ingested": product["num_ingested"],
                "volume": product["volume"],
            }

        for product in incoming_nen_files_report["products"]:
            assert product["name"] in expected_result_products
            assert product["num_ingested"] == expected_result_products[product["name"]]["num_ingested"]
            assert product["volume"] == expected_result_products[product["name"]]["volume"]

    def test_incoming_ancillary_files_report(self, mocker: MockerFixture):
        mocker.patch("accountability_api.api_utils.get_grq_es", return_value=ElasticsearchUtilityStub())

        assert issubclass(IncomingFiles, Report)

        # make sure all reports are subclasses of the Report class when getting importing via the import_module

        report_name = "IncomingFiles"
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module("accountability_api.api_utils.reporting." + module_path)

        cls = getattr(module, report_name)

        assert issubclass(cls, Report)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        incoming_ancillary_files_report = generator.generate_report(
            report_name="IncomingFiles",
            report_type="ancillary",
            output_format="json",
        )

        expected_result = None
        with open(
            "tests/expected_reports/incoming_files_reports/incoming_ancillary_files.json", "r"
        ) as f:
            expected_result = json.loads(f.read())

        incoming_ancillary_files_report = json.loads(incoming_ancillary_files_report)

        assert incoming_ancillary_files_report["root_name"] == expected_result["root_name"]
        assert incoming_ancillary_files_report["header"]["data_received_time_range"] == expected_result["header"]["data_received_time_range"]
        assert incoming_ancillary_files_report["header"]["total_products_produced"] == expected_result["header"]["total_products_produced"]
        assert incoming_ancillary_files_report["header"]["total_data_volume"] == expected_result["header"]["total_data_volume"]

        expected_result_products = {}

        for product in expected_result["products"]:
            expected_result_products[product["name"]] = {
                "num_ingested": product["num_ingested"],
                "volume": product["volume"],
            }

        for product in incoming_ancillary_files_report["products"]:
            assert product["name"] in expected_result_products
            assert product["num_ingested"] == expected_result_products[product["name"]]["num_ingested"]
            assert product["volume"] == expected_result_products[product["name"]]["volume"]

    def test_generated_sds_products_(self, mocker: MockerFixture):
        mocker.patch("accountability_api.api_utils.get_grq_es", return_value=ElasticsearchUtilityStub())

        assert issubclass(GeneratedSdsProducts, Report)

        # make sure all reports are subclasses of the Report class when getting importing via the import_module

        report_name = "GeneratedSdsProducts"
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module("accountability_api.api_utils.reporting." + module_path)

        cls = getattr(module, report_name)

        assert issubclass(cls, Report)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        generated_sds_products_report = generator.generate_report(
            report_name=report_name, report_type="brief", output_format="json"
        )

        expected_result = None
        with open(
            "tests/expected_reports/generated_sds_products_reports/generated_sds_products.json",
            "r",
        ) as f:
            expected_result = json.loads(f.read())

        generated_sds_products_report = json.loads(generated_sds_products_report)

        assert generated_sds_products_report["root_name"] == expected_result["root_name"]
        assert generated_sds_products_report["header"]["data_received_time_range"] == expected_result["header"]["data_received_time_range"]
        assert generated_sds_products_report["header"]["total_products_produced"] == expected_result["header"]["total_products_produced"]
        assert generated_sds_products_report["header"]["total_data_volume"] == expected_result["header"]["total_data_volume"]

        expected_result_products = {}

        for product in expected_result["products"]:
            expected_result_products[product["name"]] = {
                "files_produced": product["files_produced"],
                "volume": product["volume"],
            }

        for product in generated_sds_products_report["products"]:
            assert product["name"] in expected_result_products
            assert product["files_produced"] == expected_result_products[product["name"]]["files_produced"]
            assert product["volume"] == expected_result_products[product["name"]]["volume"]

    def test_daac_outgoing_products_report(self, mocker: MockerFixture):
        mocker.patch("accountability_api.api_utils.get_grq_es", return_value=ElasticsearchUtilityStub())

        assert issubclass(DaacOutgoingProducts, Report)

        # make sure all reports are subclasses of the Report class when getting importing via the import_module

        report_name = "DaacOutgoingProducts"
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module("accountability_api.api_utils.reporting." + module_path)

        cls = getattr(module, report_name)

        assert issubclass(cls, Report)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        daac_outgoing_products_report = generator.generate_report(
            report_name=report_name, report_type="brief", output_format="json"
        )

        expected_result = None
        with open(
            "tests/expected_reports/daac_outgoing_products_reports/daac_outgoing_products.json",
            "r",
        ) as f:
            expected_result = json.loads(f.read())

        daac_outgoing_products_report = json.loads(daac_outgoing_products_report)

        assert daac_outgoing_products_report["root_name"] == expected_result["root_name"]
        assert daac_outgoing_products_report["header"]["data_received_time_range"] == expected_result["header"]["data_received_time_range"]
        assert daac_outgoing_products_report["header"]["total_products_produced"] == expected_result["header"]["total_products_produced"]
        assert daac_outgoing_products_report["header"]["total_data_volume"] == expected_result["header"]["total_data_volume"]

        expected_result_products = {}

        for product in expected_result["products"]:
            expected_result_products[product["name"]] = {
                "products_delivered": product["products_delivered"],
                "volume": product["volume"],
            }

        for product in daac_outgoing_products_report["products"]:
            assert product["name"] in expected_result_products
            assert product["products_delivered"] == expected_result_products[product["name"]]["products_delivered"]
            assert product["volume"] == expected_result_products[product["name"]]["volume"]

    def test_data_accountability_report_1(self, mocker: MockerFixture):
        mocker.patch("accountability_api.api_utils.get_grq_es", return_value=ElasticsearchUtilityStub())

        assert issubclass(DataAccountabilityReport, Report)

        # make sure all reports are subclasses of the Report class when getting importing via the import_module

        report_name = "DataAccountabilityReport"
        module_path = re.sub(r"(?<!^)(?=[A-Z])", "_", report_name).lower()

        module = import_module("accountability_api.api_utils.reporting." + module_path)

        cls = getattr(module, report_name)

        assert issubclass(cls, Report)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        dar_report = generator.generate_report(
            report_name=report_name, report_type="brief", output_format="xml"
        )

        with open("generated_reports/DAR.xml", "w") as f:
            f.write(dar_report)

        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse("generated_reports/DAR.xml", parser)
        tree.write("generated_reports/DAR.xml", pretty_print=True)

        dar_report_tree = etree.parse(
            "tests/expected_reports/data_accountability_reports/report_1.xml",
            parser,
        )

        expected_obs_root = dar_report_tree.getroot()
        actual_obs_root = tree.getroot()

        expected_obs = expected_obs_root.findall("item")
        actual_obs = actual_obs_root.findall("item")

        expected_obs = sort_observations(expected_obs)
        actual_obs = sort_observations(actual_obs)

        assert len(expected_obs) == len(actual_obs)

        generator = ReportsGenerator(
            "2021-01-01T00:00:00Z", "2025-01-01T23:59:00Z", mime="json"
        )

        dar_report = generator.generate_report(
            report_name=report_name, report_type="brief"
        )

        expected_result = None
        with open(
            "tests/expected_reports/data_accountability_reports/report_1.json",
            "r",
        ) as f:
            expected_result = json.loads(f.read())

        dar_report = json.loads(dar_report)

        assert dar_report["root_name"] == expected_result["root_name"]
        assert dar_report["header"]["data_received_time_range"] == expected_result["header"]["data_received_time_range"]
        assert dar_report["header"]["total_incoming_data_files"] == expected_result["header"]["total_incoming_data_files"]
        assert dar_report["header"]["total_incoming_data_volume"] == expected_result["header"]["total_incoming_data_volume"]
        assert dar_report["header"]["total_products_produced_files"] == expected_result["header"]["total_products_produced_files"]
        assert dar_report["header"]["total_products_produced_volume"] == expected_result["header"]["total_products_produced_volume"]

        expected_result_products = {
            "incoming_nen_products": {},
            "incoming_ancillary_products": {},
            "generated_sds_products": {},
            "daac_outgoing_products": {},
        }

        for product in expected_result["incoming_nen_products"]:
            expected_result_products["incoming_nen_products"][product["name"]] = {
                "num_ingested": product["num_ingested"],
                "volume": product["volume"],
            }
        for product in expected_result["incoming_ancillary_products"]:
            expected_result_products["incoming_ancillary_products"][product["name"]] = {
                "num_ingested": product["num_ingested"],
                "volume": product["volume"],
            }
        for product in expected_result["generated_sds_products"]:
            expected_result_products["generated_sds_products"][product["name"]] = {
                "files_produced": product["files_produced"],
                "volume": product["volume"],
            }
        for product in expected_result["daac_outgoing_products"]:
            expected_result_products["daac_outgoing_products"][product["name"]] = {
                "products_delivered": product["products_delivered"],
                "volume": product["volume"],
            }

        # check incoming_nen_products
        for product in dar_report["incoming_nen_products"]:
            assert product["name"] in expected_result_products["incoming_nen_products"]
            assert product["num_ingested"] == expected_result_products["incoming_nen_products"][product["name"]]["num_ingested"]
            assert product["volume"] == expected_result_products["incoming_nen_products"][product["name"]]["volume"]
        # check incoming_ancillary_products
        for product in dar_report["incoming_ancillary_products"]:
            assert product["name"] in expected_result_products["incoming_ancillary_products"]
            assert product["num_ingested"] == expected_result_products["incoming_ancillary_products"][product["name"]]["num_ingested"]
            assert product["volume"] == expected_result_products["incoming_ancillary_products"][product["name"]]["volume"]
        # check generated_sds_products
        for product in dar_report["generated_sds_products"]:
            assert product["name"] in expected_result_products["generated_sds_products"]
            assert product["files_produced"] == expected_result_products["generated_sds_products"][product["name"]]["files_produced"]
            assert product["volume"] == expected_result_products["generated_sds_products"][product["name"]]["volume"]
        # check daac_outgoing_products
        for product in dar_report["daac_outgoing_products"]:
            assert product["name"] in expected_result_products["daac_outgoing_products"]
            assert product["products_delivered"] == expected_result_products["daac_outgoing_products"][product["name"]]["products_delivered"]
            assert product["volume"] == expected_result_products["daac_outgoing_products"][product["name"]]["volume"]
