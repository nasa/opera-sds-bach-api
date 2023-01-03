import json
from unittest.mock import MagicMock

import pandas
from pandas.testing import assert_frame_equal
from pytest_mock import MockerFixture

from accountability_api.api_utils.reporting.retrieval_time_detailed_report import RetrievalTimeDetailedReport


def test_generate_report__when_json_and_empty(test_client, mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.get_docs", MagicMock())
    report = RetrievalTimeDetailedReport(title="Test Report", start_date="1970-01-01", end_date="1970-01-01", timestamp="1970-01-01")

    # ACT
    json_report = report.generate_report("application/json")

    # ASSERT
    assert json.loads(json_report)["payload"] == []


def test_generate_report__when_csv__and_empty(test_client, mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.get_docs", MagicMock())
    report = RetrievalTimeDetailedReport(title="Test Report", start_date="1970-01-01", end_date="1970-01-01", timestamp="1970-01-01")

    # ACT
    csv_report = report.generate_report("text/csv")

    # ASSERT
    with open(csv_report.name) as fp:
        assert fp.read() == (
            "Title: OPERA Retrieval Time Log\n"
            "Date of Report: 1970-01-01T00:00:00Z\n"
            "Period of Coverage (AcquisitionTime): 1970-01-01T00:00:00Z - 1970-01-01T00:00:00Z\n"
            "PublicAvailableDateTime: datetime when the product was first made available to the public by the DAAC.\n"
            "OperaDetectDateTime: datetime when the OPERA system first became aware of the product.\n"
            "ProductReceivedDateTime: datetime when the product arrived in our system\n"
            "\n"
        )


def test_to_report_df__when_empty_db(test_client):
    # ARRANGE
    report = RetrievalTimeDetailedReport(title="Test Report", start_date="1970-01-01", end_date="1970-01-01", timestamp="1970-01-01")

    # ACT
    report_df = report.to_report_df(dataset_docs=[], report_type="detailed", start="1970-01-01", end="1970-01-01")

    # ASSERT
    assert_frame_equal(report_df, pandas.DataFrame())


def test_to_report_df__when_no_output_products__and_detailed_report(test_client, mocker: MockerFixture):
    # ARRANGE
    report = RetrievalTimeDetailedReport(title="Test Report", start_date="1970-01-01", end_date="1970-01-01", timestamp="1970-01-01")
    mocker.patch("accountability_api.api_utils.reporting.retrieval_time_detailed_report.RetrievalTimeReport.augment_hls_products_with_hls_spatial_info", MagicMock())
    mocker.patch("accountability_api.api_utils.reporting.retrieval_time_detailed_report.RetrievalTimeReport.augment_hls_products_with_hls_info", MagicMock())
    mocker.patch("accountability_api.api_utils.reporting.retrieval_time_detailed_report.RetrievalTimeReport.augment_hls_products_with_sds_product_info", MagicMock())

    # ACT
    report_df = report.to_report_df(
        dataset_docs=[
            {
                "_id": "dummy_id",
                "dataset_type": "L2_HLS_L30",
                "daac_CNM_S_timestamp": "1970-01-01",
                "metadata": {
                    "ProductReceivedTime": "1970-01-01",
                    "FileName": "dummy_opera_product_name",
                    "ProductType": "dummy_opera_product_short_name"
                }
            }
        ],
        report_type="detailed",
        start="1970-01-01",
        end="1970-01-01"
    )

    # ASSERT
    assert_frame_equal(report_df, pandas.DataFrame())
