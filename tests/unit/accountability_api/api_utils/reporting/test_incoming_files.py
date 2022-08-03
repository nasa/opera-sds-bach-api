import json

from pytest_mock import MockerFixture

from accountability_api.api_utils.reporting.incoming_files import IncomingFiles


def test_generate_report_json_sdp(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    incoming_files = IncomingFiles(title="", start_date="1970-01-01T00:00:00", end_date="1970-01-02T00:00:00", timestamp="1970-01-03T00:00:00")

    # ACT
    report = incoming_files.generate_report(output_format="json")
    report = json.loads(report)

    # ASSERT
    assert report["root_name"] == "INCOMING_SDP_PRODUCTS_REPORT"
    assert report["header"]["time_of_report"] == "1970-01-03T00:00:00Z"
    assert report["header"]["data_received_time_range"] == "1970-01-01T00:00:00Z - 1970-01-02T00:00:00Z"
    assert len(report["products"]) > 0
    for product in report["products"]:
        assert product["name"] in ["HLS_S30", "HLS_L30"]


def test_generate_report_dict_sdp(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    incoming_files = IncomingFiles(title="", start_date="1970-01-01T00:00:00", end_date="1970-01-02T00:00:00", timestamp="1970-01-03T00:00:00")

    # ACT
    report = incoming_files.generate_report(output_format="dict")  # note the unrecognized format. used to trigger fallback format.

    # ASSERT
    assert report["root_name"] == "INCOMING_SDP_PRODUCTS_REPORT"
    assert report["header"]["time_of_report"] == "1970-01-03T00:00:00Z"
    assert report["header"]["data_received_time_range"] == "1970-01-01T00:00:00Z - 1970-01-02T00:00:00Z"
    assert len(report["products"]) > 0
    for product in report["products"]:
        assert product["name"] in ["HLS_S30", "HLS_L30"]


def test_generate_report_csv_sdp(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    incoming_files = IncomingFiles(title="", start_date="1970-01-01T00:00:00", end_date="1970-01-02T00:00:00", timestamp="1970-01-03T00:00:00")

    # ACT
    report = incoming_files.generate_report(output_format="csv")

    # ASSERT
    assert report == "name\tnum_ingested\tvolume\n" \
                     "HLS_L30\t0\t0\n" \
                     "HLS_S30\t0\t0\n"


def test_generate_report_json_ancillary(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    dummy_args = {
        "title": "",
        "start_date": "1970-01-01T00:00:00",
        "end_date": "1970-01-02T00:00:00",
        "timestamp": "1970-01-03T00:00:00"
    }
    incoming_files = IncomingFiles(report_type="ancillary", **dummy_args)

    # ACT
    report = incoming_files.generate_report(output_format="json")
    report = json.loads(report)

    # ASSERT
    assert report["root_name"] == "INCOMING_ANCILLARY_PRODUCTS_REPORT"
    assert report["header"]["time_of_report"] == "1970-01-03T00:00:00Z"
    assert report["header"]["data_received_time_range"] == "1970-01-01T00:00:00Z - 1970-01-02T00:00:00Z"
    assert len(report["products"]) == 0


def test_generate_report_dict_ancillary(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    dummy_args = {
        "title": "",
        "start_date": "1970-01-01T00:00:00",
        "end_date": "1970-01-02T00:00:00",
        "timestamp": "1970-01-03T00:00:00"
    }
    incoming_files = IncomingFiles(report_type="ancillary", **dummy_args)

    # ACT
    report = incoming_files.generate_report(output_format="dict")  # note the unrecognized format. used to trigger fallback format.

    # ASSERT
    assert report["root_name"] == "INCOMING_ANCILLARY_PRODUCTS_REPORT"
    assert report["header"]["time_of_report"] == "1970-01-03T00:00:00Z"
    assert report["header"]["data_received_time_range"] == "1970-01-01T00:00:00Z - 1970-01-02T00:00:00Z"
    assert len(report["products"]) == 0


def test_generate_report_csv_ancillary(mocker: MockerFixture):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.reporting.incoming_files.query.run_query", return_value={})
    dummy_args = {
        "title": "",
        "start_date": "1970-01-01T00:00:00",
        "end_date": "1970-01-02T00:00:00",
        "timestamp": "1970-01-03T00:00:00"
    }
    incoming_files = IncomingFiles(report_type="ancillary", **dummy_args)

    # ACT
    report = incoming_files.generate_report(output_format="csv")

    # ASSERT
    assert report == "\n"
