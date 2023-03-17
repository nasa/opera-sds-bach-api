import json
from unittest.mock import MagicMock

import pytest
from werkzeug.test import TestResponse
from flask.testing import FlaskClient
from pytest_mock import MockerFixture


class ElasticsearchUtilityStub:
    def search(self, **kwargs):
        pass

    def get_count(self, **kwargs):
        pass


@pytest.fixture
def elasticsearch_utility_stub():
    return ElasticsearchUtilityStub()


def test_Data_get(test_client: FlaskClient, mocker: MockerFixture, elasticsearch_utility_stub, monkeypatch):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.get_grq_es", return_value=elasticsearch_utility_stub)
    get_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_docs", return_value=[{
        "dataset_level": "L3",
        "daac_delivery_status": "SUCCESS",
        "test_extra_attribute": "dummy_value"
    }])
    monkeypatch.setattr("accountability_api.v2.data.consts.INPUT_PRODUCT_TYPE_TO_INDEX", {})
    monkeypatch.setattr("accountability_api.v2.data.consts.PRODUCT_TYPE_TO_INDEX", {
        "test_index_label": "test_index_name"
    })

    # ACT
    response: TestResponse = test_client.get("/data/")
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None,
        "size": -1,
        "metadata_tile_id": None,
        "metadata_sensor": None
    }
    get_docs_mock.assert_called_once_with("test_index_name", **get_docs_args)

    assert response.status_code == 200
    assert len(data) == 1

    # check transfer status
    assert data[0]["transfer_status"] == "cnm_r_success"

    # check minimization
    assert "test_extra_attribute" not in data[0]


def test_ListDataTypes_get(test_client: FlaskClient):
    # ARRANGE
    pass

    # ACT
    response: TestResponse = test_client.get("/data/list")
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    assert response.status_code == 200
    assert data == {
        "L3_DSWX_HLS": "grq_*_l3_dswx_hls",
        "HLS_L30": "grq_*_l2_hls_l30",
        "HLS_S30": "grq_*_l2_hls_s30",

        "L2_CSLC_S1": "grq_*_l2_cslc_s1",
        "L2_RTC_S1": "grq_*_l2_rtc_s1",
        "L1_S1_SLC": "grq_*_l1_s1_slc",
    }


def test_ListDataTypeCounts_get_all(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_*_l3_dswx_hls": 1,
        "grq_*_l2_hls_l30": 2,
        "grq_*_l2_hls_s30": 3,

        "grq_*_l2_cslc_s1": 4,
        "grq_*_l2_rtc_s1": 5,
        "grq_*_l1_s1_slc": 6
    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count")  # note lack of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "HLS_L30": "grq_*_l2_hls_l30",
        "HLS_S30": "grq_*_l2_hls_s30",
        "DSWX_HLS": "grq_*_l3_dswx_hls",

        "L2_CSLC_S1": "grq_*_l2_cslc_s1",
        "L2_RTC_S1": "grq_*_l2_rtc_s1",
        "L1_S1_SLC": "grq_*_l1_s1_slc",
    }, **get_docs_args)

    assert response.status_code == 200
    assert data == [
        {"id": "grq_*_l3_dswx_hls", "count": 1},
        {"id": "grq_*_l2_hls_l30", "count": 2},
        {"id": "grq_*_l2_hls_s30", "count": 3},
        {"id": "grq_*_l2_cslc_s1", "count": 4},
        {"id": "grq_*_l2_rtc_s1", "count": 5},
        {"id": "grq_*_l1_s1_slc", "count": 6},
    ]


def test_ListDataTypeCounts_get_incoming(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_*_l2_hls_l30": 2,
        "grq_*_l2_hls_s30": 3,
        "grq_*_l1_s1_slc": 6,

    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count?category=incoming")  # note inclusion of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "HLS_L30": "grq_*_l2_hls_l30",
        "HLS_S30": "grq_*_l2_hls_s30",
        "L1_S1_SLC": "grq_*_l1_s1_slc"
    }, **get_docs_args)

    assert response.status_code == 200
    assert data == [
        {"id": "grq_*_l2_hls_l30", "count": 2},
        {"id": "grq_*_l2_hls_s30", "count": 3},
        {"id": "grq_*_l1_s1_slc", "count": 6},
    ]


def test_ListDataTypeCounts_get_outgoing(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_*_l3_dswx_hls": 1,
        "grq_*_l2_cslc_s1": 4,
        "grq_*_l2_rtc_s1": 5,
    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count?category=outgoing")  # note inclusion of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "DSWX_HLS": "grq_*_l3_dswx_hls",
        "L2_CSLC_S1": "grq_*_l2_cslc_s1",
        "L2_RTC_S1": "grq_*_l2_rtc_s1"
    }, **get_docs_args)

    assert response.status_code == 200
    assert data == [
        {"id": "grq_*_l3_dswx_hls", "count": 1},
        {"id": "grq_*_l2_cslc_s1", "count": 4},
        {"id": "grq_*_l2_rtc_s1", "count": 5}
    ]


def test_DataIndex_get(test_client: FlaskClient, mocker: MockerFixture, monkeypatch):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.get_grq_es", return_value=elasticsearch_utility_stub)
    get_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_docs", return_value=[{
        "dataset_level": "L3",
        "daac_delivery_status": "SUCCESS",
        "test_extra_attribute": "dummy_value"
    }])
    monkeypatch.setattr("accountability_api.v2.data.consts.INPUT_PRODUCT_TYPE_TO_INDEX", {})
    monkeypatch.setattr("accountability_api.v2.data.consts.PRODUCT_TYPE_TO_INDEX", {
        "test_index_label": "test_index_name"
    })

    # ACT
    response: TestResponse = test_client.get("/data/test_index_label")  # note the index_name path param
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "time_key": "created_at",
        "start": None,
        "end": None,
        "size": -1,
        "metadata_tile_id": None,
        "metadata_sensor": None
    }
    get_docs_mock.assert_called_once_with("test_index_name", **get_docs_args)

    assert response.status_code == 200
    assert len(data) == 1

    # check transfer status
    assert data[0]["transfer_status"] == "cnm_r_success"

    # check minimization
    assert "test_extra_attribute" not in data[0]
