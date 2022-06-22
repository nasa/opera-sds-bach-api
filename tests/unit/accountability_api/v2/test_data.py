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
        "size": 40,
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
    assert len(data) == 3
    assert data == {
        "L3_DSWX_HLS": "grq_1_l3_dswx_hls",
        "HLS_L30": "grq_1_l2_hls_l30",
        "HLS_S30": "grq_1_l2_hls_s30"
    }


def test_ListDataTypeCounts_get_all(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_1_l3_dswx_hls": 1,
        "grq_1_l2_hls_l30": 2,
        "grq_1_l2_hls_s30": 3
    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count")  # note lack of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None,
        "workflow_start": None,
        "workflow_end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "HLS_L30": "grq_1_l2_hls_l30",
        "HLS_S30": "grq_1_l2_hls_s30",
        "DSWX_HLS": "grq_1_l3_dswx_hls"
    }, **get_docs_args)

    assert response.status_code == 200
    assert len(data) == 3
    assert data == [
        {"id": "grq_1_l3_dswx_hls", "count": 1},
        {"id": "grq_1_l2_hls_l30", "count": 2},
        {"id": "grq_1_l2_hls_s30", "count": 3}
    ]


def test_ListDataTypeCounts_get_incoming(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_1_l2_hls_l30": 2,
        "grq_1_l2_hls_s30": 3
    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count?category=incoming")  # note inclusion of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None,
        "workflow_start": None,
        "workflow_end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "HLS_L30": "grq_1_l2_hls_l30",
        "HLS_S30": "grq_1_l2_hls_s30"
    }, **get_docs_args)

    assert response.status_code == 200
    assert len(data) == 2
    assert data == [
        {"id": "grq_1_l2_hls_l30", "count": 2},
        {"id": "grq_1_l2_hls_s30", "count": 3}
    ]


def test_ListDataTypeCounts_get_outgoing(test_client: FlaskClient, mocker: MockerFixture):
    # ARRANGE
    get_num_docs_mock: MagicMock = mocker.patch("accountability_api.api_utils.query.get_num_docs", return_value={
        "grq_1_l3_dswx_hls": 1
    })

    # ACT
    response: TestResponse = test_client.get("/data/list/count?category=outgoing")  # note inclusion of query param "category"
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    get_docs_args = {
        "start": None,
        "end": None,
        "workflow_start": None,
        "workflow_end": None
    }
    get_num_docs_mock.assert_called_once_with({
        "DSWX_HLS": "grq_1_l3_dswx_hls"
    }, **get_docs_args)

    assert response.status_code == 200
    assert len(data) == 1
    assert data == [
        {"id": "grq_1_l3_dswx_hls", "count": 1}
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
        "size": 40,
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
