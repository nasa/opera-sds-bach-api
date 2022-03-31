import json

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


def test_get(test_client: FlaskClient, mocker: MockerFixture, elasticsearch_utility_stub, monkeypatch):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.get_grq_es", return_value=elasticsearch_utility_stub)
    mocker.patch("accountability_api.api_utils.query.get_docs", return_value=[{
        "daac_delivery_status": "SUCCESS"
    }])
    monkeypatch.setattr("accountability_api.v2.data.consts.ANCILLARY_INDEXES", {})
    monkeypatch.setattr("accountability_api.v2.data.consts.PRODUCT_INDEXES", {
        "test_index_label": "test_index_name"
    })

    # ACT
    response: TestResponse = test_client.get("/data/")
    data = json.loads(response.data.decode(response.charset))

    # ASSERT
    assert response.status_code == 200
    assert len(data) == 1
    assert data[0]["transfer_status"] == "cnm_r_success"
