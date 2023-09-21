from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from accountability_api.api_utils import query


class ElasticsearchUtilityStub:
    es = MagicMock()

    def search(self, **kwargs):
        pass

    def get_count(self, **kwargs):
        pass


@pytest.fixture
def elasticsearch_utility_stub():
    return ElasticsearchUtilityStub()


@pytest.fixture
def elasticsearch_single_product():
    return {
            "hits": {
                "hits": [
                    {
                        "_id": "dummy_id",
                        "_source": {
                            "mock": "yes"
                        }
                    }
                ]
            }
        }


@pytest.fixture
def elasticsearch_no_hits():
    return {
            "hits": {
                "hits": []
            }
        }


@pytest.fixture
def elasticsearch_index_non_empty():
    return {
            "hits": {
                "total": {
                    "value": 2
                },
                "hits": [
                    {
                        "_id": "dummy_id",
                        "_source": {
                            "mock": "yes"
                        }
                    },
                    {
                        "_id": "dummy_id",
                        "_source": {
                            "mock": "yes"
                        }
                    }
                ]
            }
        }


def test_run_query_with_sort(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    elasticsearch_utility_stub.es.search.return_value = {
        "_scroll_id": None,
        "hits": {
            "total": {
                "value": 1
            },
            "hits": [{"mock": "yes", "_id": "dummy_id"}]
        }
    }
    mocker.patch("accountability_api.api_utils.query.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)

    spy = mocker.spy(elasticsearch_utility_stub.es, "search")

    # ACT
    query.run_query_with_scroll(
        index="test_index",
        body="test_body",
        doc_type="test_doc_type",
        sort=["test_sort_field:test_sort_direction"],  # note use of sort
        size=-1
    )

    # ASSERT
    spy.assert_called_with(
        index="test_index",
        body="test_body",
        doc_type="test_doc_type",
        sort=["test_sort_field:test_sort_direction"],  # checking for sort arg
        size=10000,
        scroll="30s"
    )


def test_run_query_no_sort(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    elasticsearch_utility_stub.es.search.return_value = {
        "_scroll_id": None,
        "hits": {
            "total": {
                "value": 0
            },
            "hits": []
        }
    }
    mocker.patch("accountability_api.api_utils.query.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)
    spy = mocker.spy(elasticsearch_utility_stub.es, "search")

    # ACT
    query.run_query_with_scroll(
        index="test_index",
        body="test_body",
        doc_type="test_doc_type",
        # note lack of sort arg
        size=-1
    )

    # ASSERT
    spy.assert_called_with(
        index="test_index",
        body="test_body",
        doc_type="test_doc_type",
        # note lack of sort arg
        size=10000,
        scroll="30s"
    )


def test_get_product_with_hits(mocker: MockerFixture, elasticsearch_single_product):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.run_query_with_scroll", return_value=elasticsearch_single_product)

    # ACT
    retrieved_product = query.get_product("OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0", index="grq_*_*")

    # ASSERT
    assert retrieved_product == {"mock": "yes"}


def test_get_product_no_hits(mocker: MockerFixture, elasticsearch_no_hits):
    # ARRANGE
    from accountability_api.api_utils import query
    mocker.patch("accountability_api.api_utils.query.run_query_with_scroll", return_value=elasticsearch_no_hits)

    # ACT
    retrieved_product = query.get_product("OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0", index="grq_*_*")

    # ASSERT
    assert retrieved_product is None


def test_get_num_docs_in_index(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)
    mocker.patch.object(elasticsearch_utility_stub, "search", return_value={
        "hits": {
            "total": {"value": 3},
            "hits": [
                {"_source": {"metadata": {}}},
                {"_source": {"metadata": {}}},
                {"_source": {"metadata": {}}}
            ]
        }
    })

    # ACT
    # ASSERT
    assert query.get_num_docs_in_index("*") == 3


def test_get_docs_in_index(mocker: MockerFixture, elasticsearch_index_non_empty):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.run_query_with_scroll", return_value=elasticsearch_index_non_empty)

    # ACT
    docs, total = query.get_docs_in_index("grq_1_l3_dswx_hls", start=None, end=None, size=50, time_key=None)

    # ASSERT
    assert total == 2


def test_get_num_docs(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)
    mocker.patch.object(elasticsearch_utility_stub, "search", return_value={
        "hits": {
            "total": {"value": 3},
            "hits": [
                {"_source": {"metadata": {}}},
                {"_source": {"metadata": {}}},
                {"_source": {"metadata": {}}}
            ]
        }
    })

    # ACT
    index_alias_to_count = query.get_num_docs({"test_index_label": "test_index_name"})

    # ASSERT
    assert index_alias_to_count["test_index_label"] == 3
