import pytest
from pytest_mock import MockerFixture

from accountability_api.api_utils import query


class ElasticsearchUtilityStub:
    def search(self, **kwargs):
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
                        "_source": {
                            "mock": "yes"
                        }
                    },
                    {
                        "_source": {
                            "mock": "yes"
                        }
                    }
                ]
            }
        }


def test_run_query_with_sort(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    mocker.patch("accountability_api.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)

    spy = mocker.spy(elasticsearch_utility_stub, "search")

    # ACT
    query.run_query(
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
        size=-1,
        params={}
    )


def test_run_query_no_sort(mocker: MockerFixture, elasticsearch_utility_stub):
    # ARRANGE
    mocker.patch("accountability_api.es_connection.get_grq_es", return_value=elasticsearch_utility_stub)

    spy = mocker.spy(elasticsearch_utility_stub, "search")

    # ACT
    query.run_query(
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
        size=-1,
        params={}
    )


def test_get_product_with_hits(mocker: MockerFixture, elasticsearch_single_product):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.run_query", return_value=elasticsearch_single_product)

    # ACT
    retrieved_product = query.get_product("OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0", index="grq_*_*")

    # ASSERT
    assert retrieved_product == {"mock": "yes"}


def test_get_product_no_hits(mocker: MockerFixture, elasticsearch_no_hits):
    # ARRANGE
    from accountability_api.api_utils import query
    mocker.patch("accountability_api.api_utils.query.run_query", return_value=elasticsearch_no_hits)

    # ACT
    retrieved_product = query.get_product("OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0", index="grq_*_*")

    # ASSERT
    assert retrieved_product is None


def test_get_docs_in_index(mocker: MockerFixture, elasticsearch_index_non_empty):
    # ARRANGE
    mocker.patch("accountability_api.api_utils.query.run_query", return_value=elasticsearch_index_non_empty)

    # ACT
    docs, total = query.get_docs_in_index("grq_1_l3_dswx_hls", start=None, end=None, size=50, time_key=None)

    # ASSERT
    assert total == 2
