from pytest_mock import MockerFixture


def test_get_product(mocker: MockerFixture):
    """
    Test grabbing 3 different products
    """
    class ElasticsearchUtilityStub:
        def search(self, **kwargs):
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
    mocker.patch("accountability_api.es_connection.get_grq_es", return_value=ElasticsearchUtilityStub())

    from accountability_api.api_utils import query

    products = [
        "OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0",
        "OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0",
    ]

    for id in products:
        print(f"looking for {id}")
        retrieved_product = query.get_product(id, index="grq_*_*")
        assert retrieved_product is not None


def test_get_docs_in_index(mocker: MockerFixture):
    """
    Test that the correct number of docs are being retrieved from 3 different indexes
    """
    class ElasticsearchUtilityStub:
        def search(self, **kwargs):
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
                        }
                    ]
                }
            }
    mocker.patch("accountability_api.es_connection.get_grq_es", return_value=ElasticsearchUtilityStub())

    from accountability_api.api_utils import query

    indexes = [
        ("grq_1_l3_dswx_hls", 2),
    ]
    for index, count in indexes:
        documents = query.get_docs_in_index(
            index, start=None, end=None, size=50, time_key=None
        )
        assert documents[1] == count
