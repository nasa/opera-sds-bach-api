# class Test
import unittest

from accountability_api.api_utils import query


class TestQuery(unittest.TestCase):
    def test_get_product(self):
        """
        Test grabbing 3 different products
        """
        self.maxDiff = None
        products = [
            "OPERA_L3_DSWx_HLS_SENTINEL-2A_T15SXR_20210907T163901_v2.0",
            "OPERA_L3_DSWx_HLS_LANDSAT-8_T22VEQ_20210905T143156_v2.0",
        ]

        for id in products:
            print(f"looking for {id}")
            retrieved_product = query.get_product(id, index="grq_*_*")
            self.assertIsNotNone(retrieved_product)
        return

    def test_get_docs_in_index(self):
        """
        Test that the correct number of docs are being retrieved from 3 different indexes
        """
        indexes = [
            ("grq_1_l3_dswx_hls", 2),
        ]
        for index, count in indexes:
            documents = query.get_docs_in_index(
                index, start=None, end=None, size=50, time_key=None
            )
            self.assertEqual(documents[1], count)
        return
