# class Test
import unittest

from accountability_api.api_utils import query


class TestQuery(unittest.TestCase):
    def test_construct_orbit_range_obj_01(self):
        """
        start only and end only test cases
        :return:
        """
        start_only_range = query.construct_range_object("OrbitNumber", 23)
        self.assertTrue(
            "OrbitNumber" in start_only_range, "OrbitNumber not in start_only_result"
        )
        self.assertTrue(
            "gte" in start_only_range["OrbitNumber"], "gte not in start_only_result"
        )
        self.assertEqual(
            23, start_only_range["OrbitNumber"]["gte"], "wrong range start_only_result"
        )
        self.assertTrue(
            "lte" not in start_only_range["OrbitNumber"], "lte in start_only_result"
        )
        self.assertTrue(
            "lt" not in start_only_range["OrbitNumber"], "lt in start_only_result"
        )
        end_only_range = query.construct_range_object("OrbitNumber", None, 23, False)
        self.assertTrue(
            "OrbitNumber" in end_only_range, "OrbitNumber not in end_only_result"
        )
        self.assertTrue(
            "lt" in end_only_range["OrbitNumber"], "lt not in end_only_result"
        )
        self.assertEqual(
            23, end_only_range["OrbitNumber"]["lt"], "wrong range end_only_result"
        )
        self.assertTrue(
            "gte" not in end_only_range["OrbitNumber"], "gte in end_only_result"
        )
        self.assertTrue(
            "gt" not in end_only_range["OrbitNumber"], "gt in end_only_result"
        )
        return

    def test_construct_orbit_range_obj_02(self):
        """
        start & end range with same magnitude
        :return:
        """
        single_range = query.construct_range_object("OrbitNumber", 1234, 5678)
        self.assertTrue(
            "OrbitNumber" in single_range, "OrbitNumber not in single_range_result"
        )
        self.assertTrue(
            "lte" in single_range["OrbitNumber"], "lte not in single_range_result"
        )
        self.assertTrue(
            "gte" in single_range["OrbitNumber"], "gte not in single_range_result"
        )
        self.assertEqual(
            1234,
            single_range["OrbitNumber"]["gte"],
            "wrong start range single_range_result",
        )
        self.assertEqual(
            5678,
            single_range["OrbitNumber"]["lte"],
            "wrong end range single_range_result",
        )
        return

    def test_construct_orbit_range_obj_03(self):
        """
        start & end range with 1 difference in magnitude
        :return:
        """
        double_range = query.construct_range_object("OrbitNumber", 123, 5678)
        self.assertTrue(
            "OrbitNumber" in double_range, "OrbitNumber not in double_range_result:1"
        )
        self.assertTrue(
            "lte" in double_range["OrbitNumber"], "lte not in double_range_result:1"
        )
        self.assertTrue(
            "gte" in double_range["OrbitNumber"], "gte not in double_range_result:1"
        )
        self.assertEqual(
            123,
            double_range["OrbitNumber"]["gte"],
            "wrong start range double_range_result:1",
        )
        self.assertEqual(
            5678,
            double_range["OrbitNumber"]["lte"],
            "wrong end range double_range_result:1",
        )
        return

    def test_construct_orbit_range_obj_04(self):
        """
        start & end range with 4 difference in magnitude
        :return:
        """
        multiple_range = query.construct_range_object("OrbitNumber", 12, 10000)
        self.assertTrue(
            "OrbitNumber" in multiple_range,
            "OrbitNumber not in multiple_range_result:1",
        )
        self.assertTrue(
            "lte" in multiple_range["OrbitNumber"], "lte not in multiple_range_result:1"
        )
        self.assertTrue(
            "gte" in multiple_range["OrbitNumber"], "gte not in multiple_range_result:1"
        )
        self.assertEqual(
            12,
            multiple_range["OrbitNumber"]["gte"],
            "wrong start range multiple_range_result:1",
        )
        self.assertEqual(
            10000,
            multiple_range["OrbitNumber"]["lte"],
            "wrong end range multiple_range_result:1",
        )
        return

    def test_construct_orbit_range_obj_05(self):
        """
        invalid range resulting in a single range
        :return:
        """
        invalid_range = query.construct_range_object("OrbitNumber", 1234, 1233)
        self.assertTrue(
            "OrbitNumber" in invalid_range, "OrbitNumber not in single_range_result"
        )
        self.assertTrue(
            "lte" in invalid_range["OrbitNumber"], "lte not in single_range_result"
        )
        self.assertTrue(
            "gte" in invalid_range["OrbitNumber"], "gte not in single_range_result"
        )
        self.assertEqual(
            1234,
            invalid_range["OrbitNumber"]["gte"],
            "wrong start range single_range_result",
        )
        self.assertEqual(
            1233,
            invalid_range["OrbitNumber"]["lte"],
            "wrong end range single_range_result",
        )
        return

    # def test_get_downlink_data(self):
    #     """
    #     Test grabbing all pass data and formatting it.
    #     There should be as many pass entries as there are ldfs (expected[1] is the number of results retrieved).
    #     """
    #     data, num_expected = query.get_docs_in_index(
    #         "pass_accountability_catalog", start=None, end=None, size=50, time_key=None
    #     )
    #     pass_data = query.get_downlink_data()
    #     self.assertEqual(len(pass_data), num_expected)
    #     return

    def test_get_product(self):
        """
        Test grabbing 3 different products
        """
        self.maxDiff = None
        products = [
            "ASF_NISAR_2022_008_06_30_59_ARP",
            "NISAR_L0_PR_RRSD_001_001_D_132S_20220108T171156_20220108T171216_D00200_M_001",
            "COP_e2020-041_c2020-041_v001",
        ]

        # retrieved_products = []
        for _id in products:
            print("looking for %s" % _id)
            retrieved_product = query.get_product(_id, index="grq_*_*")
            self.assertIsNotNone(retrieved_product)
        return

    def test_get_docs_in_index(self):
        """
        Test that the correct number of docs are being retrieved from 3 different indexes
        """
        indexes = [
            ("cop_catalog", 100),
            ("grq_1_ldf", 12),
        ]
        for index, count in indexes:
            documents = query.get_docs_in_index(
                index, start=None, end=None, size=50, time_key=None
            )
            self.assertEqual(documents[1], count)
        return
