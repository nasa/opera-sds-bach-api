from datetime import datetime, timedelta
import unittest

from accountability_api.api_utils.utils import (
    magnitude,
    get_orbit_range_list,
    from_iso_to_dt,
    from_dt_to_iso,
    set_transfer_status,
    to_iso_format_truncated,
    from_td_to_str,
)


class TestUtils(unittest.TestCase):
    def test_magnitude(self):
        """
        testing positive integers for magnitude
        :return:
        """
        self.assertEqual(0, magnitude(1), "magnitude testing")
        self.assertEqual(0, magnitude(4), "magnitude testing")
        self.assertEqual(1, magnitude(10), "magnitude testing")
        self.assertEqual(1, magnitude(88), "magnitude testing")
        self.assertEqual(1, magnitude(99), "magnitude testing")
        self.assertEqual(2, magnitude(299), "magnitude testing")
        self.assertEqual(3, magnitude(2995), "magnitude testing")
        self.assertEqual(4, magnitude(29956), "magnitude testing")
        return

    def test_get_orbit_range_list(self):
        """
        multiple test cases:
        1. nominal one with a huge difference
        2. invalid one
        3. valid. range is the same
        4. valid. range is the same magnitude
        5. 1 magnitude difference (big number)
        6. 1 magnitude difference (small number and start with 0
        :return:
        """
        result = get_orbit_range_list(3, 23345)
        self.assertEqual(5, len(result), "size of returned list is wrong")
        self.assertEqual((3, 9), result[0], "magnitude 0 range is wrong")
        self.assertEqual((10, 99), result[1], "magnitude 1 range is wrong")
        self.assertEqual((100, 999), result[2], "magnitude 2 range is wrong")
        self.assertEqual((1000, 9999), result[3], "magnitude 3 range is wrong")
        self.assertEqual((10000, 23345), result[4], "magnitude 4 range is wrong")
        result = get_orbit_range_list(32222, 23345)
        self.assertEqual(
            1, len(result), "size of returned list is wrong for invalid input"
        )
        self.assertEqual(
            (32222, 23345),
            result[0],
            "size of returned list is wrong for valid input, same number",
        )
        result = get_orbit_range_list(23345, 23345)
        self.assertEqual(
            1,
            len(result),
            "size of returned list is wrong for valid input, same number",
        )
        self.assertEqual(
            (23345, 23345),
            result[0],
            "size of returned list is wrong for valid input, same number",
        )
        result = get_orbit_range_list(23345, 98989)
        self.assertEqual(
            1,
            len(result),
            "size of returned list is wrong for valid input, same magnitude",
        )
        self.assertEqual(
            (23345, 98989),
            result[0],
            "size of returned list is wrong for valid input, same magnitude",
        )
        result = get_orbit_range_list(8787, 12121)
        self.assertEqual(
            2,
            len(result),
            "size of returned list is wrong for valid input, small magnitude diff",
        )
        self.assertEqual(
            (8787, 9999),
            result[0],
            "size of returned list is wrong for valid input, small magnitude diff",
        )
        self.assertEqual(
            (10000, 12121),
            result[1],
            "size of returned list is wrong for valid input, small magnitude diff",
        )
        result = get_orbit_range_list(0, 10)
        self.assertEqual(
            2,
            len(result),
            "size of returned list is wrong for valid input, small magnitude diff & small num",
        )
        self.assertEqual(
            (0, 9),
            result[0],
            "size of returned list is wrong for valid input, small magnitude diff & small num",
        )
        self.assertEqual(
            (10, 10),
            result[1],
            "size of returned list is wrong for valid input, small magnitude diff & small num",
        )
        return

    def test_from_iso_to_dt(self):
        assert from_iso_to_dt("1970-01-01T00:00:00.000000Z") == datetime(1970, 1, 1)
        assert from_iso_to_dt("1970-01-01T00:00:00.000000") == datetime(1970, 1, 1)

        assert from_iso_to_dt("1970-001T00:00:00.000000Z") == datetime(1970, 1, 1)
        assert from_iso_to_dt("1970-001T00:00:00.000000") == datetime(1970, 1, 1)

    def test_from_dt_to_iso(self):
        assert from_dt_to_iso(datetime(1970, 1, 1)) == "1970-01-01T00:00:00.000000Z"
        assert from_dt_to_iso(datetime(1970, 1, 1), custom_format="%Y-%m-%dT%H:%M:%SZ") == "1970-01-01T00:00:00Z"

    def test_set_transfer_status(self):
        es_doc_source = {"dataset_level": "L3", "daac_delivery_status": "SUCCESS", "dataset_type": "L3_DSWx_HLS"}
        assert set_transfer_status(es_doc_source)["transfer_status"] == "cnm_r_success"

        es_doc_source = {"dataset_level": "L3", "daac_delivery_status": "NOT_SUCCESS", "dataset_type": "L3_DSWx_HLS"}
        assert set_transfer_status(es_doc_source)["transfer_status"] == "cnm_r_failure"

        es_doc_source = {"dataset_level": "L3", "daac_CNM_S_status": "SUCCESS", "dataset_type": "L3_DSWx_HLS"}
        assert set_transfer_status(es_doc_source)["transfer_status"] == "cnm_s_success"

        es_doc_source = {"dataset_level": "L3", "daac_CNM_S_status": "NOT_SUCCESS", "dataset_type": "L3_DSWx_HLS"}
        assert set_transfer_status(es_doc_source)["transfer_status"] == "cnm_s_failure"

        es_doc_source = {"dataset_level": "L3", "dataset_type": "L3_DSWx_HLS"}  # note lack of any status
        assert set_transfer_status(es_doc_source)["transfer_status"] == "unknown"

        es_doc_source = {"dataset_level": "L2", "dataset_type": "test_dataset_type_unknown"}  # note L2 dataset_level
        assert set_transfer_status(es_doc_source)["transfer_status"] == "not_applicable"

    def test_to_iso_format_truncated(self):
        # year, month, day, -12:00 (United States Minor Outlying Islands)
        assert to_iso_format_truncated("1970-01-01T12:34:56.789012-12:00") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34:56-12:00") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34-12:00") == "19700101T1234"

        # year, month, day, +13:45 (New Zealand, Chatham Islands)
        assert to_iso_format_truncated("1970-01-01T12:34:56.789012+13:45") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34:56+13:45") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34+13:45") == "19700101T1234"

        # year, month, day, Z
        assert to_iso_format_truncated("1970-01-01T12:34:56.789012Z") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34:56Z") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34Z") == "19700101T1234"

        # year, month, day
        assert to_iso_format_truncated("1970-01-01T12:34:56.789012") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34:56") == "19700101T123456"
        assert to_iso_format_truncated("1970-01-01T12:34") == "19700101T1234"

        # year, day of year, -12:00 (United States Minor Outlying Islands)
        assert to_iso_format_truncated("1970001T12:34:56.789012-12:00") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34:56-12:00") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34-12:00") == "1970001T1234"

        # year, day of year, +13:45 (New Zealand, Chatham Islands)
        assert to_iso_format_truncated("1970001T12:34:56.789012+13:45") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34:56+13:45") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34+13:45") == "1970001T1234"

        # year, day of year, Z
        assert to_iso_format_truncated("1970001T12:34:56.789012Z") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34:56Z") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34Z") == "1970001T1234"

        # year, day of year
        assert to_iso_format_truncated("1970001T12:34:56.789012") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34:56") == "1970001T123456"
        assert to_iso_format_truncated("1970001T12:34") == "1970001T1234"

    def test_from_td_to_str(self):
        assert from_td_to_str(timedelta()) == "000T00:00:00"
        assert from_td_to_str(timedelta(days=1, seconds=60 * 60 + 60 + 1)) == "001T01:01:01"
        assert from_td_to_str(timedelta(days=-1, seconds=60 * 60 + 60 + 1)) == "-001T01:01:01"  # negative delta

        # test long day string
        assert from_td_to_str(timedelta(days=1000)) == "1000T00:00:00"
