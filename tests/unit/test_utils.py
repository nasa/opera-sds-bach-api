import unittest

from accountability_api.api_utils.utils import magnitude, get_orbit_range_list


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

    pass
