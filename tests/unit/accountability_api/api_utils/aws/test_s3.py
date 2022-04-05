from accountability_api.api_utils.aws.s3 import AWSS3


def test_to_s3_url():
    # ARRANGE
    s3 = AWSS3()
    s3.bucket = "dummy_bucket"
    s3.s3_key = "dummy_key"

    # ACT
    s3_url = s3.to_s3_url()

    # ASSERT
    assert s3_url == "s3://dummy_bucket/dummy_key"


def test_from_http_url():
    # ARRANGE
    s3 = AWSS3()

    # ACT
    s3 = s3.from_http_url("http://dummy_bucket.us-west-2.amazonaws.com/dummy_key")

    # ASSERT
    assert s3.bucket == "dummy_bucket"
    assert s3.s3_key == "dummy_key"


def test_from_s3_url():
    # ARRANGE
    s3 = AWSS3()

    # ACT
    s3 = s3.from_s3_url("s3://dummy_bucket/dummy_key")

    # ASSERT
    assert s3.bucket == "dummy_bucket"
    assert s3.s3_key == "dummy_key"
