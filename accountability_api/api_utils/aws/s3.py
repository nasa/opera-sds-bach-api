import boto3
import logging

LOGGER = logging.getLogger()


class AWSS3:
    def __init__(self):
        """"""
        self._client = boto3.Session().client("s3")
        self._bucket = None
        self._key = None
        pass

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, val):
        if val is None or val == "":
            raise ValueError("invalid input bucket name")
        self._bucket = val
        return

    @property
    def s3_key(self):
        return self._key

    @s3_key.setter
    def s3_key(self, val):
        if val is None or val == "":
            raise ValueError("invalid input s3 key")
        self._key = val
        return

    def to_s3_url(self):
        if self.bucket is None or self.s3_key is None:
            raise ValueError("bucket or s3 key is NULL")
        return "s3://{}/{}".format(self.bucket, self.s3_key)

    def from_http_url(self, http_url):
        """
        getting bucket and key from http url
        :param http_url: str - assuming to be in this format <protocol>://<bucket>.<region>.amazonaws.com/<key>
        :return:
        """
        if not http_url.startswith("http://") and not http_url.startswith("https://"):
            raise ValueError("invalid s3 url: {}".format(http_url))
        protocol_index = http_url.find("://") + 4
        split_index = http_url[protocol_index:].find("/")
        self._key = http_url[(protocol_index + split_index + 1) :]
        self.bucket = http_url[protocol_index - 1 : split_index].split(".")[0]
        return self

    def from_s3_url(self, s3_url):
        if not s3_url.startswith("s3://"):
            raise ValueError("invalid s3 url: {}".format(s3_url))
        split_index = s3_url[5:].find("/")
        self._bucket = s3_url[5 : split_index + 5]
        self._key = s3_url[(split_index + 6) :]
        return self

    def get_stream(self):
        if self._bucket is None or self._key is None:
            raise ValueError("invalid bucket or key")
        try:
            return self._client.get_object(Bucket=self._bucket, Key=self._key)["Body"]
        except Exception as e:
            raise ValueError(
                "error while getting the stream for {}. cause: {}".format(
                    self.to_s3_url(), str(e)
                )
            )
            pass

    pass
