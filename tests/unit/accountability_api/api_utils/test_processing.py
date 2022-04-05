from datetime import datetime, timedelta

from accountability_api.api_utils import processing


def test_gt():
    assert type(processing.gt("1970-01-01T01:01:01.123456Z")) is datetime
    assert processing.gt("1970-01-01T01:01:01.123456Z").tzinfo is None


def test_get_duration():
    es_doc_source = {
        "created_at": "1970-01-01T00:00:00.000000Z",
        "last_modified": "1971-01-01T00:00:01.000001Z"
    }
    assert processing.get_duration(es_doc_source) == timedelta(days=365, seconds=1, microseconds=1)
