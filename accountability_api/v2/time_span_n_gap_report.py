from __future__ import division
from accountability_api.api_utils import query
from datetime import datetime


def getTimeSpanNGapReport(
    productType, startTime=None, endTime=None, startOrbit=None, endOrbit=None
):
    """
    issue with sorting and range queries
    not all types have metadata.OrbitNumber, metadata.StartOrbit mapping

    need to adjust fields and sorts for each product type

    Update: removing the sort because of this error.
        Error: SearchParseException[[grq_00000_spicesclk][0]: query[metadata.RangeBeginningDate:[2019-01-01 TO 2019-12-02]],from[-1],size[-1]: Parse Failure [Failed to parse source [{"query": {"range": {"metadata.RangeBeginningDate": {"gte": "2019-01-01", "lte": "2019-12-02"}}}, "sort": [{"metadata.RangeBeginningDateTime": "asc"}]}]]]; nested: SearchParseException[[grq_00000_spicesclk][0]: query[metadata.RangeBeginningDate:[2019-01-01 TO 2019-12-02]],from[-1],size[-1]: Parse Failure [No mapping found for [metadata.RangeBeginningDateTime] in order to sort on]]

        Oh, just read the above comment. The problem is that the index have other doc_types
            which don't have those fields.
        TODO need a better fix
    """

    if not startTime and not endTime and not startOrbit and not endOrbit:
        return []
    report = []
    if startTime and endTime:
        r = query.construct_range_object(
            "metadata.RangeBeginningDate", startTime[:10], endTime[:10]
        )
        s = [{"metadata.RangeBeginningDateTime": "asc"}]
        body = {
            "query": {"range": r},
            # 'sort': s
        }

    elif startOrbit and endOrbit:
        r = query.construct_range_object("metadata.OrbitNumber", startOrbit, endOrbit)
        s = [{"metadata.OrbitNumber": "asc"}]
        body = {"query": {"range": r}, "sort": s}

    data = query.run_query(doc_type=productType, body=body)

    if data["hits"]["total"] == 0:
        return []

    hits = data["hits"]["hits"]
    prevT = datetime.strptime(
        hits[0]["_source"]["metadata"]["RangeBeginningDateTime"],
        "%Y-%m-%dT%H:%M:%S.%fZ",
    )
    for elem in hits:
        # if score == 1

        metadata = elem["_source"]["metadata"]

        file = metadata["Filename"]
        begin = metadata["RangeBeginningDateTime"]
        end = metadata["RangeEndingDateTime"]

        t1 = datetime.strptime(begin, "%Y-%m-%dT%H:%M:%S.%fZ")
        t2 = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%fZ")

        timeSpan = (t2 - t1).total_seconds() / 60
        timeGap = (t1 - prevT).total_seconds() / 60

        prevT = t2

        report.append(
            {
                "FileName": file,
                "RangeBeginningDateTime": begin,
                "RangeEndingDateTime": end,
                "TimeSpan(mins)": "{:.3f}".format(timeSpan),
                "TimeGap(mins)": "{:.3f}".format(timeGap),
                "Gap Between Files": "YES" if timeGap > 0 else "NO",
            }
        )

    return report
