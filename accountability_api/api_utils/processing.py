# from accountability_api.api_utils.metadata import *
import logging
from datetime import datetime, timedelta
from dateutil import parser

# from pandas import DataFrame as df

LOGGER = logging.getLogger()


def gt(dt_str):
    dt, _, us = dt_str.partition(".")
    dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
    us = int(us.rstrip("Z"), 10)
    return dt + timedelta(microseconds=us)


def _get_processed_volume(results):
    total_volume = 0
    for entry in results:
        source = entry.get("_source")
        if source is not None:
            metadata = source.get("metadata")
            if metadata is not None:
                total_volume += metadata.get("FileSize")

    return total_volume


def clean_l0b_metadata(l0b_metadata):
    result = {}
    if l0b_metadata is None or l0b_metadata.get("metadata", None) is None:
        return None
    else:
        for key in l0b_metadata["metadata"]:
            result[key] = l0b_metadata["metadata"][key]

    return result


def get_duration(source):
    created_at = parser.parse(source["created_at"])
    last_modified = parser.parse(source["last_modified"])

    duration = last_modified - created_at

    return duration


def format_downlink_data(results=None):
    records = []
    for entry in results:
        # id = entry.get("_id")
        entry = entry.get("_source")
        entry["id"] = entry["ldf_id"]
        del entry["ldf_id"]
        duration = get_duration(entry)
        entry["duration"] = duration.total_seconds() / 60
        # set end_time as unknown for now
        entry["end_time"] = "N/A"
        records.append(entry)
    return records


def format_data(results=None):
    if results is not None:
        data = []
        for result in results:
            cleaned = {
                "id": result["_id"],
            }

            cleaned.update(result["_source"])
            data.append(cleaned)
        return data
    return None


def seperate_observations(datatakes):
    data = []
    for dt in datatakes:
        ids = dt["observation_ids"]
        del dt["observation_ids"]
        for obs in ids:
            datatake_obj = dt.copy()
            datatake_obj["observation_id"] = obs
            data.append(datatake_obj)
    return data


def get_l0b_info(l0b_product):
    data = {
        "CompositeReleaseID": "not found",
        "CycleNumber": -1,
        "RangeStartDateTime": "not found",
        "RangeStopDateTime": "not found",
        "ProcessingType": "not found",
    }
    if "metadata" in l0b_product:
        metadata = l0b_product["metadata"]
        # finding CRID
        if "CompositeReleaseID" in metadata:
            data["CompositeReleaseID"] = l0b_product["metadata"]["CompositeReleaseID"]
        # finding Cycle
        if "CycleNumber" in metadata:
            data["CycleNumber"] = l0b_product["metadata"]["CycleNumber"]
        # finding Observations range start date
        if "RangeStartDateTime" in metadata:
            data["RangeStartDateTime"] = l0b_product["metadata"]["RangeStartDateTime"]
        # finding Observations Range stop date
        if "RangeStopDateTime" in metadata:
            data["RangeStopDateTime"] = l0b_product["metadata"]["RangeStopDateTime"]
        # finding Processing Type
        if "ProcessingType" in metadata:
            data["ProcessingType"] = l0b_product["metadata"]["ProcessingType"]

    return data


# def make_job_str(num_chars):
#     p = ["a", "b", "c", "d", "e", "f", "1", "2", "3", "4", "5", "6","7","8","9","0"]
#     return "".join(random.choices(p, k=num_chars))

# def gen_job_id():
#     return "{}-{}-{}-{}-{}".format(make_job_str(8), make_job_str(4), make_job_str(4), make_job_str(4), make_job_str(12))

# def make_job_status():
#     options = ["completed", "started", "failed", "queued"]
#     return "".join(random.choices(options))

# def gen_job_status():
#     return "job-{}".format(make_job_status())
