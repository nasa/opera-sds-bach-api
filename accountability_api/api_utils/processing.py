import logging
from datetime import datetime, timedelta
from dateutil import parser

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


def get_duration(source):
    created_at = parser.parse(source["created_at"])
    last_modified = parser.parse(source["last_modified"])

    duration = last_modified - created_at

    return duration
def format_l0b_data(results=None):
    data = []
    if results is not None:
        for result in results:
            entry = {}
            entry["datatake_id"] = result
            obs_results = results[result]
            for obs_id in obs_results:
                entry_copy = entry.copy()
                entry_copy["id"] = "{}-{}".format(result, obs_id)
                entry_copy["observation_id"] = obs_id
                if obs_results[obs_id]:
                    entry_copy["CompositeReleaseID"] = obs_results[obs_id]["metadata"][
                        "CompositeReleaseID"
                    ]
                    entry_copy["CycleNumber"] = obs_results[obs_id]["metadata"][
                        "CycleNumber"
                    ]
                    entry_copy["RangeStartDateTime"] = obs_results[obs_id]["metadata"][
                        "ObservationStartDateTime"
                    ]
                    entry_copy["RangeStopDateTime"] = obs_results[obs_id]["metadata"][
                        "ObservationEndDateTime"
                    ]
                    entry_copy["last_modified"] = obs_results[obs_id][
                        "creation_timestamp"
                    ]
                    data.append(entry_copy)
            del entry_copy
    return data
