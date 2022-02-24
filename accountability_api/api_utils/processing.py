import logging
from datetime import datetime, timedelta
from dateutil import parser

from accountability_api.api_utils import metadata as consts

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
    for ldf in results:
        new_record = {}
        # id = entry.get("_id")
        new_record["start_time"] = results[ldf]["workflow_start"]
        if "Z" not in new_record["start_time"]:
            new_record["start_time"] = "{}Z".format(new_record["start_time"])
        del results[ldf]["workflow_start"]
        state_configs = results[ldf]
        for state_config in results[ldf]:
            new_record["id"] = ldf
            i = 0
            while True:
                if "@timestamp" not in state_configs[state_config]:
                    l0a_pps = state_configs[state_config]
                    for l0a_l_rrst_pp in l0a_pps:
                        record_copy = new_record.copy()
                        record_copy["L0A_L_RRST_PP_id"] = l0a_l_rrst_pp
                        duration = parser.parse(
                            l0a_pps[l0a_l_rrst_pp]["@timestamp"]
                        ) - parser.parse(record_copy["start_time"])
                        if l0a_pps[l0a_l_rrst_pp]["dataset"] == "L0A_L_RRST":

                            record_copy["L0A_L_RRST_id"] = l0a_pps[l0a_l_rrst_pp]["id"]
                            record_copy["end_time"] = l0a_pps[l0a_l_rrst_pp][
                                "@timestamp"
                            ]
                        else:
                            record_copy["L0A_L_RRST_id"] = "N/A"
                            record_copy["end_time"] = "N/A"

                        record_copy["duration"] = duration.total_seconds() / 60
                        record_copy["vcid"] = l0a_pps[l0a_l_rrst_pp]["metadata"][
                            "VCID"
                        ].lower()
                        record_copy["last_modified"] = l0a_pps[l0a_l_rrst_pp][
                            "@timestamp"
                        ]
                        records.append(record_copy)
                    # process as l0a_l_rrst_pp
                else:
                    state_config_dict = state_configs[state_config]
                    record_copy = new_record.copy()
                    duration = parser.parse(
                        state_config_dict["@timestamp"]
                    ) - parser.parse(record_copy["start_time"])
                    record_copy["vcid"] = state_config_dict["metadata"]["vcid"]
                    record_copy["end_time"] = "N/A"
                    record_copy["duration"] = duration.total_seconds() / 60
                    record_copy["last_modified"] = state_config_dict["@timestamp"]
                    records.append(record_copy)
                i += 1
                if i < len(state_configs):
                    continue
                else:
                    break
    return records


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


def get_track_frame_entry(l0b_l_rrsd, obs_id, rslc_id, track="", frame="", coverage=""):

    entry = {
        "id": "{}-{}".format(l0b_l_rrsd, rslc_id),
        "observation_id": obs_id,
        "L0B_L_RRSD": l0b_l_rrsd,
        "L1_L_RSLC": rslc_id,
        "Track": track,
        "Frame": frame,
        "frame_coverage": coverage,
    }

    return entry


def format_track_frame_data(results=None):
    entries = []

    for l0b_l_rrsd in results:
        l0b_dict = results[l0b_l_rrsd]

        track = ""
        frame = ""

        if "L1_L_RSLC" in l0b_dict:
            if l0b_dict["L1_L_RSLC"]:
                for l1_rslc in l0b_dict["L1_L_RSLC"]:
                    if "track" in l0b_dict["L1_L_RSLC"][l1_rslc]:
                        track = l0b_dict["L1_L_RSLC"][l1_rslc]["track"]
                        del l0b_dict["L1_L_RSLC"][l1_rslc]["track"]

                    if "frame" in l0b_dict["L1_L_RSLC"][l1_rslc]:
                        frame = l0b_dict["L1_L_RSLC"][l1_rslc]["frame"]
                        del l0b_dict["L1_L_RSLC"][l1_rslc]["frame"]

                    if "coverage" in l0b_dict["L1_L_RSLC"][l1_rslc]:
                        coverage = l0b_dict["L1_L_RSLC"][l1_rslc]["coverage"]
                        del l0b_dict["L1_L_RSLC"][l1_rslc]["coverage"]

                    rslc_dict = l0b_dict["L1_L_RSLC"][l1_rslc]

                    entry = get_track_frame_entry(
                        l0b_l_rrsd,
                        l0b_dict["observation_id"],
                        l1_rslc,
                        track=track,
                        frame=frame,
                        coverage=coverage,
                    )

                    for dataset in consts.RSLC_CHILDREN:
                        if dataset in rslc_dict:

                            keys = list(rslc_dict[dataset].keys())

                            entry[dataset] = rslc_dict[dataset][keys[0]]["id"]

                            if dataset == "L1_L_RIFG":
                                entry["sec_ref_cycle"] = "{}/{}".format(
                                    rslc_dict[dataset][keys[0]]["metadata"][
                                        "SecondaryCycleNumber"
                                    ],
                                    rslc_dict[dataset][keys[0]]["metadata"][
                                        "ReferenceCycleNumber"
                                    ],
                                )
                        else:
                            entry[dataset] = ""
                    entries.append(entry)
        else:
            entry = get_track_frame_entry(
                l0b_l_rrsd, l0b_dict["observation_id"], "", track=track, frame=frame
            )
            entries.append(entry)

    return entries


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
