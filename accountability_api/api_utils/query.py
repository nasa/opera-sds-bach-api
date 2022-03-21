# from accountability_api.api_utils.utils import (
#     get_orbit_range_list,
#     ElasticsearchResultDictWrapper,
# )
import os
import logging
from more_itertools import always_iterable
import traceback
import json

from accountability_api.api_utils import GRQ_ES, JOBS_ES
from accountability_api.api_utils import metadata as consts
from accountability_api.api_utils.processing import (
    format_downlink_data,
    format_track_frame_data,
    format_l0b_data,
)

LOGGER = logging.getLogger()


def run_query(
    es=GRQ_ES,
    body=None,
    doc_type=None,
    q=None,
    sort=None,
    size=500,
    index=consts.PRODUCTS_INDEX,
    **kwargs
):

    if sort:
        if q and body is None:
            return es.search(
                index=index, doc_type=doc_type, sort=sort, size=size, q=q, params=kwargs
            )
        try:
            return es.search(
                index=index,
                body=body,
                doc_type=doc_type,
                sort=sort,
                size=size,
                params=kwargs,
            )
        except Exception as e:
            raise (e)
    if q and body is None:
        return es.search(index=index, doc_type=doc_type, size=size, q=q, params=kwargs)
    return es.search(
        index=index, body=body, doc_type=doc_type, size=size, params=kwargs
    )


def run_query_with_scroll(
    es=GRQ_ES,
    body=None,
    doc_type=None,
    q=None,
    sort=None,
    size=-1,
    index=consts.PRODUCTS_INDEX,
    **kwargs
):
    """
    Updated method of `run_query` to scroll the result set to get all results.

    NOTE:
    - Potential hazard: both q and body is None. it will pass body = None
    - scroll timeout is hardcoded as 1 minute
    - assumption: elastic-search result is a valid json

    :param es:
    :param body:
    :param doc_type:
    :param q:
    :param sort:
    :param size:
    :param index:
    :param kwargs:
    :return:
    """
    scroll_timeout = "30s"  # 30second.
    max_size_wo_scroll = 10000  # for up to 10k, no need to scroll
    params = {
        "doc_type": doc_type,
        "index": index,
        "size": size if size != -1 else max_size_wo_scroll,
        "scroll": scroll_timeout,
    }
    if sort:
        params["sort"] = sort
        pass
    if q and body is None:
        params["q"] = q
        pass
    else:
        params["body"] = body
        pass
    params.update(kwargs)  # copy all other arguments.
    primary_result = es.search(**params)  # initial result.
    total_size = primary_result["hits"]["total"]
    if size != -1:  # caller only wants some results
        total_size = size  # updating the target size
    current_size = len(primary_result["hits"]["hits"])
    scroll_id = primary_result["_scroll_id"]
    while current_size < total_size:  # need to scroll
        scrolled_result = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        scroll_id = scrolled_result["_scroll_id"]
        scrolled_result_size = len(scrolled_result["hits"]["hits"])
        if scrolled_result_size == 0:
            break
            pass
        else:
            current_size += scrolled_result_size
            primary_result["hits"]["hits"].extend(scrolled_result["hits"]["hits"])
            pass
        pass
    return primary_result


def construct_range_object(field, start_value=None, stop_value=None, inclusive=True):
    """
    making the existing method public.
    NOTE: it's possible to refactor the existing method, but lots of places are using it.

    :param field:
    :param start_value:
    :param stop_value:
    :param inclusive:
    :return:
    """
    return _construct_range_object(field, start_value, stop_value, inclusive)


def is_null(input_val) -> bool:
    """
    Helper method:
    Checking if a given value is None type or the string value of 'null' or 'undefined'

    :param input_val: any
    :return: bool
    """
    return input_val in [None, "null", "undefined"]


def _construct_range_object(field, start_value=None, stop_value=None, inclusive=True):
    """
    Helper method that returns a range object for query body

    :param field: name of the field being tracked (required)
    :param start_value: lower bound of range (optional)
    :param stop_value: upper bound of range (optional)
    :param inclusive: set to true if range contains bounds (optional)
    :return dict representing range object of query
    """

    lower_bound = "gte" if inclusive else "gt"
    upper_bound = "lte" if inclusive else "lt"
    range_query = {field: {}}
    if start_value:
        range_query[field][lower_bound] = start_value
    if stop_value:
        range_query[field][upper_bound] = stop_value

    return range_query


def _construct_range_query(field_name, operator, value):
    range = {"range": {field_name: {operator: value}}}
    return range


def _range_within_filter(field_name, start_value, stop_value):
    greater_than = _construct_range_query(field_name, "gte", value=start_value)
    less_than = _construct_range_query(field_name, "lte", value=stop_value)
    _filter = [greater_than, less_than]
    return _filter


def _construct_sub_filter(field_name, start=None, stop=None, sub_filter={}):
    current_filter = sub_filter.get("and", [])
    if start and stop:
        current_filter = _range_within_filter(
            field_name=field_name, start_value=start, stop_value=stop
        )
    elif start and stop is None:
        current_filter = [
            _construct_range_query(field_name=field_name, operator="gte", value=start)
        ]
    elif stop and start is None:
        current_filter = [
            _construct_range_query(field_name=field_name, operator="lte", value=stop)
        ]

    if len(current_filter) > 0:
        sub_filter["and"] = current_filter
    return sub_filter


def add_range_filter(query=None, time_key=None, start=None, stop=None):
    _filter = _construct_sub_filter(field_name=time_key, start=start, stop=stop)

    if "filter" not in query["query"]["bool"]:
        query["query"]["bool"]["filter"] = []

    if start and stop:
        query["query"]["bool"]["filter"].append(_filter["and"][0])
        query["query"]["bool"]["filter"].append(_filter["and"][1])
    elif start or stop:
        query["query"]["bool"]["filter"].append(_filter["and"][0])
    return query


def add_query_find(query=None, field_name=None, value=None):
    new_filter = {"match": {field_name: value}}
    query["query"]["bool"]["should"].append(new_filter)
    return query


def add_query_match(query=None, field_name=None, value=None):
    new_filter = {"match": {field_name: value}}
    query["query"]["bool"]["must"].append(new_filter)
    return query


def add_query_filter(query=None, field_name=None, value=None):
    new_filter = {"match": {field_name: value}}
    if "filter" not in query["query"]["bool"]:
        query["query"]["bool"]["filter"] = []

    query["query"]["bool"]["filter"].append(new_filter)
    return query


def get_downlink_data(
    size=40,
    work_start=None,
    work_end=None,
    start=None,
    end=None,
    time_key=None,
    vcid=None,
    ldf_filename=None,
    obs_id=None,
    **kwargs
):
    """
    Gets LDFs status doc by filtering based on the provided criteria. If none are provided it gets all the docs.
    :param size:
    :return:
    """
    LOGGER.debug(
        "Getting LDFs statuses with paramters: {}".format(json.dumps(locals()))
    )
    index = consts.ANCILLARY_INDEXES["LDF"]
    filter_path = ["hits.hits._id", "hits.hits._source"]
    query = {"query": {"bool": {"must": [{"term": {"_index": index}}]}}, "_source": []}

    sort = "creation_timestamp:desc"
    try:
        if start and end:
            # add created_at range filter
            query = add_range_filter(
                query=query, time_key=time_key, start=start, stop=end
            )
        elif start:
            query = add_range_filter(
                query=query, time_key=time_key, start=start, stop=None
            )
        elif end:
            query = add_range_filter(
                query=query, time_key=time_key, start=None, stop=end
            )
        # if work_start and work_end:
        #     query = add_range_filter(
        #         query=query, time_key="created_at", start=work_start, stop=work_end
        #     )
        # elif work_start:
        #     query = add_range_filter(
        #         query=query, time_key="created_at", start=work_start, stop=None
        #     )
        # elif work_end:
        #     query = add_range_filter(
        #         query=query, time_key="created_at", start=None, stop=work_end
        #     )

        if vcid:
            query = add_query_filter(query=query, field_name="vcid", value=vcid)

        if ldf_filename:
            query = add_query_filter(
                query=query, field_name="ldf_id", value=ldf_filename
            )

        if obs_id:
            obs_query = {"query": {"bool": {"must": []}}, "_source": []}
            # add obs_id filter

            obs_query = add_query_filter(
                query=obs_query, field_name="metadata.observation_ids", value=obs_id
            )

            obs_results = (
                run_query(
                    index=consts.PRODUCT_INDEXES["DATATAKE_STATE_CONFIGS"],
                    sort=sort,
                    size=size,
                    body=obs_query,
                    **kwargs
                )
                .get("hits")
                .get("hits")
            )

            for result in obs_results:
                source = result["_source"]
                metadata = source["metadata"]
                if (
                    "l0a_rrst_product_paths" in metadata
                    and len(metadata["l0a_rrst_product_paths"]) > 0
                ):
                    for l0a in metadata["l0a_rrst_product_paths"]:
                        query = add_query_filter(
                            query=query,
                            field_name="L0A_L_RRST_id",
                            value=os.path.basename(l0a),
                        )
        result = run_query(index=index, sort=sort, size=size, body=query, **kwargs)

        # Remove this when the orbit status is fixed
        tmpresult = result.get("hits").get("hits")
        ldf_tree = {}
        for entry in tmpresult:
            ldf = entry["_source"]["id"]
            workflow_start = entry["_source"]["creation_timestamp"]
            state_config_results = run_query(
                index="grq_*_ldf-*",
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"metadata.ldf_name": ldf.split("_LDF")[0]}}
                            ]
                        }
                    }
                },
            )
            # print(state_config_results)
            results = list(
                map(
                    lambda x: (x["_id"], x["_source"]),
                    state_config_results["hits"]["hits"],
                )
            )
            for state_config, sc_source in results:
                state_config_results = run_query(
                    index=consts.PRODUCT_INDEXES["L0A_L_RRST"],
                    body={
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "metadata.accountability.L0A_L_RRST_PP.trigger_dataset_ids": state_config
                                        }
                                    }
                                ]
                            }
                        }
                    },
                )
                # print(ldf)
                # print(state_config_results)
                if len(state_config_results["hits"]["hits"]) == 0:
                    ldf_tree[ldf] = {
                        "workflow_start": workflow_start,
                        state_config: {state_config: sc_source},
                    }
                else:
                    for res in state_config_results["hits"]["hits"]:
                        source = res["_source"]
                        if ldf not in ldf_tree:
                            ldf_tree[ldf] = {
                                "workflow_start": workflow_start,
                                state_config: {
                                    source["metadata"]["accountability"][
                                        "L0A_L_RRST_PP"
                                    ]["outputs"][0]: source
                                },
                            }
                        elif state_config not in ldf_tree[ldf]:
                            ldf_tree[ldf] = {
                                "workflow_start": workflow_start,
                                state_config: {
                                    source["metadata"]["accountability"][
                                        "L0A_L_RRST_PP"
                                    ]["outputs"][0]: source
                                },
                            }
                        else:
                            ldf_tree[ldf][state_config][
                                source["metadata"]["accountability"]["L0A_L_RRST_PP"][
                                    "outputs"
                                ][0]
                            ] = source
                            ldf_tree[ldf]["workflow_start"] = workflow_start
                    state_config_results = run_query(
                        index=consts.PRODUCT_INDEXES["L0A_L_RRST_PP"],
                        body={
                            "query": {
                                "bool": {
                                    "must": [
                                        {
                                            "match": {
                                                "metadata.accountability.L0A_L_RRST_PP.trigger_dataset_id": state_config
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                    )
                    for res in state_config_results["hits"]["hits"]:
                        source = res["_source"]
                        if ldf not in ldf_tree:
                            ldf_tree[ldf] = {
                                "workflow_start": workflow_start,
                                state_config: {source["id"]: source},
                            }
                        elif state_config not in ldf_tree[ldf]:
                            ldf_tree[ldf] = {
                                "workflow_start": workflow_start,
                                state_config: {source["id"]: source},
                            }
                        elif source["id"] in ldf_tree[ldf][state_config]:
                            continue
                        else:
                            ldf_tree[ldf][state_config][source["id"]] = source
                            ldf_tree[ldf]["workflow_start"] = workflow_start
            # print(ldf_tree)
        resulting_data = format_downlink_data(ldf_tree)
        return resulting_data
    except Exception as e:
        LOGGER.error("Error occurred while getting LDFs: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return []


def get_obs_data(
    size=40,
    obs_start=None,
    obs_end=None,
    start=None,
    end=None,
    time_key=None,
    obs_id=None,
    dt_id=None,
    processing_type=None,
    cycle=None,
    **kwargs
):
    """
    Gets LDFs status doc by filtering based on the provided criteria. If none are provided it gets all the docs.
    :param size:
    :return:
    """
    LOGGER.debug(
        "Getting LDFs statuses with paramters: {}".format(json.dumps(locals()))
    )
    index = consts.PRODUCT_INDEXES["DATATAKE_STATE_CONFIGS"]
    filter_path = ["hits.hits._id", "hits.hits._source"]
    query = {"query": {"bool": {"must": [{"term": {"_index": index}}]}}}

    sort = "creation_time:desc"
    _filter = {}
    # sort = ""
    datatake_state_configs = []
    try:
        # add datetime filters
        if start and end:
            query = add_range_filter(
                query=query, time_key=time_key, start=start, stop=end
            )
        elif start:
            query = add_range_filter(
                query=query, time_key=time_key, start=start, stop=None
            )
        elif end:
            query = add_range_filter(
                query=query, time_key=time_key, start=None, stop=end
            )
        datatake_state_configs = (
            run_query(index=index, sort=sort, size=size, body=query, **kwargs)
            .get("hits")
            .get("hits")
        )
    except Exception as e:
        LOGGER.error("Error occurred while getting Observations: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return []

    results = {}

    for dt_state_config in datatake_state_configs:
        last_modified = dt_state_config["_source"]["@timestamp"]
        id = dt_state_config["_source"]["metadata"]["datatake_id"]
        obs_ids = dt_state_config["_source"]["metadata"]["observation_ids"]
        l0b_query = {"query": {"bool": {"should": []}}}
        if id not in results:
            results[id] = {}
        for obs_id in obs_ids:
            results[id][obs_id] = {}
            l0b_query["query"]["bool"]["should"].append(
                {"match": {"metadata.OBS_ID": obs_id}}
            )
        l0b_results = (
            run_query(index=consts.PRODUCT_INDEXES["L0B_L_RRSD"], body=l0b_query)
            .get("hits")
            .get("hits")
        )

        for result in l0b_results:
            obs_id = result["_source"]["metadata"]["OBS_ID"]
            results[id][obs_id] = result["_source"]

    return format_l0b_data(results)


def get_result_ids(results):
    hits = results.get("hits").get("hits")

    if len(hits) > 0:

        return list(
            map(
                lambda x: {
                    "id": x["_id"],
                    "track": x["_source"]["metadata"]["accountability"]["L1_L_RSLC"][
                        "id"
                    ].split("_")[5]
                    if "accountability" in x["_source"]["metadata"]
                    else "",
                    "frame": x["_source"]["metadata"]["accountability"]["L1_L_RSLC"][
                        "id"
                    ].split("_")[7]
                    if "accountability" in x["_source"]["metadata"]
                    else "",
                    "coverage": x["_source"]["metadata"]["accountability"]["L1_L_RSLC"][
                        "trigger_dataset_id"
                    ].split("_")[5]
                    if "accountability" in x["_source"]["metadata"]
                    else "",
                },
                hits,
            )
        )
    else:
        return []


def grab_l0b_rrsd_rslc_children(l0b_id, track="", frame=""):
    index = consts.PRODUCT_INDEXES["L1_L_RSLC"]
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"metadata.accountability.L1_L_RSLC.inputs": l0b_id}}
                ]
            }
        }
    }

    if track:
        query = add_query_match(
            query=query, field_name="metadata.RelativeOrbitNumber", value=int(track)
        )

    if frame:
        query = add_query_match(
            query=query, field_name="metadata.FrameNumber", value=int(frame)
        )

    results = run_query(index=index, body=query)

    return get_result_ids(results)


def grab_rslc_children(rslc_id, cycle_sec="", cycle_ref=""):
    index = "grq_*_l*_l_*"
    query = {"query": {"bool": {"should": [], "must": []}}}

    for dataset in consts.RSLC_CHILDREN:
        query = add_query_find(
            query=query,
            field_name="metadata.accountability.{}.inputs".format(dataset),
            value=rslc_id,
        )

        # if cycle_ref:
        #     query = add_query_filter(
        #         query=query, field_name="metadata.ReferenceCycleNumber", value=cycle_ref
        #     )
        # if cycle_sec:
        #     query = add_query_filter(
        #         query=query, field_name="metadata.SecondaryCycleNumber", value=cycle_sec
        #     )

    results = run_query(index=index, body=query)

    hits = results.get("hits").get("hits")

    return hits


def get_track_frame_data(
    size=40,
    cycle_sec=None,
    cycle_ref=None,
    track=None,
    frame=None,
    obs_id=None,
    l0b_rrsd_id=None,
    **kwargs
):
    """
    Gets LDFs status doc by filtering based on the provided criteria. If none are provided it gets all the docs.
    :param size:
    :return:
    """
    LOGGER.debug(
        "Getting LDFs statuses with paramters: {}".format(json.dumps(locals()))
    )
    index = consts.PRODUCT_INDEXES["L0B_L_RRSD"]
    filter_path = ["hits.hits._id", "hits.hits._source"]
    query = {"query": {"bool": {"must": [{"match": {"_index": index}}]}}}

    sort = "creation_timestamp:desc"
    _filter = {}
    # sort = ""
    try:
        # this happens during the L0B querys
        if l0b_rrsd_id:
            query = add_query_match(query=query, field_name="_id", value=l0b_rrsd_id)

        # add obs_id
        if obs_id:
            query = add_query_filter(
                query=query, field_name="metadata.OBS_ID", value=obs_id
            )

        l0b_results = run_query(index=index, body=query, sort=sort, size=size, **kwargs)

        l0b_tree = {}
        for entry in l0b_results.get("hits").get("hits"):
            metadata = entry["_source"]["metadata"]
            if "OBS_ID" not in metadata:
                continue
            obs_id = metadata["OBS_ID"]
            l0b_id = entry["_id"]
            if l0b_id not in l0b_tree:
                l0b_tree[l0b_id] = {
                    "observation_id": obs_id,
                    "workflow_start": entry["_source"]["creation_timestamp"],
                    "L1_L_RSLC": {},
                }

                rslcs = grab_l0b_rrsd_rslc_children(l0b_id, track=track, frame=frame)

                for rslc in rslcs:
                    rslc_id = rslc["id"]
                    del rslc["id"]
                    l0b_tree[l0b_id]["L1_L_RSLC"][rslc_id] = rslc

                    children = grab_rslc_children(
                        rslc_id, cycle_sec=cycle_sec, cycle_ref=cycle_ref
                    )

                    rslc_dict = l0b_tree[l0b_id]["L1_L_RSLC"]

                    if rslc_id not in rslc_dict:
                        rslc_dict[rslc_id] = {}

                    for child in children:
                        source = child["_source"]
                        dataset = source["dataset"]

                        if dataset not in rslc_dict[rslc_id]:
                            rslc_dict[rslc_id][dataset] = {}

                        rslc_dict[rslc_id][dataset][child["_id"]] = source

        with open("f.json", "w") as p:
            json.dump(l0b_tree, p)

        return format_track_frame_data(l0b_tree)
    except Exception as e:
        LOGGER.error("Error occurred while getting Observations: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return []


def flatten_doc(doc, skip_keys=list(), parent_key=""):
    flatten_dict = {}
    for key in doc:
        if key in skip_keys:
            flatten_dict["{}.{}".format(parent_key, key).strip(".")] = doc.get(key)
            continue
        if isinstance(doc.get(key), dict):
            flatten_dict.update(
                flatten_doc(doc.get(key), skip_keys=skip_keys, parent_key=key)
            )
        else:
            flatten_dict["{}.{}".format(parent_key, key).strip(".")] = doc.get(key)
    return flatten_dict


def get_job(job_id):
    """
    Get job status doc based on ID
    :param job_id:
    :return:
    """
    LOGGER.debug("Getting job with id {}".format(job_id))
    index = consts.JOB_STATUS_INDEX
    query = {"query": {"bool": {"must": [{"match": {"_id": job_id}}]}}}
    try:
        source_includes = [
            "job_id",
            "_id",
            "status",
            "job.job_info.job_queue",
            "job.job_info.time_start",
            "job.job_info.execute_node",
            "job.job_info.job_dir",
            "job.job_info.cmd_duration",
            "job.job_info.metrics.products_staged.browse_urls",
            "job.job_info.metrics.products_staged.dataset",
            "job.params.runconfig",
            "payload_id",
            "error",
            "short_error",
            "traceback",
            "job.job_info.duration",
        ]
        result = run_query(
            index=index, body=query, es=JOBS_ES, _source_include=source_includes
        )
        result = list(map(lambda doc: doc["_source"], result["hits"]["hits"]))
        if len(result) < 1:  # if there are no results
            return {}
        result = result[0]  # choosing the first result.
        return result
    except Exception as e:
        LOGGER.error("Error occurred while getting Job: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return {}


def get_jobs_by_status(job_status):
    body = {
        "query": {
            "bool": {
                "must": [{"term": {"status": job_status}}],
                "must_not": [],
                "should": [],
            }
        },
        "from": 0,
        "size": 10,
        "sort": [{"@timestamp": {"order": "desc"}}],
    }
    job_query = run_query(es=JOBS_ES, index="job_status-current", body=body, size=10)
    # return job_query.get("hits")
    arr = []
    total = job_query["hits"]["total"]
    for hit in job_query["hits"]["hits"]:
        arr.append([hit["_source"]["payload_id"]])
    flattened = []
    for sublist in arr:
        for val in sublist:
            flattened.append(val)
    return {"payload_id": flattened, "total": total}


def get_job_by_uuid(uuid):
    index = consts.JOB_STATUS_INDEX
    source_includes = [
        "job_id",
        "_id",
        "status",
        "job.job_info.job_queue",
        "job.job_info.time_start",
        "job.job_info.execute_node",
        "job.job_info.job_dir",
        "job.job_info.cmd_duration",
        "job.params.runconfig",
        "payload_id",
        "error",
        "short_error",
        "traceback",
        "job.job_info.duration",
    ]

    query = {
        "query": {
            "bool": {"must": [{"term": {"uuid": uuid}}], "must_not": [], "should": []}
        },
        "from": 0,
        "size": 10,
        "sort": [],
        "aggs": {},
    }
    result = run_query(
        index=index, body=query, es=JOBS_ES, _source_include=source_includes
    )
    if result["hits"]["total"] > 0:
        result = result["hits"]["hits"][0]["_source"]
    return {}


def process_product(doc):
    doc = doc.get("_source")


def get_product(product_id, index=None):
    """
    Get product doc based on ID
    :param product_id:
    :return:
    """
    LOGGER.debug("Getting product with id {}".format(product_id))
    if index is None:
        index = consts.PRODUCTS_INDEX
    query = {"query": {"bool": {"must": [{"match": {"id": product_id}}]}}}
    try:
        exclude = ["metadata.context.context"]
        result = run_query(index=index, body=query, _source_excludes=exclude)
        result = list(map(lambda doc: doc["_source"], result["hits"]["hits"]))
        if len(result) > 0:
            return result[0]
        return None
    except Exception as e:
        LOGGER.error("Error occurred while getting product: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return None


def get_num_docs_in_index(
    index, start=None, end=None, time_key=None, es=GRQ_ES, **kwargs
):
    query = {"query": {"bool": {"must": [], "filter": []}}}
    if index in list(consts.ACCOUNTABILITY_INDEXES.values()):
        query = add_range_filter(
            query=query, time_key="created_at", start=start, stop=end
        )
    elif index in list(consts.STATE_CONFIG_INDEXES.values()):
        query = add_range_filter(
            query=query, time_key="creation_time", start=start, stop=end
        )
    else:
        query = add_range_filter(
            query=query, time_key="creation_timestamp", start=start, stop=end
        )
    query["query"]["bool"]["must"].append({"term": {"_index": index}})
    result = es.get_count(body=query, index=index, doc_type="_doc")
    return result


def get_docs_in_index(index, size=40, start=None, end=None, time_key=None, **kwargs):
    """
    Get docs within particular index between a certain time range
    :param index:
    :param start:
    :param end:
    :param size:
    :return:
    """
    query = {}
    _from = 0
    _to = _from + size
    docs = list()
    total = 0
    if start and end:
        query = {"query": {"bool": {"must": [{"match": {"_index": index}}]}}}

        if index in consts.ACCOUNTABILITY_INDEXES:
            query = add_range_filter(
                query=query, time_key="last_modified", start=start, stop=end
            )
            workflow_start = kwargs.get("workflow_start", None)
            workflow_end = kwargs.get("workflow_end", None)
            query = add_range_filter(
                query=query,
                time_key="created_at",
                start=workflow_start,
                stop=workflow_end,
            )
        else:
            query = add_range_filter(
                query=query, time_key="creation_timestamp", start=start, stop=end
            )

    if "metadata_tile_id" in kwargs:
        if kwargs.get("metadata_tile_id"):
            query = add_query_match(query=query, field_name="metadata.tile_id.keyword", value=kwargs["metadata_tile_id"])
        # removing from kwargs so this is not passed as an Elasticsearch client property downstream.
        del kwargs['metadata_tile_id']

    while _from <= _to:
        result = run_query(index=index, size=size, body=query, from_=_from, **kwargs)
        total = result.get("hits").get("total").get("value")
        docs.extend(map(lambda doc: doc.get("_source"), result.get("hits").get("hits")))
        _from += size
        _to = total
    return docs, total


def get_docs(index, start=None, end=None, source=None, size=40, **kwargs):
    docs = []
    for partial in always_iterable(index):
        result, total = get_docs_in_index(
            partial, start=start, end=end, size=size, **kwargs
        )
        docs.extend(result)
    return docs


def get_num_docs(index_dict, start=None, end=None, **kwargs):
    docs_count = {}
    for name in index_dict:
        docs_count[name] = 0
        for index in always_iterable(index_dict[name]):
            if index:
                docs_count[name] += get_num_docs_in_index(
                    index, start, end, **kwargs)
    return docs_count
