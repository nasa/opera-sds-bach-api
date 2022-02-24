# from accountability_api.api_utils.utils import (
#     get_orbit_range_list,
#     ElasticsearchResultDictWrapper,
# )
from accountability_api.api_utils import GRQ_ES, JOBS_ES
from accountability_api.api_utils import metadata as consts
from accountability_api.api_utils.processing import format_downlink_data, format_data
import logging
import traceback
import json

# import copy
# from pandas import DataFrame as df
# import requests

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
    # headers= {"content-type": "application/json"}
    # pdb.set_trace()
    # response = requests.post("http://" + GRQ_ES_URL + "/_search", data=body, headers=headers)
    # return json.loads(response.text)
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
        current_filter = [_construct_range_query(
            field_name=field_name, operator="gte", value=start
        )]
    elif stop and start is None:
        current_filter = [_construct_range_query(
            field_name=field_name, operator="lte", value=stop
        )]

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
    new_filter = {"term": {field_name: value}}
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
    index = consts.ACCOUNTABILITY_INDEXES["DOWNLINK"]
    filter_path = ["hits.hits._id", "hits.hits._source"]
    query = {"query": {"bool": {"must": [{"term": {"_index": index}}]}}, "_source": []}

    sort = "last_modified:desc"
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
        if work_start and work_end:
            query = add_range_filter(
                query=query, time_key="created_at", start=work_start, stop=work_end
            )
        elif work_start:
            query = add_range_filter(
                query=query, time_key="created_at", start=work_start, stop=None
            )
        elif work_end:
            query = add_range_filter(
                query=query, time_key="created_at", start=None, stop=work_end
            )

        if vcid:
            query = add_query_filter(
                query=query, field_name="vcid", value=vcid
            )

        if ldf_filename:
            query = add_query_filter(
                query=query, field_name="ldf_id", value=ldf_filename
            )

        if obs_id:
            obs_query = {"query": {"bool": {"must": []}}, "_source": []}
            # add obs_id filter

            obs_query = add_query_filter(
                query=obs_query, field_name="observation_ids", value=obs_id
            )

            obs_results = (
                run_query(
                    index=consts.ACCOUNTABILITY_INDEXES["OBSERVATION"],
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
                if "L0A_L_RRST_ids" in source and len(source["L0A_L_RRST_ids"]) > 1:
                    for l0a in source["L0A_L_RRST_ids"]:
                        query = add_query_filter(
                            query=query, field_name="L0A_L_RRST_id", value=l0a
                        )
        result = run_query(
            index=consts.ACCOUNTABILITY_INDEXES["DOWNLINK"],
            sort=sort,
            size=size,
            body=query,
            **kwargs
        )

        # Remove this when the orbit status is fixed
        tmpresult = result.get("hits").get("hits")
        if len(tmpresult) == 0:
            return []
        #
        formatted_results = format_downlink_data(tmpresult)
        return formatted_results
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
    index = consts.ACCOUNTABILITY_INDEXES["OBSERVATION"]
    filter_path = ["hits.hits._id", "hits.hits._source"]
    query = {"query": {"bool": {"must": [{"term": {"_index": index}}]}}}

    sort = "last_modified:desc"
    _filter = {}
    # sort = ""
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
        if obs_start:
            query = add_range_filter(
                query=query, time_key="ref_start_datetime_iso", start=obs_start, stop=None
            )
        if obs_end:
            query = add_range_filter(
                query=query, time_key="ref_end_datetime_iso", start=None, stop=obs_end
            )

        # add obs_id
        if obs_id:
            query = add_query_filter(
                query=query, field_name="observation_ids", value=obs_id
            )
        if dt_id:
            query = add_query_filter(query=query, field_name="datatake_id", value=dt_id)
        if processing_type:
            query = add_query_filter(
                query=query, field_name="processing_type", value=processing_type
            )
        if cycle:
            L0B_query = {"query": {"bool": {"must": []}}}

            L0B_query = add_query_match(
                L0B_query, field_name="metadata.CycleNumber", value=cycle
            )

            l0b_results = (
                run_query(
                    index=consts.PRODUCT_INDEXES["L0B_RRSD"],
                    sort=sort,
                    size=size,
                    body=L0B_query,
                    **kwargs
                )
                .get("hits")
                .get("hits")
            )

            for l0b in l0b_results:
                query = add_query_filter(
                    query=query, field_name="L0B_L_RRSD_id", value=l0b["_id"]
                )

        result = run_query(
            index=consts.ACCOUNTABILITY_INDEXES["OBSERVATION"],
            sort=sort,
            size=size,
            body=query,
            **kwargs
        )

        # Remove this when the orbit status is fixed
        tmpresult = result.get("hits").get("hits")
        #
        return format_data(tmpresult)
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

    while _from <= _to:
        result = run_query(index=index, size=size, body=query, from_=_from, **kwargs)
        total = result.get("hits").get("total").get("value")
        docs.extend(map(lambda doc: doc.get("_source"), result.get("hits").get("hits")))
        _from += size
        _to = total
    return docs, total


def get_docs(index, start=None, end=None, source=None, size=40, **kwargs):
    docs = []
    if isinstance(index, list):
        for partial in index:
            result, total = get_docs_in_index(
                partial, start=start, end=end, size=size, **kwargs
            )
            docs.extend(result)
    else:
        result, total = get_docs_in_index(
            index, start=start, end=end, size=size, **kwargs
        )
        docs.extend(result)
    return docs


def get_num_docs(index_dict, start=None, end=None, **kwargs):
    docs_count = {}
    for name in index_dict:
        docs_count[name] = 0
        if isinstance(index_dict[name], list):
            for index in index_dict[name]:
                if index:
                    docs_count[name] += get_num_docs_in_index(index, start, end, **kwargs)
        else:
            index = index_dict[name]
            if index:
                docs_count[name] = get_num_docs_in_index(index, start, end, **kwargs)
    return docs_count
