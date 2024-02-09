import logging
import traceback
from typing import Union, List, Dict, Tuple, Optional

from elasticsearch.exceptions import NotFoundError
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
from more_itertools import always_iterable

from accountability_api import es_connection
from accountability_api.api_utils import JOBS_ES
from accountability_api.api_utils import metadata as consts

LOGGER = logging.getLogger()


def run_query(
    es: Optional[ElasticsearchUtility] = None,
    body: Optional[Dict] = None,
    doc_type: Optional[str] = None,
    sort: Optional[List[str]] = None,
    size=500,
    index=consts.PRODUCTS_INDEX,
    **kwargs
):
    es = es or es_connection.get_grq_es()
    es = es.es or es_connection.get_grq_es().es

    if sort:
        return es.search(index=index, body=body, doc_type=doc_type, sort=sort, size=size, params=kwargs)
    else:
        return es.search(index=index, body=body, doc_type=doc_type, size=size, params=kwargs)


def run_query_with_scroll(
    es: Optional[ElasticsearchUtility] = None,
    body: Optional[Dict] = None,
    q: Optional[str] = None,
    doc_type: Optional[str] = None,
    sort: Optional[List[str]] = None,
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
    es = es or es_connection.get_grq_es()
    if hasattr(es, "es"):
        es = es.es

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
    total_size = primary_result["hits"]["total"]["value"]
    if size != -1:  # caller only wants some results
        total_size = size  # updating the target size

    current_size = len(primary_result["hits"]["hits"])

    scroll_id = primary_result.get("_scroll_id")
    if not scroll_id:
        return primary_result

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


def add_range_filter(query: Dict, time_key=None, start=None, stop=None) -> Dict:
    _filter = _construct_sub_filter(field_name=time_key, start=start, stop=stop)

    if "filter" not in query["query"]["bool"]:
        query["query"]["bool"]["filter"] = []

    if start and stop:
        query["query"]["bool"]["filter"].append(_filter["and"][0])
        query["query"]["bool"]["filter"].append(_filter["and"][1])
    elif start or stop:
        query["query"]["bool"]["filter"].append(_filter["and"][0])
    return query


def add_query_find(query: Dict, field_name=None, value=None) -> Dict:
    new_filter = {"match": {field_name: value}}
    query["query"]["bool"]["should"].append(new_filter)
    return query


def add_query_match(query: Dict, field_name=None, value=None) -> Dict:
    new_filter = {"match": {field_name: value}}
    query["query"]["bool"]["must"].append(new_filter)
    return query


def add_query_filter(query: Dict, field_name=None, value=None) -> Dict:
    new_filter = {"match": {field_name: value}}
    if "filter" not in query["query"]["bool"]:
        query["query"]["bool"]["filter"] = []

    query["query"]["bool"]["filter"].append(new_filter)
    return query


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
        result = run_query_with_scroll(
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
    job_query = run_query_with_scroll(es=JOBS_ES, index="job_status-current", body=body, size=10)
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
    result = run_query_with_scroll(
        index=index, body=query, es=JOBS_ES, _source_include=source_includes
    )
    if result["hits"]["total"] > 0:
        result = result["hits"]["hits"][0]["_source"]
    return {}


def process_product(doc):
    doc = doc.get("_source")


def get_product(product_id: str, index=consts.PRODUCTS_INDEX):
    """
    Get product doc based on ID
    :param product_id:
    :param index:
    :return:
    """
    LOGGER.debug(f"Getting product with id {product_id}")
    query = {"query": {"bool": {"must": [{"match": {"id": product_id}}]}}}
    try:
        exclude = ["metadata.context.context"]
        result = run_query_with_scroll(index=index, body=query, _source_excludes=exclude)
        result = list(map(lambda doc: doc["_source"], result["hits"]["hits"]))
        if len(result) > 0:
            return result[0]
        return None
    except Exception as e:
        LOGGER.error("Error occurred while getting product: {}".format(e))
        LOGGER.error(traceback.format_exc())
        return None


def get_num_docs_in_index(
        index,
        start=None,
        end=None,
        time_key=None,
        es: Optional[ElasticsearchUtility] = None,
        **kwargs
):
    es = es or es_connection.get_grq_es()

    query = {"query": {"bool": {"must": [], "filter": []}}}
    if index in list(consts.ACCOUNTABILITY_INDEXES.values()):
        query = add_range_filter(
            query=query, time_key="created_at", start=start, stop=end
        )
    else:
        query = add_range_filter(
            query=query, time_key="creation_timestamp", start=start, stop=end
        )
    query["query"]["bool"]["must"].append({"term": {"_index": index}})
    search_results = run_query_with_scroll(body=query, index=index, doc_type="_doc", _source=False)["hits"]["hits"]
    result = 0
    for search_result in search_results:
        result += 1
    return result


def get_docs_in_index(index: str, size=-1, start=None, end=None, time_key=None, **kwargs) -> Tuple[List[Dict], int]:
    """
    Get docs within particular index between a certain time range
    :param index:
    :param start:
    :param end:
    :param size:
    :return:
    """
    query = {}
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
    if "metadata_sensor" in kwargs:
        if kwargs.get("metadata_sensor"):
            query = add_query_match(query=query, field_name="metadata.sensor.keyword", value=kwargs["metadata_sensor"])
        # removing from kwargs so this is not passed as an Elasticsearch client property downstream.
        del kwargs['metadata_sensor']

    result = run_query_with_scroll(index=index, size=size, body=query, **kwargs)
    total = result.get("hits").get("total").get("value")

    docs = [map_doc_to_source(doc) for doc in result.get("hits", {}).get("hits", [])]

    return docs, total


def map_doc_to_source(doc: dict):
    source: dict = doc["_source"]
    source.update({"_id": doc["_id"]})
    source.update({"_index": doc["_index"]})
    return source


def get_docs(indexes: Union[str, List[str]], start=None, end=None, source=None, size=-1, **kwargs) -> List[Dict]:
    """
    Get docs within particular indexes between a certain time range
    :param indexes: a single index name or list of index names
    :param index:
    :param start:
    :param end:
    :param size:
    :return:
    """
    docs = []
    for partial in always_iterable(indexes):
        result, total = get_docs_in_index(
            partial, start=start, end=end, size=size, **kwargs
        )
        docs.extend(result)
    return docs


def get_num_docs(index_dict: Dict, start=None, end=None, **kwargs):
    docs_count = {}
    for name in index_dict:
        docs_count[name] = 0
        for index in always_iterable(index_dict[name]):
            if index:
                try:
                    docs_count[name] += get_num_docs_in_index(index, start, end, **kwargs)
                except NotFoundError:
                    logging.error(f"Index ({index}) was not found. Is the index name valid? Does it exist?")
    return docs_count
