import os
import uuid
from collections import defaultdict

from api_utils import metadata, JOBS_ES
from api_utils.metadata import FROM_NOT_UNDERSCORE_SHORT_NAMES, JOB_STATUS_INDEX
from api_utils.utils import (
    from_iso_to_dt,
    from_dt_to_iso,
    is_valid_json,
    chunk_list,
    ElasticsearchResultDictWrapper,
)
from configuration_obj import ConfigurationObj
from accountability_api.api_utils import query

LAST_MODIFIED_DATE_KEY = "LastModifiedTime"


PROD_ACC_DOC_SCHEMA = {
    "type": "object",
    "properties": {
        "OrbitNumber": {"type": "integer"},
        "OrbitDirection": {"type": "string", "minLength": 1},
        "ShortName": {"type": "string", "minLength": 1},
        "job_status": {"type": "string", "minLength": 1},
        "job_id": {"type": "string", "minLength": 1},
        "ProductCounter": {"type": "string", "minLength": 1},
        "ProductName": {"type": "string", "minLength": 1},
        "CompositeReleaseID": {"type": "string", "minLength": 1},
        LAST_MODIFIED_DATE_KEY: {"type": "string", "minLength": 1},
    },
    "required": [
        "OrbitNumber",
        "OrbitDirection",
        "ShortName",
        "job_status",
        "ProductCounter",
        "ProductName",
        "CompositeReleaseID",
        "job_id",
        LAST_MODIFIED_DATE_KEY,
    ],
}

PROD_SCHEMA = {
    "type": "object",
    "properties": {
        "job_id": {},
        "ProductName": {},
    },
    "required": ["job_id", "ProductName"],
}

JOB_STATUS_DICT = {  # 'job-completed' is not in this since it will be replaced with the product-name
    "F": "job-failed",
    "R": "job-started",  # or 'job-queued'
    # 'D': 'job-deduped',  # if original = completed, pull product ID from it, if original = running / queued, it fails
}

SCIFLOW_JOB_REVERSE_DICT = {
    "job-failed": "F",
    "job-offline": "F",
    "job-revoked": "F",
    "job-started": "R",
    "job-queued": "R",
    "job-completed": "C",
    "job-deduped": "C",
}


class ProdAccInterface:
    """
    Using Informal Interface approach from https://realpython.com/python-interface/
    It can be updated to Formal Interface if needed
    """

    _PROD_ACC_SORT = [
        {LAST_MODIFIED_DATE_KEY: "desc"},
        {"OrbitNumber": "desc"},
        {"OrbitDirection": "desc"},
    ]

    def get_w_date(self, start_time, end_time):
        raise NotImplementedError("need implementation")

    def get_w_composite(self, start_com_id, stop_com_id):
        raise NotImplementedError("need implementation")

    def get_w_orbit(self, min_orbit, max_orbit):
        raise NotImplementedError("need implementation")

    def get_orbit_history(self, half_orbit_id):
        raise NotImplementedError("need implementation")

    pass


class ProdAccIndexImpl(ProdAccInterface):
    @staticmethod
    def __generate_tbl(raw_data):
        resulting_list = defaultdict(dict)

        for k in raw_data:
            each = k["_source"]
            if not is_valid_json(each, PROD_ACC_DOC_SCHEMA):
                continue
            sn = each["ShortName"]
            orbit_num = each["OrbitNumber"]
            orbit_dir = each["OrbitDirection"]
            job_id_key = "{}_job_id".format(sn)
            key = "{}{}".format(orbit_num, orbit_dir[0])
            if job_id_key not in resulting_list[key]:  # need to add
                current_last_modified = from_iso_to_dt(each[LAST_MODIFIED_DATE_KEY])
                if (
                    "Last Modified" not in resulting_list[key]
                    or resulting_list[key]["Last Modified"] < current_last_modified
                ):
                    resulting_list[key]["Last Modified"] = current_last_modified

                resulting_list[key]["half_orbit_id"] = key
                resulting_list[key][job_id_key] = each["job_id"]
                resulting_list[key]["{}_job_status".format(sn)] = each["job_status"]
                resulting_list[key]["{}_ProductCounter".format(sn)] = each[
                    "ProductCounter"
                ]
                resulting_list[key]["{}_ProductName".format(sn)] = each["ProductName"]
                resulting_list[key]["OrbitNumber"] = orbit_num
                resulting_list[key]["OrbitDirection"] = orbit_dir
                resulting_list[key]["CompositeReleaseID"] = each[
                    "CompositeReleaseID"
                ]  # TODO how to get latest
                pass
            pass
        for each in resulting_list.values():
            if "Last Modified" in each:
                each["Last Modified"] = from_dt_to_iso(each["Last Modified"])
        return list(resulting_list.values())

    def get_w_date(self, start_time, end_time):
        """
        update: to show missing products which are out of the time range, query the orbits first with the time range.
                Then query for those orbits
        :param start_time:
        :param end_time:
        :return:
        """
        prod_query_for_orbit = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": query.construct_range_object(
                                LAST_MODIFIED_DATE_KEY, start_time, end_time
                            )
                        },
                        # {"bool": {
                        #     "must_not": [
                        #         {"match": {"OrbitNumber": None}},
                        #         {"match": {"OrbitDirection": None}},
                        #     ]
                        # }}
                    ]
                }
            },
            "aggs": {"orbits": {"terms": {"field": "OrbitNumber", "size": 999999}}},
        }
        orbits_result = query.run_query(
            body=prod_query_for_orbit,
            index=metadata.PRODUCT_ACCOUNTABILITY_INDEX,
            size=0,
        )
        orbits = [
            int(k["key"]) for k in orbits_result["aggregations"]["orbits"]["buckets"]
        ]
        if len(orbits) < 1:
            return []
        specific_orbit_query = {
            "query": {
                "bool": {
                    "must": [
                        {"bool": {"should": [{"terms": {"OrbitNumber": orbits}}]}},
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        if end_time is not None:
            specific_orbit_query["query"]["bool"]["must"].append(
                {
                    "range": query.construct_range_object(
                        LAST_MODIFIED_DATE_KEY, None, end_time
                    )
                }
            )
            pass
        es_result = query.run_query_with_scroll(
            body=specific_orbit_query, index=metadata.PRODUCT_ACCOUNTABILITY_INDEX
        )
        return self.__generate_tbl(es_result["hits"]["hits"])

    def get_w_composite(self, start_com_id, stop_com_id):
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": query.construct_range_object(
                                "CompositeReleaseNumber", start_com_id, stop_com_id
                            )
                        },
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        es_result = query.run_query_with_scroll(
            body=product_query, index=metadata.PRODUCT_ACCOUNTABILITY_INDEX
        )
        return self.__generate_tbl(es_result["hits"]["hits"])

    def get_w_orbit(self, min_orbit, max_orbit):
        # TODO use collapse when it becomes available
        # TODO use exists when it becomes available
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": query.construct_range_object(
                                "OrbitNumber", min_orbit, max_orbit
                            )
                        },
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        es_result = query.run_query_with_scroll(
            body=product_query, index=metadata.PRODUCT_ACCOUNTABILITY_INDEX
        )
        return self.__generate_tbl(es_result["hits"]["hits"])

    def get_orbit_history(self, half_orbit_id):
        raise TypeError("cannot query orbit history using product-accountability index")

    pass


class OrbitStatusIndexImpl(ProdAccInterface):
    _JOB_DURATION_SCHEMA = {
        "type": "object",
        "properties": {
            "job": {
                "type": "object",
                "properties": {
                    "job_info": {
                        "type": "object",
                        "properties": {"duration": {"type": "number"}},
                        "required": ["duration"],
                    }
                },
                "required": ["job_info"],
            }
        },
        "required": ["job"],
    }

    def __init__(self):
        # we can overwrite how many products is a total products in prod-acc table by configuring it in app.conf.ini
        try:
            self._total_prods_count = int(
                ConfigurationObj().get_item("PROD_ACC_PRODS_COUNT", default="-1")
            )
        except:
            self._total_prods_count = -1
        self._result_dict = {}
        self._result_workflow = {}
        pass

    @staticmethod
    def __get_workflow_history_counts(half_orbit_array):
        workflow_agg = {
            "query": {
                "bool": {
                    "should": [
                        {"terms": {"half_orbit_id": k}}
                        for k in chunk_list(half_orbit_array, 1024)
                    ]
                },
            },
            "aggs": {
                "workflow_history": {
                    "terms": {
                        "field": "half_orbit_id",
                        "min_doc_count": 2,
                        "size": len(half_orbit_array),
                    }
                }
            },
        }
        es_result = query.run_query(
            body=workflow_agg, index=metadata.HALF_ORBIT_STATUS_INDEX, size=0
        )
        dotted_es_result = ElasticsearchResultDictWrapper(es_result)
        result_bucket = dotted_es_result.get_val(
            "aggregations.workflow_history.buckets"
        )
        if result_bucket is None:
            return {}
        return {k["key"].upper(): k["doc_count"] for k in result_bucket}

    @staticmethod
    def __get_short_name(input_str, replacing_str):
        return input_str.replace(replacing_str, "")

    def __create_prod(self, input_obj, prefix, half_orbit_dict):
        if input_obj["job_id"] is None and input_obj["ProductName"] is None:
            return
        half_orbit_dict["{}_job_id".format(prefix)] = input_obj["job_id"]
        if input_obj["ProductName"] in JOB_STATUS_DICT:
            half_orbit_dict["{}_ProductName".format(prefix)] = None
            half_orbit_dict["{}_ProductCounter".format(prefix)] = None
            half_orbit_dict["{}_job_status".format(prefix)] = JOB_STATUS_DICT[
                input_obj["ProductName"]
            ]
            return
        half_orbit_dict["{}_ProductName".format(prefix)] = input_obj["ProductName"]
        half_orbit_dict[
            "{}_ProductCounter".format(prefix)
        ] = self.__get_product_counter(input_obj["ProductName"])
        half_orbit_dict["{}_job_status".format(prefix)] = "job-completed"
        return

    @staticmethod
    def __get_product_counter(input_str):
        if input_str is None or input_str == "":
            return None
        return os.path.splitext(input_str)[0].split("_")[-1]

    def __get_workflow_duration_v2(self, workflow_ids=None):
        if workflow_ids is None:
            workflow_ids = [k for k in self._result_workflow.keys() if k is not None]
        if len(workflow_ids) < 1:
            return {}
        sciflow_job_query = {
            "query": {
                "bool": {
                    "should": [
                        {"terms": {"payload_id": k}}
                        for k in chunk_list(workflow_ids, 1024)
                    ]
                }
            }
        }
        sources = ["job.job_info.duration", "payload_id", "status"]
        sciflow_jobs = query.run_query_with_scroll(
            es=JOBS_ES,
            index=JOB_STATUS_INDEX,
            body=sciflow_job_query,
            _source_include=sources,
        )
        for each in sciflow_jobs["hits"]["hits"]:
            source = each["_source"]
            current_result_dict = self._result_dict[
                self._result_workflow[source["payload_id"]]
            ]
            if is_valid_json(source, self._JOB_DURATION_SCHEMA):
                current_result_dict["workflow_duration"] = source["job"]["job_info"][
                    "duration"
                ]
                pass
            if "status" in source:
                workflow_status = SCIFLOW_JOB_REVERSE_DICT[source["status"]]
                current_result_dict["workflow_status"] = workflow_status
                if (
                    workflow_status == "F"
                ):  # needs to convert all Running products to Failed
                    for k, v in current_result_dict.items():
                        if k.endswith("_job_status") and v != "job-completed":
                            current_result_dict[k] = "job-failed"
                    pass
            pass
        return

    def __create_prod_acc_dict(self, source):
        prod_acc_dict = {
            "half_orbit_id": source["half_orbit_id"],
            "Last Modified": source["LastModifiedTime"],
            "OrbitNumber": source["OrbitDirection"],
            "OrbitDirection": source["OrbitNumber"],
            "CompositeReleaseID": source["CompositeReleaseID"],
            "RadiometerDataStatus": source["RadiometerDataStatus"],  # for future
            "workflow_status": source["sciflo_run_status"],
            "workflow_id": source["sciflo_job_id"],
        }
        all_prods = defaultdict(dict)
        for k, v in source.items():
            if k.endswith("JobId"):
                short_name = self.__get_short_name(k, "JobId")
                all_prods[short_name]["job_id"] = v
                pass
            elif k.endswith("Status"):
                short_name = self.__get_short_name(k, "Status")
                all_prods[short_name]["ProductName"] = v
                pass
            pass
        all_prods = {
            k: v for k, v in all_prods.items() if is_valid_json(v, PROD_SCHEMA)
        }
        prod_acc_dict["total_products"] = (
            len(all_prods) if self._total_prods_count == -1 else self._total_prods_count
        )
        for k, v in all_prods.items():
            formal_short_name = FROM_NOT_UNDERSCORE_SHORT_NAMES[k]
            self.__create_prod(v, formal_short_name, prod_acc_dict)
            pass
        return prod_acc_dict

    def __generate_tbl(self, raw_results):
        """
        {
            "Last Modified": "2020-03-31T02:21:54.073000Z",
            "half_orbit_id": "1469A",
            "L2_SM_P_job_id": "4a713d8c-6a73-493a-8a74-8588d7729786",
            "L2_SM_P_job_status": "job-completed",
            "L2_SM_P_ProductCounter": "013",
            "L2_SM_P_ProductName": "SMAP_L2_SM_P_01469_A_20150511T234018_P16020_013.h5",
            "OrbitNumber": 1469,
            "OrbitDirection": "Ascending",
            "CompositeReleaseID": "P16020",
            "L1C_TB_job_id": "5da6b250-ac7f-4241-8704-34a971a7158b",
            "L1C_TB_job_status": "job-completed",
            "L1C_TB_ProductCounter": "015",
            "L1C_TB_ProductName": "SMAP_L1C_TB_01469_A_20150511T234018_P16020_015.h5",
            "L1B_TB_job_id": "046d1d87-bbaa-4dea-b0b8-b82ddbf08b6e",
            "L1B_TB_job_status": "job-completed",
            "L1B_TB_ProductCounter": "019",
            "L1B_TB_ProductName": "SMAP_L1B_TB_01469_A_20150511T234018_P16020_019.h5",
            "L1A_Radiometer_job_id": "20b93d80-d6d7-4286-98b5-4f9354e2303d",
            "L1A_Radiometer_job_status": "job-deduped",
            "L1A_Radiometer_ProductCounter": "022",
            "L1A_Radiometer_ProductName": "SMAP_L1A_RADIOMETER_01469_A_20150511T234018_P16020_022.h5"
        }
        :param raw_results:
        :return:
        """

        for each in raw_results:
            each = each["_source"]
            if each["half_orbit_id"] in self._result_dict:
                # already has it. this must be an older record. may need it later
                continue
                pass
            if each["sciflo_job_id"] is not None:
                self._result_workflow[each["sciflo_job_id"]] = each["half_orbit_id"]
            self._result_dict[each["half_orbit_id"]] = self.__create_prod_acc_dict(each)
            pass
        self.__get_workflow_duration_v2()
        history_results = self.__get_workflow_history_counts(
            [k.lower() for k in self._result_dict.keys()]
        )
        for each in self._result_dict.values():
            each["history_count"] = (
                history_results[each["half_orbit_id"]]
                if each["half_orbit_id"] in history_results
                else 1
            )
        return list(self._result_dict.values())

    def get_w_date(self, start_time, end_time):
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": query.construct_range_object(
                                LAST_MODIFIED_DATE_KEY, start_time, end_time
                            )
                        },
                        # {"bool": {
                        #     "must_not": [
                        #         {"match": {"OrbitNumber": None}},
                        #         {"match": {"OrbitDirection": None}},
                        #     ]
                        # }}
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        es_result = query.run_query_with_scroll(
            body=product_query, index=metadata.HALF_ORBIT_STATUS_INDEX
        )
        return self.__generate_tbl(es_result["hits"]["hits"])
        pass

    def get_w_composite(self, start_com_id, stop_com_id):
        raise NotImplementedError(
            "need to have composite-release-number field in index to get them"
        )

    def get_w_orbit(self, min_orbit, max_orbit):
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": query.construct_range_object(
                                "OrbitNumber", min_orbit, max_orbit
                            )
                        },
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        es_result = query.run_query_with_scroll(
            body=product_query, index=metadata.HALF_ORBIT_STATUS_INDEX
        )
        return self.__generate_tbl(es_result["hits"]["hits"])

    def get_orbit_history(self, half_orbit_id):
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"half_orbit_id": half_orbit_id}},
                    ]
                }
            },
            "sort": self._PROD_ACC_SORT,
        }
        es_result = query.run_query_with_scroll(
            body=product_query, index=metadata.HALF_ORBIT_STATUS_INDEX
        )
        for each in es_result["hits"]["hits"]:
            each = each["_source"]
            current_hit_id = uuid.uuid4()
            if each["sciflo_job_id"] is not None:
                self._result_workflow[each["sciflo_job_id"]] = current_hit_id
            self._result_dict[current_hit_id] = self.__create_prod_acc_dict(each)
            pass
        self.__get_workflow_duration_v2()
        return list(self._result_dict.values())

    pass
