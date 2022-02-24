import logging

from accountability_api.api_utils import metadata
from accountability_api.api_utils.metadata import TIMER_INDEX
from accountability_api.api_utils.utils import (
    ElasticsearchResultDictWrapper,
    chunk_list,
)
from flask_restx import Namespace, Resource, reqparse
from accountability_api.api_utils import query, JOBS_ES

api = Namespace(
    "Timer-Monitor",
    path="/timerMonitor",
    description="Endpoint to retrieve current timers",
)

parser = reqparse.RequestParser()
parser.add_argument(
    "startDateTime", dest="start_dt", type=str, location="args", default=""
)
parser.add_argument("endDateTime", dest="end_dt", type=str, location="args", default="")
parser.add_argument(
    "startOrbit", dest="start_orbit", type=str, location="args", default=""
)
parser.add_argument("endOrbit", dest="end_orbit", type=str, location="args", default="")
parser.add_argument(
    "timer_status", dest="timer_status", type=str, location="args", default="all"
)

LOGGER = logging.getLogger()


# @api.route('/', defaults={'path': 'all'})
@api.route("/<path:timer_names>")
class TimeMonitor(Resource):
    """
    parent level:
    {
        "start_time": "",
        "half_orbit_id": "",
        "expiration_time": "",
        "half_orbit_start_time": "",
        "name": "",
        "status": "",
        "triggered_job_id": "",
        "children": []
    }
    children level:
    {
        "status": "",
        "timestamp": ""
    }

    TODO use collapse to get the latest status & create another endpoint to get all the status
        Or NOT since there are only 2 states for each timer.
    """

    @staticmethod
    def __get_prod_id(id_type_list, dataset_type):
        if id_type_list is None:
            return ""
        for each in id_type_list:
            if each["dataset_type"] == dataset_type:
                return each["id"]
            pass
        return ""

    def __retrieve_jobs(self, job_id_dict):
        if len(job_id_dict) < 1:  # return
            return
        job_query = {
            "query": {
                "bool": {
                    "should": [
                        {"terms": {"payload_id": k}}
                        for k in chunk_list(list(job_id_dict.keys()), 1024)
                    ]
                }
            }
        }
        sources = [
            "payload_id",
            "job.job_info.job_payload.payload_task_id",
            "status",
            "job.job_info.metrics.products_staged.id",
            "job.job_info.metrics.products_staged.dataset_type",
            "job.params.job_specification.params.value.ProductPathGroup.ShortName",
        ]
        job_result = query.run_query_with_scroll(
            index=metadata.JOB_STATUS_INDEX,
            body=job_query,
            es=JOBS_ES,
            _source_include=sources,
        )
        for each in job_result["hits"]["hits"]:
            source = each["_source"]
            job_id = source["payload_id"]

            if (
                job_id not in job_id_dict
            ):  # somehow got the job which is not being looked for.
                LOGGER.warning("job result is not the one in query. {}".format(source))
                continue
            dotted_result = ElasticsearchResultDictWrapper(source)
            dset_type = dotted_result.get_val(
                "job.params.job_specification.params.value.ProductPathGroup.ShortName"
            )
            task_id = dotted_result.get_val("job.job_info.job_payload.payload_task_id")
            prod_id = self.__get_prod_id(
                dotted_result.get_val("job.job_info.metrics.products_staged"), dset_type
            )

            job_id_dict[job_id]["job_status"] = source["status"]
            job_id_dict[job_id]["product_id"] = prod_id
            job_id_dict[job_id]["task_id"] = task_id
            pass
        return

    @api.expect(parser)
    def get(self, timer_names):
        """
        Querying Timer Monitors
        Provide one of following. (Listed by Priority)
        <b>startDateTime - endDateTime</b>: Half Orbit Start Time Range
        <b>startOrbit - endOrbit</b>: Orbit Range
        <b>timer_status</b>: get from the other endpoint


        Ref: https://smap-sds-web.jpl.nasa.gov/confluence/display/SWHD/Timer+Index+and+document+metadata+for+ES
        """
        """
        Querying all products is intentionally not implemented to prevent load on ES


        current row:
        26858D	2020-02-11T00:29:21.000Z	L3_SM_P	2020-02-11T00:10:02.000Z	2020-02-12T18:29:21.000Z	started	SMAP_L2_SM_P_26858_A_20200210T233807_R16515_001.h5, SMAP_L2_SM_P_26858_D_20200211T002722_R16515_001.h5, SMAP_L2_SM_P_26859_A_20200211T011637_R16515_001.h5, SMAP_L2_SM_P_26859_D_20200211T020552_R16515_001.h5, SMAP_L2_SM_P_26860_A_20200211T025502_R16515_001.h5, SMAP_L2_SM_P_26860_D_20200211T034417_R16515_001.h5, SMAP_L2_SM_P_26861_A_20200211T043332_R16515_001.h5, SMAP_L2_SM_P_26861_D_20200211T052247_R16515_001.h5, SMAP_L2_SM_P_26862_A_20200211T061158_R16515_001.h5, SMAP_L2_SM_P_26862_D_20200211T070112_R16515_001.h5, SMAP_L2_SM_P_26863_A_20200211T075027_R16515_001.h5
        :return:
        """
        args = parser.parse_args()
        start_dt, end_dt = args["start_dt"], args["end_dt"]
        start_orbit, end_orbit = args["start_orbit"], args["end_orbit"]
        timer_status = args["timer_status"].lower()
        if start_dt == "" and start_orbit == "":
            return "both date range and orbit range are missing", 500
        if start_dt != "":
            range_query = query.construct_range_object(
                "half_orbit_start_time", start_dt, end_dt
            )
        else:
            range_query = query.construct_range_object(
                "half_orbit_number", start_orbit, end_orbit
            )
        sources = [
            "half_orbit_id",
            "half_orbit_start_time",
            "name",
            "start_time",
            "expiration_time",
            "status",
            "triggered_job_id",
            "timestamp",
            "notes",
            "input_names",
        ]
        timer_query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": range_query},
                    ]
                }
            },
            "sort": [
                {"half_orbit_start_time": "desc"},
                {"timestamp": "desc"},
            ],
        }
        # NOTE: cannot filter it at elastic-search level because doing so will return timers with outdated states..
        # if args['timer_status'].lower() != 'all':
        #     timer_query['query']['bool']['must'].append({'match': {'status': args['timer_status']}})
        if timer_names != "all":
            timer_query["query"]["bool"]["must"].append(
                {"match": {"name": timer_names}}
            )
        timer_response = query.run_query_with_scroll(
            es=JOBS_ES, index=TIMER_INDEX, body=timer_query, _source_include=sources
        )
        result_dict = {}
        job_id_dict = {}
        for each in timer_response["hits"]["hits"]:
            source = each["_source"]
            dotted_result = ElasticsearchResultDictWrapper(source)

            key = "{}__{}".format(source["name"], source["half_orbit_id"])
            if key in result_dict:  # old entry. update the child
                result_dict[key]["children"].append(
                    {
                        "status": source["status"],
                        "timestamp": source["timestamp"],
                    }
                )
                pass
            else:  # new entry.
                new_timer_job = {
                    "name": dotted_result.get_val("name"),
                    "input_names": dotted_result.get_val("input_names", default=""),
                    "half_orbit_id": dotted_result.get_val("half_orbit_id"),
                    "half_orbit_start_time": dotted_result.get_val(
                        "half_orbit_start_time"
                    ),
                    "start_time": dotted_result.get_val("start_time"),
                    "expiration_time": dotted_result.get_val("expiration_time"),
                    "status": dotted_result.get_val("status"),
                    "triggered_job_id": dotted_result.get_val(
                        "triggered_job_id", default=""
                    ),
                    "notes": dotted_result.get_val("notes", default=""),
                    "children": [
                        {
                            "status": dotted_result.get_val("status"),
                            "timestamp": dotted_result.get_val("timestamp"),
                        }
                    ],
                }
                result_dict[key] = new_timer_job
                if new_timer_job["triggered_job_id"] != "":
                    job_id_dict[new_timer_job["triggered_job_id"]] = new_timer_job
                pass
            pass
        if (
            timer_status != "all"
        ):  # does not want all timers.. need to get rid of timers with other states.
            for k in list(result_dict.keys()):
                if result_dict[k]["status"].lower() != timer_status:
                    result_dict.pop(k)
                pass
            pass
        self.__retrieve_jobs(job_id_dict)
        return list(result_dict.values()), 200
