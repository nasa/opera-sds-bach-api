# import logging

# from flask_restx import Namespace, Resource
# from accountability_api.api_utils import query, JOBS_ES

# api = Namespace("SMAP Names", path="/names", description="Retrieving Lists of Names")
# LOGGER = logging.getLogger()


# @api.route("/<path:name_type>")
# class SupportNames(Resource):
#     def get_timer_names(self):
#         timer_query = {
#             "query": {"match_all": {}},
#             "from": 0,
#             "aggs": {"count_names": {"terms": {"field": "name", "size": 10000}}},
#         }
#         timer_response = query.run_query(
#             es=JOBS_ES, index=TIMER_INDEX, body=timer_query, size=0
#         )
#         if (
#             "aggregations" not in timer_response
#             or "count_names" not in timer_response["aggregations"]
#             or "buckets" not in timer_response["aggregations"]["count_names"]
#         ):
#             LOGGER.error(
#                 "response from timer names do not have valid keywords. response: {}".format(
#                     timer_response
#                 )
#             )
#             return []
#         return [
#             k["key"] for k in timer_response["aggregations"]["count_names"]["buckets"]
#         ], 200

#     def get_daacs(self):
#         pdr_config = PDRConfig()
#         return pdr_config.daac, 200

#     def get_processing_mode(self):
#         """
#         Using PDR indices to retrieve processing-mode
#         :return:
#         """
#         proc_mode_query = {
#             "query": {"match_all": {}},
#             "aggs": {
#                 "processing_modes": {
#                     "terms": {"field": "metadata.PROCESSING_MODE.raw", "size": 10000}
#                 }
#             },
#         }
#         pdr_processing_mode_response = query.run_query(
#             index=SIGNAL_FILES_ALIAS, body=proc_mode_query, size=0
#         )
#         return [
#             k["key"]
#             for k in pdr_processing_mode_response["aggregations"]["processing_modes"][
#                 "buckets"
#             ]
#         ], 200

#     @api.response(code=200, description="Array of a given name types")
#     @api.response(code=500, description="When the name_type is not valid")
#     def get(self, name_type):
#         """
#         Get all Name Types

#         Valid name_type:

#         - timer_names
#         - daacs
#         - processing_modes
#         - preprocessor_workflows

#         """
#         if name_type.startswith("timer_names"):
#             return self.get_timer_names()
#         if name_type.startswith("daacs"):
#             return self.get_daacs()
#         if name_type.startswith("processing_modes"):
#             return self.get_processing_mode()

#         return {"message": "{} is invalid name".format(name_type)}, 500
