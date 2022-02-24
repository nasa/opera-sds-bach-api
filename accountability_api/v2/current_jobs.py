import logging

from api_utils.query import construct_orbit_range_obj
from api_utils.utils import (
    from_iso_to_dt,
    from_dt_to_iso,
    ElasticsearchResultDictWrapper,
)
from accountability_api.api_utils import query
from accountability_api.api_utils import JOBS_ES, GRQ_ES
from collections import defaultdict

LOGGER = logging.getLogger()


class LatestJobProd:
    """
    Helper class to keep a job and product.
    If a job is already there, adding a new job is rejected.
    """

    def __init__(self):
        self._job = None
        self._prod = None
        pass

    @property
    def job(self):
        return self._job

    @job.setter
    def job(self, val):
        if self._job is None:
            self._job = val
        return

    @property
    def product(self):
        return self._prod

    @product.setter
    def product(self, val):
        if self._prod is None:
            self._prod = val
        return

    def choose(self):
        """
        if the job is completed, return the product
        if the job is not, return the job or product with the latest time stamp
        :return:
        """
        if self.job is None and self.product is None:
            return None
        if self.job is None:
            return self.product
        if self.product is None:
            return self.job
        if self.job["status"] == "completed":
            return self.product

        job_time = from_iso_to_dt(self.job["Timestamp"])
        prod_time = from_iso_to_dt(self.product["Timestamp"])
        if job_time > prod_time:
            return self.job
        return self.product

    pass


class ProdAccountability:
    def __init__(self):
        self._job_sources = [
            "job_id",
            "status",
            "job.job_info.metrics.products_staged.id",
            "job.job_info.metrics.products_staged.dataset_type",
            "job.params.job_specification.params.value.Geometry",
            "job.params.job_specification.params.value.ProductPathGroup",
            "job.params.job_specification.params.value.PrimaryExecutable.CompositeReleaseID",
            "@timestamp",
            "payload_id",
            "type",
        ]
        self._prod_sources = [
            "metadata.ProductReceivedTime",
            "metadata.ShortName",
            "prov.wasGeneratedBy",
            "metadata.OrbitNumber",
            "metadata.CompositeReleaseID",
            "metadata.OrbitDirection",
            "metadata.ProductCounter",
            "id",
        ]
        self._prod_acc = defaultdict(lambda: defaultdict(LatestJobProd))
        self._min_orbit = None
        self._max_orbit = None
        self._start_dt = None
        self._end_dt = None
        pass

    @property
    def start_dt(self):
        return from_dt_to_iso(self._start_dt)

    @start_dt.setter
    def start_dt(self, val):
        val_dt = from_iso_to_dt(val)
        if self._start_dt is None:
            self._start_dt = val_dt
        elif self._start_dt > val_dt:
            self._start_dt = val_dt
        return

    @property
    def end_dt(self):
        return from_dt_to_iso(self._end_dt)

    @end_dt.setter
    def end_dt(self, val):
        val_dt = from_iso_to_dt(val)
        if self._end_dt is None:
            self._end_dt = val_dt
        elif self._end_dt < val_dt:
            self._end_dt = val_dt
        return

    @property
    def min_orbit(self):
        return self._min_orbit

    @min_orbit.setter
    def min_orbit(self, val):
        if self.min_orbit is None:
            self._min_orbit = val
        elif self.min_orbit > val:
            self._min_orbit = val
        return

    @property
    def max_orbit(self):
        return self._max_orbit

    @max_orbit.setter
    def max_orbit(self, val):
        if self.max_orbit is None:
            self._max_orbit = val
        elif self.max_orbit < val:
            self._max_orbit = val
        return

    def __set_orbit(self, orbit_num):
        int_orbit_num = int(orbit_num)
        self.min_orbit = int_orbit_num
        self.max_orbit = int_orbit_num
        return

    def __set_dt(self, dt):
        self.start_dt = dt
        self.end_dt = dt
        return

    def __get_jobs(self, include_completed=True):
        job_query = {
            "query": {
                "bool": {
                    "must": [
                        {"wildcard": {"type": "job-smap_pge:*"}},
                        {
                            "range": query.construct_range_object(
                                "@timestamp", self.start_dt, self.end_dt
                            )
                        },
                    ]
                }
            }
        }
        if not include_completed:
            job_query["query"]["bool"]["must"].append(
                {"bool": {"must_not": [{"term": {"status": "job-completed"}}]}}
            )
        job_response = query.run_query_with_scroll(
            es=JOBS_ES,
            body=job_query,
            index="job_status-current",
            size=1000,
            sort="@timestamp:desc",
            _source_include=self._job_sources,
        )
        LOGGER.info("JOBS_ES query size: {}".format(len(job_response["hits"]["hits"])))
        return job_response

    def __get_prod_by_orbit(self):
        # TODO use collapse when it becomes available
        # TODO use exists when it becomes available
        product_query = {
            "query": {
                "bool": {
                    "must": [
                        construct_orbit_range_obj(
                            "OrbitNumber", self.min_orbit, self.max_orbit
                        )
                    ]
                }
            }
        }
        # sort by latest query date so the older products are not used.
        resp = query.run_query_with_scroll(
            es=GRQ_ES,
            body=product_query,
            index="products",
            sort="metadata.ProductReceivedTime:desc",
            _source_include=self._prod_sources,
        )
        LOGGER.info("GRQ query size: {}".format(len(resp["hits"]["hits"])))
        return resp

    @staticmethod
    def __get_latest_dt(left_dt, right_str):
        right_dt = from_iso_to_dt(right_str)
        if left_dt is None:
            return right_dt
        if left_dt > right_dt:
            return left_dt
        return right_dt

    def __generate_tbl(self):
        resulting_list = []
        for (
            k,
            v,
        ) in (
            self._prod_acc.items()
        ):  # TODO 'Last Modified' logic is wrong. CompositeReleaseID is also wrong.
            row = {"half_orbit_id": k}
            last_modified_dt = None
            for v2 in v.values():
                product = v2.choose()
                sn = product["ShortName"]
                if (
                    sn + "_job_id" not in row
                ):  # new job. need to add to the list. ignoring the duplicated results
                    row[sn + "_job_status"] = product["status"]
                    row[sn + "_ProductCounter"] = product["ProductCounter"]
                    row[sn + "_job_id"] = product["job_id"]
                    row[sn + "_ProductName"] = product["ProductName"]
                    last_modified_dt = self.__get_latest_dt(
                        last_modified_dt, product["Timestamp"]
                    )
                    row["OrbitNumber"] = product["OrbitNumber"]
                    row["OrbitDirection"] = product["OrbitDirection"]
                    row["CompositeReleaseID"] = product["CompositeReleaseID"]
                pass
            row["Last Modified"] = from_dt_to_iso(last_modified_dt)
            resulting_list.append(row)
            pass
        return resulting_list

    def __get_by_date_prod_portion(self):
        resp = self.__get_prod_by_orbit()
        for hit in resp["hits"]["hits"]:
            doc = hit.get("_source")
            metadata = doc.get("metadata")
            orbit_id = "{}{}".format(
                doc.get("metadata").get("OrbitNumber"),
                str(doc.get("metadata").get("OrbitDirection"))[0],
            )
            self._prod_acc[orbit_id][metadata["ShortName"]].product = {
                "ShortName": metadata.get("ShortName"),
                "OrbitNumber": metadata.get("OrbitNumber"),
                "OrbitDirection": metadata.get("OrbitDirection"),
                "ProductCounter": metadata.get("ProductCounter"),
                "Timestamp": metadata.get("ProductReceivedTime"),
                "status": "job-completed",
                "job_id": str(doc.get("prov").get("wasGeneratedBy")).replace(
                    "task_id:", ""
                ),
                "ProductName": doc.get("id"),
                "CompositeReleaseID": metadata["CompositeReleaseID"],
            }
        return self.__generate_tbl()

    def get_by_date(self, start_time, end_time):
        self.start_dt = start_time
        self.end_dt = end_time
        job_response = self.__get_jobs()
        for elem in job_response["hits"]["hits"]:
            source = elem["_source"]
            current_orbit_dict = {
                "status": source["status"],
                "Timestamp": source["@timestamp"],
                "job_id": source["payload_id"],
            }
            dotted_result = ElasticsearchResultDictWrapper(source)

            sub_dict = dotted_result.get_val(
                "job.params.job_specification.params.value"
            )
            if sub_dict is not None:
                dotted_sub_dict = ElasticsearchResultDictWrapper(sub_dict)
                orbit_num = dotted_sub_dict.get_val("Geometry.OrbitNumber")
                orbit_dir = dotted_sub_dict.get_val("Geometry.OrbitDirection")
                short_name = dotted_sub_dict.get_val("ProductPathGroup.ShortName")
                if (
                    orbit_num is None or orbit_dir is None or short_name is None
                ):  # validation failed
                    LOGGER.warning(
                        "orbit_number or direction or Short_Name not found in result: {}".format(
                            source
                        )
                    )
                    continue
                self.__set_orbit(orbit_num)
                orbit_id = "{}{}".format(orbit_num, orbit_dir[0])
                product_name = "Product not Found"
                products_staged = dotted_result.get_val(
                    "job.job_info.metrics.products_staged"
                )
                if products_staged is not None:
                    for product in products_staged:
                        if product["dataset_type"] == short_name:
                            product_name = product["id"]
                            break
                        pass
                    pass
                current_orbit_dict["ShortName"] = short_name
                current_orbit_dict["OrbitNumber"] = orbit_num
                current_orbit_dict["OrbitDirection"] = orbit_dir
                current_orbit_dict["ProductCounter"] = dotted_sub_dict.get_val(
                    "ProductPathGroup.ProductCounter"
                )
                current_orbit_dict["ProductName"] = product_name
                current_orbit_dict["CompositeReleaseID"] = dotted_sub_dict.get_val(
                    "PrimaryExecutable.CompositeReleaseID"
                )
                self._prod_acc[orbit_id][short_name].job = current_orbit_dict
                pass
            else:
                LOGGER.warning(
                    "job.params.job_specification.params.value not in result {}".format(
                        source
                    )
                )
                pass
            pass
        return self.__get_by_date_prod_portion()

    def __get_jobs_for_orbits(self):
        job_response = self.__get_jobs(False)
        for elem in job_response["hits"]["hits"]:
            source = elem["_source"]
            current_orbit_dict = {
                "status": source["status"],
                "Timestamp": source["@timestamp"],
                "job_id": source["payload_id"],
            }
            dotted_result = ElasticsearchResultDictWrapper(source)
            sub_dict = dotted_result.get_val(
                "job.params.job_specification.params.value"
            )
            if sub_dict is not None:
                dotted_sub_dict = ElasticsearchResultDictWrapper(sub_dict)
                orbit_num = dotted_sub_dict.get_val("Geometry.OrbitNumber")
                orbit_dir = dotted_sub_dict.get_val("Geometry.OrbitDirection")
                short_name = dotted_sub_dict.get_val("ProductPathGroup.ShortName")
                if (
                    orbit_num is None or orbit_dir is None or short_name is None
                ):  # validation failed
                    LOGGER.warning(
                        "orbit_number or direction or Short_Name not found in result: {}".format(
                            source
                        )
                    )
                    continue
                orbit_num_int = int(orbit_num)
                if not (
                    self.min_orbit <= orbit_num_int <= self.max_orbit
                ):  # no need to store this orbit
                    continue
                orbit_id = "{}{}".format(orbit_num, orbit_dir[0])
                product_name = "Product not Found"
                products_staged = dotted_result.get_val(
                    "job.job_info.metrics.products_staged"
                )
                if products_staged is not None:
                    for product in products_staged:
                        if product["dataset_type"] == short_name:
                            product_name = product["id"]
                            break
                        pass
                    pass
                current_orbit_dict["ShortName"] = short_name
                current_orbit_dict["OrbitNumber"] = orbit_num
                current_orbit_dict["OrbitDirection"] = orbit_dir
                current_orbit_dict["ProductCounter"] = dotted_sub_dict.get_val(
                    "ProductPathGroup.ProductCounter"
                )
                current_orbit_dict["ProductName"] = product_name
                current_orbit_dict["CompositeReleaseID"] = dotted_sub_dict.get_val(
                    "PrimaryExecutable.CompositeReleaseID"
                )
                self._prod_acc[orbit_id][short_name].job = current_orbit_dict
                pass
            else:
                LOGGER.warning(
                    "job.params.job_specification.params.value not in result {}".format(
                        source
                    )
                )
                pass
            pass
        return self.__generate_tbl()

    def get_by_orbit(self, start_orbit, end_orbit):
        self.min_orbit = start_orbit
        self.max_orbit = end_orbit
        resp = self.__get_prod_by_orbit()
        for hit in resp["hits"]["hits"]:
            doc = hit["_source"]
            metadata = doc["metadata"]
            self.__set_dt(metadata.get("ProductReceivedTime"))
            orbit_id = "{}{}".format(
                doc.get("metadata").get("OrbitNumber"),
                str(doc.get("metadata").get("OrbitDirection"))[0],
            )
            self._prod_acc[orbit_id][metadata["ShortName"]].product = {
                "ShortName": metadata.get("ShortName"),
                "OrbitNumber": metadata.get("OrbitNumber"),
                "OrbitDirection": metadata.get("OrbitDirection"),
                "ProductCounter": metadata.get("ProductCounter"),
                "Timestamp": metadata.get("ProductReceivedTime"),
                "status": "job-completed",
                "job_id": str(doc.get("prov").get("wasGeneratedBy")).replace(
                    "task_id:", ""
                ),
                "ProductName": doc.get("id"),
                "CompositeReleaseID": metadata["CompositeReleaseID"],
            }
            pass
        return self.__get_jobs_for_orbits()

    pass
