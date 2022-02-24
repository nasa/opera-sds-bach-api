from .report import Report
from accountability_api.api_utils import utils, query
from accountability_api.api_utils import metadata as consts


class ObservationAccountabilityReport(Report):
    """
    https://wiki.jpl.nasa.gov/display/NISARSDS/04-B1a.+MS+Data+Accounting

    """

    def __init__(self, title, start_date, end_date, timestamp, **kwargs):
        super(ObservationAccountabilityReport, self).__init__(
            title, start_date, end_date, timestamp, **kwargs
        )
        self._dt_format = "%Y-%jT%H:%M:%S"
        self._start_datetime = utils.from_dt_to_iso(
            utils.from_iso_to_dt(self._start_datetime), custom_format=self._dt_format
        )
        self._end_datetime = utils.from_dt_to_iso(
            utils.from_iso_to_dt(self._end_datetime), custom_format=self._dt_format
        )
        self._creation_time = utils.from_dt_to_iso(
            utils.from_iso_to_dt(self._creation_time), custom_format=self._dt_format
        )
        self._content_type = "NASA"
        self._observations = []

    def get_dict_format(self):
        return {
            "root_name": "OBSERVATION_ACCOUNTING_FILE",
            "HEADER": {
                "CONTENT_TYPE": self._content_type,
                "CREATION_DATETIME": self._creation_time,
                "START_DATETIME": self._start_datetime,
                "END_DATETIME": self._end_datetime,
            },
            "OBSERVATION": self._observations,
        }

    def strip_extra(self, dt):
        split_dt = dt.split("T")
        half1 = "".join(split_dt[0].split("-"))
        half2 = "".join(split_dt[1].split(":")).split(".")[0]
        return half1 + half2

    def obs_id_to_dt(self, obs_id):
        dt_obs_id = "{y}-{doy}T{h}:{M}:{s}.000000Z".format(
            y=obs_id[0:4],
            doy=obs_id[4:7],
            h=obs_id[7:9],
            M=obs_id[9:11],
            s=obs_id[11:13],
        )
        dt = utils.from_iso_to_dt(dt_obs_id)
        return utils.from_dt_to_iso(dt, custom_format="%Y-%m-%dT%H:%M:%S")

    def populate_data(self):
        obs_ids = self._get_observations_within_timeframe()
        observations = self._get_l0b_observations(obs_ids)

        for data in observations:
            # cleans up and makes all the keys [key_name]:"" instead of metadata.[key_name]: ""
            if data is None:
                continue

            obs = {}
            obs["OBS_ID"] = data["runconfig"]["Observations"][0]["PlannedObservationId"]
            obs["PRODUCT_SIZE"] = data["FileSize"]
            # LATENCY
            # from L0B_L_RRSD catalog metadata, pull metadata.RangeStartDateTime
            # from L0B_L_RRSD catalog metadata, pull metadata.Production_DateTime
            # perform delta time and report using the units defined
            range_start_datetime = utils.from_iso_to_dt(data["RangeStartDateTime"])
            production_datetime = utils.from_iso_to_dt(data["Production_DateTime"])
            dt_str = utils.from_td_to_str(production_datetime - range_start_datetime)
            obs["LATENCY"] = dt_str
            # PERCENT_COMPLETE
            # calculate ((TotalNumberRangelines - TotalNumberOfMissingRangelines) / TotalNumberRangelines) * 100
            percent_complete = (
                (
                    (
                        data["TotalNumberRangelines"]
                        - (
                            data["TotalNumberOfMissingRangelines"]
                            + data["TotalNumberOfRangelinesFailedChecksum"]
                        )
                    )
                    / data["TotalNumberRangelines"]
                )
            ) * 100
            obs["PERCENT_COMPLETE"] = percent_complete
            if percent_complete == 100:
                obs["COMPLETENESS"] = "complete"
            elif percent_complete == 0:
                obs["COMPLETENESS"] = "missing"
            else:
                obs["COMPLETENESS"] = "partial"

            self._observations.append(obs)

    def _get_observations_within_timeframe(self):
        # make sure that start_date and end_date are iso formatted
        r_start = query.construct_range_object(
            "ref_start_datetime_iso", start_value=self._start_datetime, stop_value=None
        )
        r_end = query.construct_range_object(
            "ref_end_datetime_iso", start_value=None, stop_value=self._end_datetime
        )

        body = {"query": {"bool": {"must": [{"range": r_start}, {"range": r_end}]}}}

        body = self.add_universal_query_params(body)

        try:
            results = query.run_query(
                index=consts.ACCOUNTABILITY_INDEXES["OBSERVATION"],
                body=body,
                doc_type="_doc",
                size=1000,
            )
        except Exception:
            print("could not find index")
            return []
        return list(map(lambda doc: doc["_source"], results["hits"]["hits"]))

    def _get_l0b_observations(self, obs_ids):
        obs_body = {"query": {"bool": {"should": []}}}
        for obs in obs_ids:
            for obs_id in obs["observation_ids"]:
                obs_body = query.add_query_find(
                    query=obs_body,
                    field_name="metadata.runconfig.Observations.PlannedObservationId",
                    value=obs_id,
                )

        obs_source_includes = [
            "metadata.FileSize",
            "metadata.RangeStartDateTime",
            "metadata.Production_DateTime",
            "metadata.TotalNumberRangelines",
            "metadata.TotalNumberOfMissingRangelines",
            "metadata.TotalNumberOfRangelinesFailedChecksum",
            "metadata.runconfig.Observations.PlannedObservationId",
        ]

        obs_body["_source"] = obs_source_includes

        obs_body = self.add_universal_query_params(obs_body)

        obs_results = query.run_query(
            index=consts.PRODUCT_INDEXES["L0B_L_RRSD"], body=obs_body
        )

        tmpresults = list(map(lambda doc: doc["_source"], obs_results["hits"]["hits"]))
        results = []
        for r in tmpresults:
            results.append(r["metadata"])
        return results

        # start 2022-01-08T06:02:28.000000Z
        # end 2022-008T08:14:56.000000Z

    def get_data(self):
        return self._observations

    def to_xml(self):
        data = self.get_dict_format()
        root_name = "root"
        if "root_name" in data:
            root_name = data["root_name"]
            del data["root_name"]

        return utils.convert_to_xml_str(root_name, data)

    def to_json(self):
        return super().to_json()

    def to_csv(self):
        return super().to_csv()

    def get_filename(self, output_format):
        return "OAD_{}_{}_{}.{}".format(
            self._content_type, self._start_datetime, self._end_datetime, output_format
        )

    def generate_report(self, output_format=None):
        return super().generate_report(output_format=output_format)
