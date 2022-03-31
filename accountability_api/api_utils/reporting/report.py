import json
import pandas as pd

from abc import ABC, abstractmethod

from accountability_api.api_utils import utils, query


class Report(ABC):
    """
    https://wiki.jpl.nasa.gov/display/NISARSDS/04-B1a.+MS+Data+Accounting

    """

    def __init__(self, title: str, start_date: str, end_date: str, timestamp: str, **kwargs):
        self._title = title
        self._creation_time = timestamp
        self._start_datetime = start_date
        self._end_datetime = end_date

        self._args = kwargs

        self._crid = ""
        self._venue = ""
        self._processing_mode = ""
        self._report_type = ""

        for key, val in kwargs.items():
            if key == "crid":
                self._crid = val
            elif key == "venue":
                self._venue = val
            elif key == "processing_mode":
                self._processing_mode = val
            elif key == "detailed":
                if type(val) is bool:
                    if val:
                        self._report_type = "detailed"
                    else:
                        self._report_type = "brief"
                elif type(val) is str:
                    self._report_type = val
            elif key == "report_type":
                self._report_type = val

        self._dt_format = "%Y-%m-%dT%H:%M:%S"

        self._creation_time = utils.from_iso_to_dt(self._creation_time).strftime(
            self._dt_format
        )
        self._start_datetime = utils.from_iso_to_dt(self._start_datetime).strftime(
            self._dt_format
        )
        self._end_datetime = utils.from_iso_to_dt(self._end_datetime).strftime(
            self._dt_format
        )

        self.report = {}

        super().__init__()

    def add_universal_query_params(self, query_body):
        if self._crid:
            query_body = query.add_query_filter(
                query=query_body,
                field_name="metadata.CompositeReleaseID",
                value=self._crid,
            )
        if self._processing_mode:
            query_body = query.add_query_filter(
                query=query_body,
                field_name="metadata.ProcessingType",
                value=self._processing_mode,
            )
        return query_body

    @abstractmethod
    def populate_data(self):
        pass

    @abstractmethod
    def generate_report(self, output_format=None):
        self.populate_data()

        self.filename = self.get_filename(output_format)
        if output_format == "xml" or output_format is None:
            return self.to_xml()
        elif output_format == "csv":
            return self.to_csv()
        elif output_format == "json":
            return self.to_json()
        else:
            return self.get_dict_format()

    @abstractmethod
    def get_data(self):
        return {}

    @abstractmethod
    def to_json(self):
        return json.dumps(self.get_dict_format())

    @abstractmethod
    def get_dict_format(self):
        return {}

    @abstractmethod
    def to_xml(self):
        return ""

    @abstractmethod
    def to_csv(self):
        normalized = pd.json_normalize(self.get_data())
        return normalized.to_csv(index=False, sep="\t", encoding="utf-8")

    @abstractmethod
    def get_filename(self, output_format):
        return "*.{}".format(output_format)
