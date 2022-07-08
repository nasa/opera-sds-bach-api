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
        self.start_datetime = start_date
        self.end_datetime = end_date

        self._args = kwargs

        self._crid = kwargs.get("crid", "")
        self._venue = kwargs.get("venue", "")
        self._processing_mode = kwargs.get("processing_mode", "")
        self._report_type = kwargs.get("report_type", "")

        # override report_type if "detailed" arg is supplied
        if type(kwargs.get("detailed")) is bool:
            if kwargs["detailed"]:
                self._report_type = "detailed"
            else:
                self._report_type = "brief"
        elif type(kwargs.get("detailed")) is str:
            self._report_type = kwargs["detailed"]

        self._dt_format = "%Y-%m-%dT%H:%M:%S"

        self._creation_time = utils.from_iso_to_dt(self._creation_time).strftime(self._dt_format)
        self.start_datetime = utils.from_iso_to_dt(self.start_datetime).strftime(self._dt_format)
        self.end_datetime = utils.from_iso_to_dt(self.end_datetime).strftime(self._dt_format)

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
        pass

    @abstractmethod
    def to_xml(self):
        pass

    @abstractmethod
    def to_csv(self):
        normalized = pd.json_normalize(self.get_data())
        return normalized.to_csv(index=False, sep="\t", encoding="utf-8")

    @abstractmethod
    def get_filename(self, output_format):
        pass
