""" Views for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import zipfile

from io import BytesIO
from collections import namedtuple
from django.views.generic import View
from django.http import HttpResponse

from cdm_interface.utils import LayerQuery, WFSQuery


class QueryView(View):

    OutputFormat = namedtuple("OutputFormat", [
        "fetch_key", "output_method", "content_type", "extension"
    ])
    OUTPUT_FORMAT_MAP = {
        "csv": OutputFormat("csv", "to_csv", "text/csv", "csv"),
        "json": OutputFormat("json", "to_json", "application/json", "json"),
    }
    DEFAULT_OUTPUT_FORMAT = "json"
    DEFAULT_COMPRESS_VALUE = "true"

    CODE_TABLES = [
        ("report_type", "type"),
        ("meaning_of_time_stamp", "meaning"),
        ("observed_variable", "variable"),
        ("units", "units"),
        ("observation_value_significance", "significance"),
        ("duration", "duration"),
        ("platform_type", "type"),
        ("station_type", "type"),
        ("quality_flag", "flag"),
        ("data_policy_licence", "policy"),
    ]

    @property
    def response_file_name(self):
        return "results"

    @property
    def output_format(self):

        if not self._output_format:

            key = self.request.GET.get("output_format")
            if not key or key not in self.OUTPUT_FORMAT_MAP:
                key = self.DEFAULT_OUTPUT_FORMAT
            self._output_format = self.OUTPUT_FORMAT_MAP[key]

        return self._output_format

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        self._output_format = None

    def _build_response(self, data):

        compress = json.loads(
            self.request.GET.get("compress", self.DEFAULT_COMPRESS_VALUE))
        if compress:

            content_type = "application/x-zip-compressed"
            response_file_name = f"{self.response_file_name}.zip"

            data = self._compress_data(data, include_code_tables=True)

        else:

            content_type = self.output_format.content_type
            response_file_name = \
                f"{self.response_file_name}.{self.output_format.extension}"

        response = HttpResponse(data, content_type=content_type)
        content_disposition = f"attachment; filename=\"{response_file_name}\""
        response["Content-Disposition"] = content_disposition

        return response

    def _compress_data(self, data, include_code_tables=False):

        file_like_object = BytesIO()
        zipfile_ob = zipfile.ZipFile(file_like_object, "w")
        zipfile_ob.writestr(f"results.{self.output_format.extension}", data)

        if include_code_tables:
            for code_table, code_table_index_field in self.CODE_TABLES:

                query = LayerQuery(
                    code_table,
                    index_field=code_table_index_field,
                    data_format=self.output_format.fetch_key
                )
                code_table_data = query.fetch_data(
                    self.output_format.output_method)

                zipfile_ob.writestr(
                    f"codetables/{code_table}.{self.output_format.extension}",
                    code_table_data
                )

        return file_like_object.getvalue()


class RawWFSView(QueryView):

    def get(self, request):

        query = WFSQuery(**request.GET)
        data = query.fetch_data()

        return self._build_response(data)


class LayerView(QueryView):

    DEFAULT_OUTPUT_FORMAT = "csv"
    DEFAULT_COMPRESS_VALUE = "false"

    layer_name = None
    index_field = None

    @property
    def response_file_name(self):
        return self.layer_name

    def get(self, request, index=None):

        query = LayerQuery(
            self.layer_name,
            index_field=self.index_field,
            index=index,
            count=self.request.GET.get("count"),
            data_format=self.output_format.fetch_key
        )
        data = query.fetch_data(
            self.output_format.output_method)

        return self._build_response(data)


class LiteRecordView(LayerView):
    layer_name = "observations"
    index_field = "observation_id"


class ReportTypeView(LayerView):
    layer_name = "report_type"
    index_field = "type"


class MeaningOfTimeStampView(LayerView):
    layer_name = "meaning_of_time_stamp"
    index_field = "meaning"


class ObservedVariableView(LayerView):
    layer_name = "observed_variable"
    index_field = "variable"


class UnitsView(LayerView):
    layer_name = "units"
    index_field = "units"


class ObservationValueSignificanceView(LayerView):
    layer_name = "observation_value_significance"
    index_field = "significance"


class DurationView(LayerView):
    layer_name = "duration"
    index_field = "duration"


class PlatformTypeView(LayerView):
    layer_name = "platform_type"
    index_field = "type"


class StationTypeView(LayerView):
    layer_name = "station_type"
    index_field = "type"


class QualityFlagView(LayerView):
    layer_name = "quality_flag"
    index_field = "flag"


class DataPolicyLicenceView(LayerView):
    layer_name = "data_policy_licence"
    index_field = "policy"
