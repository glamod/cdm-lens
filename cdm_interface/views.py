""" Views for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import zipfile

from io import BytesIO
from django.views.generic import View
from django.http import HttpResponse

from cdm_interface.utils import fetch_data, format_data, get_output_format


CODE_TABLES = [
    ('report_type', 'type'),
    ('meaning_of_time_stamp', 'meaning'),
    ('observed_variable', 'variable'),
    ('units', 'units'),
    ('observation_value_significance', 'significance'),
    ('duration', 'duration'),
    ('platform_type', 'type'),
    ('station_type', 'type'),
    ('quality_flag', 'flag'),
    ('data_policy_licence', 'policy'),
]


class LayerView(View):

    layer_name = None
    index_field = None

    def get(self, request, index=None):

        output_format = get_output_format(request.GET.get('format'))

        data = fetch_data(
            self.layer_name,
            index_field=self.index_field,
            index=index,
            count=self.request.GET.get('count'),
            format_key=output_format.fetch_key
        )

        compress = json.loads(request.GET.get('compress', 'false'))
        if compress:

            file_like_object = BytesIO()
            zipfile_ob = zipfile.ZipFile(file_like_object, 'w')
            data = format_data(data, output_format.output_method)
            zipfile_ob.writestr(f'results.{output_format.file_extension}', data)

            for code_table, code_table_index_field in CODE_TABLES:

                code_table_data = fetch_data(
                    code_table,
                    index_field=code_table_index_field,
                    format_key=output_format.fetch_key
                )
                code_table_data = format_data(
                    code_table_data, output_format.output_method
                )

                zipfile_ob.writestr(
                    f'codetables/{code_table}.{output_format.file_extension}',
                    code_table_data
                )

            response = HttpResponse(
                file_like_object.getvalue(),
                content_type='application/x-zip-compressed'
            )
            response_file_name = 'results.zip'

        else:

            response = HttpResponse(
                format_data(data, output_format.output_method),
                content_type=output_format.content_type
            )
            response_file_name = f'results.{output_format.file_extension}'

        content_disposition = f'attachment; filename="{response_file_name}"'
        response['Content-Disposition'] = content_disposition
        return response


class LiteRecordView(LayerView):
    layer_name = 'source_configuration'
    index_field = 'source_id'


class ReportTypeView(LayerView):
    layer_name = 'report_type'
    index_field = 'type'


class MeaningOfTimeStampView(LayerView):
    layer_name = 'meaning_of_time_stamp'
    index_field = 'meaning'


class ObservedVariableView(LayerView):
    layer_name = 'observed_variable'
    index_field = 'variable'


class UnitsView(LayerView):
    layer_name = 'units'
    index_field = 'units'


class ObservationValueSignificanceView(LayerView):
    layer_name = 'observation_value_significance'
    index_field = 'significance'


class DurationView(LayerView):
    layer_name = 'duration'
    index_field = 'duration'


class PlatformTypeView(LayerView):
    layer_name = 'platform_type'
    index_field = 'type'


class StationTypeView(LayerView):
    layer_name = 'station_type'
    index_field = 'type'


class QualityFlagView(LayerView):
    layer_name = 'quality_flag'
    index_field = 'flag'


class DataPolicyLicenceView(LayerView):
    layer_name = 'data_policy_licence'
    index_field = 'policy'
