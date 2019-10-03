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

from cdm_interface.utils import fetch_data


CODE_TABLES = [
    'report_type',
    'meaning_of_time_stamp',
    'observed_variable',
    'units',
    'observation_value_significance',
    'duration',
    'platform_type',
    'station_type',
    'quality_flag',
    'data_policy_licence',
]


class LayerView(View):

    layer_name = None
    index_field = None

    def get(self, request, index=None):

        data = fetch_data(
            self.layer_name,
            index_field=self.index_field,
            index=index,
            count=self.request.GET.get('count')
        )

        compress = json.loads(request.GET.get('compress', 'false'))
        if compress:

            file_like_object = BytesIO()
            zipfile_ob = zipfile.ZipFile(file_like_object, 'w')
            zipfile_ob.writestr('results.json', data.to_json(orient='records'))

            for code_table in CODE_TABLES:
                code_table_data = fetch_data(code_table)
                code_table_json = code_table_data.to_json(orient='records')

                zipfile_ob.writestr(f'codetables/{code_table}.json', code_table_json)

            return HttpResponse(
                file_like_object.getvalue(),
                content_type='application/x-zip-compressed'
            )

        else:

            return HttpResponse(
                data.to_json(orient='records'),
                content_type='application/json'
            )


class LiteRecordView(LayerView):
    layer_name = 'source_configuration'
    index_field = 'id'


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
