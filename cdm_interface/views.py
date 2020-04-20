""" Views for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import re
import json
import zipfile

from io import BytesIO
from collections import namedtuple
from django.views.generic import View
from django.http import HttpResponse

import io

import pandas as pd

from cdm_interface.utils import LayerQuery, WFSQuery, extract_json_records, extract_csv_records
from cdm_interface.wfs_mappings import wfs_mappings


import logging
logging.basicConfig()
log = logging.getLogger(__name__)


class QueryView(View):

    OutputFormat = namedtuple("OutputFormat", [
        "fetch_key", "output_method", "content_type", "extension"
    ])
    OUTPUT_FORMAT_MAP = {
        "csv": OutputFormat("csv", "to_csv", "text/csv", "csv"),
        "json": OutputFormat("json", "to_json", "application/json", "json"),
    }
    DEFAULT_OUTPUT_FORMAT = "csv"
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

        mappers = _get_mappers()

        # Manage the data to remove excess WFS padding in JSON/CSV
        if self.output_format.extension == 'json':
            data = extract_json_records(data)
            data = json.dumps(data)

        if self.output_format.extension == 'csv':
            data = extract_csv_records(data, mappers=mappers)
            data = data.to_csv(index=False)

#        with open('/tmp/aaa', 'w', encoding='utf-8') as writer:
#            writer.write(f'FORMAT: {self.output_format.extension}\n{type(data)}\n')
#            writer.write(f'{data}')

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


mapper_data = {
    'report_type': ['type', 'abbreviation'],
    'meaning_of_time_stamp': ['meaning', 'name'],
    'observed_variable': ['variable', 'name'],
    'units': ['units', 'abbreviation'],
    'observation_value_significance': ['significance', 'description'], 
    'duration': ['duration', 'description'],
    'platform_type': ['type', 'description'],
    'station_type': ['type', 'description'],
    'quality_flag': ['flag', 'description'],
    'data_policy_licence': ['policy', 'name']
}


def _get_mappers():
    mappers = []
    SUPPORTED_MAPPERS = mapper_data.keys()

    for code_table, code_table_index_field in QueryView.CODE_TABLES:
        if code_table not in SUPPORTED_MAPPERS: continue

        mapper = _get_mapper(code_table, code_table_index_field)
        mappers.append((code_table, mapper))

    return mappers
 

def _get_mapper(code_table, code_table_index_field):
    # Download a code table and convert it to, and return a dictionary
    query = LayerQuery(
        code_table,
        index_field=code_table_index_field,
        data_format='csv')

    code_table_data = query.fetch_data('to_csv')

#    with open('/tmp/csv.csv', 'w', encoding='utf-8') as writer:
#        writer.write(f'{code_table_data}') 
 
    df = pd.read_csv(io.StringIO(code_table_data))
    mapper = {}
    in_map, out_map = mapper_data[code_table]

    for i, rec in df.iterrows():
       
#        log.warn(f'record: {rec}')
        mapper[str(rec[in_map])] = rec[out_map]

    return mapper
    

class RawWFSView(QueryView):

    def get(self, request):

        log.warn(f'QUERY STRING: {request.GET}')

        rg = request.GET.copy()
        log.warn(f'COPIED QUERY STRING: {rg}')

        if rg.get('cql_filter'):
            rg['cql_filter'] = self._map_cql_filter(rg['cql_filter'])

        log.warn(f'FIXED QUERY STRING: {rg}')

        query = WFSQuery(**rg)
        data = query.fetch_data()

        return self._build_response(data)


    def _map_cql_filter(self, cql): 
        # NOTE: domain is MANUALLY mapped in strings because simpler than integrating into wfs_mappings
        #
        # E.G.: 'cql_filter': ['date_time DURING 1999-06-01T00:00:00Z/1999-06-01T23:59:59Z AND observed_variable IN (44,85) AND report_type=3 AND platform_type NOT IN (2,5) AND quality_flag=0 AND data_policy_licence=non_commercial']
        to_map = wfs_mappings.keys()
        to_map = ['intended_use', 'frequency', 'data_quality', 'variable']
        cql=cql + ' '
#        comps = cql.split()
        
        r0 = '^(?P<start>.*)'
        r1 = '(?P<end>.*)$'

        for key in to_map:

            mapper = wfs_mappings[key]
            target_field = mapper['target']

            res = []
            
#            re.match('^(?P<start>.*)(?P<found>x in y)(?P<end>.*)$', 'hello x in y').groupdict()
            regex = f'{r0}(?P<found>{key}=)(?P<value>[^ ]+)\s*{r1}'
            log.warn(f'regex: {regex}')
            m1 = re.match(regex, cql)

            if m1:
                d = m1.groupdict()
                mapped_value = mapper['fields'][d['value']]
                value = f'{target_field}={mapped_value}'
                cql = d['start'] + value + ' ' + d['end'].strip()
                log.warn(f'UPDATE TO cql: {cql}')

            regex = f'{r0}(?P<found>{key} IN )(?P<value>[^ ]+)\s*{r1}'
            log.warn(f'regex: {regex}')
            m1 = re.match(regex, cql)

            if m1:
                d = m1.groupdict()
                items = d['value'].split('(')[1][:-1].split(',')
                mapped_value = ','.join([mapper['fields'][_] for _ in items])
                value = f'{target_field} IN ({mapped_value})'
                cql = d['start'] + value + ' ' + d['end'].strip()
                log.warn(f'UPDATE TO cql: {cql}')

        regex = f'{r0}(?P<found>domain=)(?P<value>[^ ]+)\s*{r1}'
        log.warn(f'regex: {regex}')
        m1 = re.match(regex, cql)

        if m1:
            d = m1.groupdict()
            if d['value'] == 'land':
                mapped_value = 'platform_type NOT IN (2,5)'
            elif d['value'] == 'marine':
                mapped_value = 'platform_type IN (2,5)'
                
            value = mapped_value
            cql = d['start'] + value + ' ' + d['end'].strip()
            log.warn(f'UPDATE TO cql: {cql}')

 
        for comp in []: #comps:
                value = comp

                if value.startswith(f'{key}='):
                    mapped_value = mapper['fields'][value.split('=')[1]]
                    log.warn(f'{value} --> {target_field}={mapped_value}')
                    res.append(f'{target_field}={mapped_value}')
                elif value.startswith(f'{key} IN ('):
                    items = value.split('(')[1][:-1].split(',')
                    mapped_value = ','.join([mapper['fields'][_] for _ in items])
                    res.append(f'{target_field}={mapped_value}')
                elif value == 'domain=land':
                    res.append('platform_type NOT IN (2,5)')
                elif value == 'domain=marine':
                    res.append('platform_type IN (2,5)') 
                else:
                    res.append(value)

#            comps = res[:]

        return cql.strip()


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
