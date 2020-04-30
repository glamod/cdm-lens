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
from cdm_interface._loader import local_conn_str

import logging
logging.basicConfig()
log = logging.getLogger(__name__)




class SelectView(View):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #self._output_format = None

    @property
    def response_file_name(self):
        return "results"

    @property
    def output_format(self):
        return "csv"

    def get(self, request):
        log.warn(f'QUERY STRING: {request.GET}')

#        log.warn(f'COPIED QUERY STRING: {rg}')

        # if rg.get('cql_filter'):
        #     rg['cql_filter'] = self._map_cql_filter(rg['cql_filter'])
        # query = WFSQuery(**rg)
        # data = query.fetch_data()
        if 1:#try:
            qm = QueryManager()
            data = qm.run_query(request.GET.dict())
        else:#except KeyError as exc:
            return HttpResponse(f'Exception raised when running query: {str(exc)}')

        log.warn(f'LENGTH: {len(data)}')
        return self._build_response(data)

    def _build_response(self, data):
        mappers = _get_mappers()

#        data = extract_csv_records(data, mappers=mappers)
        data = data.to_csv(index=False)

        content_type = "application/x-zip-compressed"
        response_file_name = f"{self.response_file_name}.zip"

        data = self._compress_data(data)

        response = HttpResponse(data, content_type=content_type)
        content_disposition = f"attachment; filename=\"{response_file_name}\""
        response["Content-Disposition"] = content_disposition

        return response

    def _compress_data(self, data):

        file_like_object = BytesIO()
        zipfile_ob = zipfile.ZipFile(file_like_object, "w")
        zipfile_ob.writestr(f"{self.response_file_name}.{self.output_format}", data)
        
        return file_like_object.getvalue()



import os
import pandas
import psycopg2

# noinspection SqlDialectInspection
class QueryManager(object):

    def __init__(self, conn_str=local_conn_str):
        self._conn = psycopg2.connect(conn_str)

    def run_query(self, kwargs):
        log.warn(f'kwargs: {kwargs}')
        self._validate_request(kwargs)        

        dfs = []

        for sql_query in self._generate_queries(kwargs):
            log.warn(f'RUNNING SQL: {sql_query}')
            df = pandas.read_sql(sql_query, self._conn)
            dfs.append(df)

        if len(dfs) == 1:
            df = dfs[0]
        else:
            df = pandas.concat(dfs)

#        sql_query = self._parse_args(kwargs)

        if kwargs.get('column_selection', None) == 'basic_metadata':
            columns = ['observation_id', 'date_time', 'observation_duration', 'longitude',
                                     'latitude', 'height_above_surface', 'observed_variable', 'units',
                                     'observation_value', 'value_significance', 'primary_station_id',
                                     'station_name', 'quality_flag']
            df = df[columns]

        else:
            df = df.drop(columns=['location'])

        # df.to_csv('out.csv', sep=',', index=False, float_format='%.3f',
        #           date_format='%Y-%m-%d %H:%M:%S%z')
        return df


    def _validate_request(self, kwargs):
        required = ['domain', 'frequency', 'variable', 'bbox', 'year']
        for param in required:
            if param not in kwargs:
                raise KeyError(f'Input {param} must be provided.')

#        return dict([(key, value[0]) for key, value in kwargs.items()])

    def _bbox_to_linestring(self, w, s, e, n, srid='4326'):
        return f"ST_Polygon('LINESTRING({w} {s}, {w} {n}, {e} {n}, {e} {s}, {w} {s})'::geometry, {srid})"

    def _generate_queries(self, kwargs):
        # Query the database and obtain data as Python objects
#        query = "SELECT * FROM lite.observations WHERE date_time < '1763-01-01';"
#        query = "SELECT * FROM lite.observations_1763_land_2 WHERE date_time < '1763-01-01';"
        d = {}
        d['domain'] = kwargs['domain']

        d['report_type'] = self._map_value(kwargs['frequency'],
                                wfs_mappings['frequency']['fields'])

        if 'bbox' in kwargs:
            bbox = [float(_) for _ in kwargs['bbox'].split(',')]
        else:
            bbox = (-1, 50, 10, 60)
        d['linestring'] = self._bbox_to_linestring(*bbox)

        d['observed_variable'] = self._map_value(kwargs['variable'],
                                     wfs_mappings['variable']['fields'],
                                     as_list=True)

        d['data_policy_licence'] = self._map_value(kwargs['intended_use'],
                                     wfs_mappings['intended_use']['fields'])

        d['quality_flag'] = self._map_value(kwargs['data_quality'],
                                     wfs_mappings['data_quality']['fields'])

        d['year'] = kwargs['year']
        d['month'] = kwargs['month']

        # NEED COLUMNS FOR SELECTION!!!!
        tmpl = ("SELECT * FROM lite.observations_{year}_{domain}_{report_type} WHERE "
                "observed_variable IN {observed_variable} AND "
                "data_policy_licence = {data_policy_licence} AND "
                "quality_flag = {quality_flag} AND "
                "ST_Intersects({linestring}, location) AND ")

        if kwargs['frequency'] == 'monthly':
            time_query = "date_trunc('month', date_time) = TIMESTAMP '{year}-{month}-01 00:00:00';"
            yield (tmpl + time_query).format(**d)

        elif kwargs['frequency'] == 'daily':
            for day in kwargs['day'].split(','):
                d['day'] = day
                time_query = "date_trunc('day', date_time) = TIMESTAMP '{year}-{month}-{day} 00:00:00';"
                yield (tmpl + time_query).format(**d)
                
        elif kwargs['frequency'] == 'sub-daily':
            for day in kwargs['day'].split(','):
                d['day'] = day
                for hour in kwargs['hour'].split(','):
                    d['hour'] = hour 
                    time_query = "date_trunc('hour', date_time) = TIMESTAMP '{year}-{month}-{day} {hour}:00:00';"
                    yield (tmpl + time_query).format(**d)


    def OLD(self):
#        to_map = ['intended_use', 'frequency', 'data_quality', 'variable']
        OLD_query = ("SELECT {columns} FROM lite.observations_{year}_{domain}_{report_type} WHERE " 
                "date_time BETWEEN '{year}-{month}-{day}' AND '{year}-{month}-28' AND " 
                "observed_variable IN {observed_variable} AND " 
                "data_policy_licence = {data_policy_licence} AND "
                "quality_flag = {quality_flag} AND "
                "ST_Intersects({linestring}, location);") #.format(**d)

#        log.warn(f'QUERY: {query}')
#        return query


    def _map_value(self, value, mapper, as_list=False):
        if ',' in value or as_list:
            return '(' + ','.join([mapper[_] for _ in value.split(',')]) + ')'
        else:
            return mapper[value] 
 

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

        if compress:

            content_type = "application/x-zip-compressed"
            response_file_name = f"{self.response_file_name}.zip"

            data = self._compress_data(data, include_code_tables=False)

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
        # E.G.: 'cql_filter': ['date_time DURING 1999-06-01T00:00:00Z/1999-06-01T23:59:59Z AND 
        #     observed_variable IN (44,85) AND report_type=3 AND platform_type NOT IN (2,5) AND 
        #     quality_flag=0 AND data_policy_licence=non_commercial']
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
