""" Views for the cdm_interface app. """

__author__ = "Ag Stephens"
__date__ = "2020-08-01"
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import re
import json
import zipfile
import os
import io
import random
import time
import uuid
from dateutil import parser

import pandas as pd
import psycopg2

from io import BytesIO
from collections import namedtuple
from django.views.generic import View
from django.http import HttpResponse
from django.conf import settings

#from cdm_interface.utils import LayerQuery, WFSQuery, extract_json_records,
from cdm_interface.utils import extract_csv_records
from cdm_interface.sql_mngr import SQLManager
from cdm_interface.data_policies import get_data_policies
from cdm_interface.file_namer import OutputFileNamer
from cdm_interface.data_versions import validate_data_version

import logging
logging.basicConfig()
log = logging.getLogger(__name__)


def log_time(msg):
    now = time.time()
    log.warn(f'[TIMER] | {msg} | {now:.3f}')


class SelectView(View):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #self._output_format = None
        self._set_reqid()

    def _set_reqid(self):
        self._reqid = uuid.uuid4() 

#    @property
#    def output_format(self):
#        return "csv"

    def get(self, request, data_version):

     
        log.warn(f'QUERY STRING: {request.GET}')
        log.warn(f'Query string: {self.request.GET.urlencode()}')

        data_version = validate_data_version(data_version)

        log_time(f'{self._reqid}::RECEIVED_QUERY')
        try:
            qm = QueryManager(data_version, self._reqid)
            data, data_policy_text = qm.run_query(request.GET)
        except Exception as exc:

            log.warn(f'[ERROR] Failed with exception: {exc}')

#            with open('/tmp/failures.txt', 'a') as writer:
#                writer.write(str(request.GET) + '\n' + str(exc) + '\n\n')

            return HttpResponse(f'Exception raised when running query: {str(exc)}', status=400)

        log.warn(f'LENGTH: {len(data)}')
        compress = json.loads(request.GET.get("compress", "true"))
        return self._build_response(request, data, data_version, data_policy_text, compress=compress)

    def _build_response(self, request, data, data_version, data_policy_text='', compress=True):
        log_time(f'{self._reqid}::START_BUILD_RESPONSE')

        data = data.to_csv(index=False)

        # Check valid combination of arguments
        if data_policy_text and not compress:
            return HttpResponse('Cannot return a data policy info to uncompressed response.')

        file_namer = OutputFileNamer(data_version, request.GET)
        zip_name = file_namer.get_zip_name()

        if compress: 
            content_type = "application/x-zip-compressed"
            response_file_name = zip_name
 
            data_policy_file = (file_namer.get_policy_name(), data_policy_text)
            csv_file = (file_namer.get_csv_name(), data) 
            zipped_bytes = self._get_zipped_response((csv_file, data_policy_file))
        else:
            content_type = "text/csv"
            response_file_name = file_namer.get_csv_name()

        response = HttpResponse(zipped_bytes, content_type=content_type)
        content_disposition = f'attachment; filename="{zip_name}"'
        response["Content-Disposition"] = content_disposition

        log_time(f'{self._reqid}::END_BUILD_RESPONSE')

        return response

    def _get_zipped_response(self, file_pairs):

        file_like_object = BytesIO()
        zipfile_ob = zipfile.ZipFile(file_like_object, "w", compression=zipfile.ZIP_DEFLATED)

#        zipfile_ob.writestr(f"{self.response_file_name}.{self.output_format}", data)

        for fname, content in file_pairs:
            zipfile_ob.writestr(fname, content)

        zipfile_ob.close()
        
        return file_like_object.getvalue()



# noinspection SqlDialectInspection
class QueryManager(object):

    def __init__(self, data_version, reqid, conn_str=settings.LOCAL_CONN_STR):
        self._data_version = data_version
        self._reqid = reqid
        self._conn = psycopg2.connect(conn_str)

    def _get_data_policy_text(self, results):
        """
        Uses data policy manager to decide on the data policy info to provide.
        Returns the text content for the data policy file that should be
        returned alongside the data file(s).

        The input is a pandas DataFrame of results, whilst it still includes
        the `source_id`, which is needed for this processing.

        Full details of this process are listed at: 
            https://github.com/glamod/glamod-ingest/issues/13
        """
        return get_data_policies(results, rendered=True)

    def run_query(self, kwargs):
        "Returns tuple of: (results_data_frame, data_policy_text)"
        log.warn(f'kwargs: {kwargs}')
        self._validate_request(kwargs)        

        dfs = []

        sql_manager = SQLManager(self._data_version)

        log_time(f'{self._reqid}::START_SQL')
 
        for sql_query in [sql_manager._generate_queries(kwargs)]:
            log.warn(f'RUNNING SQL: {sql_query}')

            try:
                df = pd.read_sql(sql_query, self._conn)
                log.warn(f'SUCCESS: Extracted a DataFrame of length: {len(df)}')
            except Exception:
                log.warn(f'FAILED: Error when extracting data! - query: {sql_query}')
                continue

            dfs.append(df)

        log_time(f'{self._reqid}::END_SQL')


        all_columns = ['observation_id', 'data_policy_licence', 'date_time', 'date_time_meaning', 
                       'observation_duration', 'longitude', 'latitude', 'report_type', 
                       'height_above_surface', 'observed_variable', 'units', 'observation_value', 
                       'value_significance', 'platform_type', 'station_type', 'primary_station_id', 
                       'station_name', 'quality_flag', 'source_id', 'location']

        basic_metadata_columns = ['observation_id', 'date_time', 'observation_duration', 'longitude',
                                  'latitude', 'height_above_surface', 'observed_variable', 'units',
                                  'observation_value', 'value_significance', 'primary_station_id',
                                  'station_name', 'quality_flag', 'source_id']

        log_time(f'{self._reqid}::START_MODIFY_DATAFRAMES')

        if not dfs:
            # If no data has been found (or no valid tables for date range)
            # Create empty DataFrame with required headers
            dfs = [pd.DataFrame(columns=all_columns)]
            
        # If only one data frame return it, otherwise concatenate them into one
        if len(dfs) == 1:
            df = dfs[0]
        else:
            df = pd.concat(dfs)

        # Filter the columns if only basic metadata requested
        if kwargs.get('column_selection', None) == 'basic_metadata':
            df = df[basic_metadata_columns]

        else:
            # Only drop "location" column extended metadata required
            df = df.drop(columns=['location'])

        log_time(f'{self._reqid}::END_MODIFY_DATAFRAMES')

        # Get data policy text
        log_time(f'{self._reqid}::START_DATA_POLICY')
        data_policy_text = self._get_data_policy_text(df)
        log_time(f'{self._reqid}::END_DATA_POLICY')


        # If source_id exists then drop it before returning
#        source_id = 'source_id'
#        if source_id in df: 
#            df.drop(columns=[source_id], inplace=True)
 
        log_time(f'{self._reqid}::START_MAP_VALUES')
        self._map_values(df)
        log_time(f'{self._reqid}::END_MAP_VALUES')

        # df.to_csv('out.csv', sep=',', index=False, float_format='%.3f',
        #           date_format='%Y-%m-%d %H:%M:%S%z')
        return df, data_policy_text

    def _map_values(self, df):

#        df_test = df.copy(deep=True)
        mappers = _get_mappers()
        df.replace(dict(mappers), inplace=True)

#        found_cols = [_ for _ in df.columns]
#        df_test.replace(dict(mappers), inplace=True)

#        log.warn('4')

#        for col, mapper in mappers:
#            if col in found_cols: 
#                log.warn(f'Mapping: {col}, with {mapper}')
#                df[col].replace(mapper, inplace=True)


    def _validate_request(self, kwargs):
        required = ['domain', 'frequency', 'variable']

        for param in required:
            if param not in kwargs:
                msg = f'Input parameter "{param}" must be provided.'
                log.warn(msg)
                raise KeyError(msg)

        # Check either 'time' or 'year' provided
        if 'time' not in kwargs and ('year' not in kwargs or 'month' not in kwargs):
            raise Exception('Either "time" or time compoonents must be provided.')

        allowed_domains = ('marine', 'land')
        if kwargs['domain'] not in allowed_domains:
            raise Exception(f'"domain" must be one of: {allowed_domains}')

        allowed_frequencies = ('monthly', 'daily', 'sub_daily')
        if kwargs['frequency'] not in allowed_frequencies:
            raise Exception(f'Incorrect value for "frequency". Must be one of: {allowed_frequencies}.')


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

        log.warning(f'Query string: {self.request.GET.urlencode()}')

        compress = json.loads(
            self.request.GET.get("compress", self.DEFAULT_COMPRESS_VALUE))

        mappers = _get_mappers()

        # # Manage the data to remove excess WFS padding in JSON/CSV
        # if self.output_format.extension == 'json':
        #     data = extract_json_records(data)
        #     data = json.dumps(data)

        if self.output_format.extension == 'csv':
            data = extract_csv_records(data, mappers=mappers)
            data = data.to_csv(index=False)

        if compress:

            content_type = "application/x-zip-compressed"
            response_file_name = f"{self.response_file_name}.zip"

            data = self._get_zipped_response(data, include_code_tables=False)

        else:

            content_type = self.output_format.content_type
            response_file_name = \
                f"{self.response_file_name}.{self.output_format.extension}"

        response = HttpResponse(data, content_type=content_type)
        content_disposition = f"attachment; filename=\"{response_file_name}\""
        response["Content-Disposition"] = content_disposition

        return response

    def _get_zipped_response(self, data, include_code_tables=False):

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


# Structure: column_name: (code_table, index, description)
mapper_data = {
    'report_type': ['report_type', 'type', 'abbreviation'],
    'date_time_meaning': ['meaning_of_time_stamp', 'meaning', 'name'],
    'observed_variable': ['observed_variable', 'variable', 'name'],
    'units': ['units', 'units', 'abbreviation'],
    'value_significance': ['observation_value_significance', 'significance', 'description'], 
    'observation_duration': ['duration', 'duration', 'description'],
    'platform_type': ['platform_type', 'type', 'description'],
    'station_type': ['station_type', 'type', 'description'],
    'quality_flag': ['quality_flag', 'flag', 'description'],
    'data_policy_licence': ['data_policy_licence', 'policy', 'name']
}


def _insert_underscores(s):
    return s.replace(' ', '_')


def _get_mappers():
    mappers = []

    for column, (code_table, index_field, desc_field) in mapper_data.items(): 

        processor = None
        if column == 'observed_variable':
            processor = _insert_underscores  
 
        mapper = _get_mapper(code_table, index_field, desc_field, processor=processor)
        mappers.append((column, mapper))

    return mappers
 

def _get_mapper(code_table, index_field, desc_field, processor=None, conn=None):

    # Download a code table and convert it to, and return a dictionary
    # query = LayerQuery(code_table, index_field=index_field, data_format='csv')
    # code_table_data = query.fetch_data('to_csv')
    # df = pd.read_csv(io.StringIO(code_table_data))

    # Set up psycopg2 connection
    if not conn:
        conn_str = settings.LOCAL_CONN_STR
        conn = psycopg2.connect(conn_str)

    sql_query = f"SELECT * FROM {code_table};"
    df = pd.read_sql(sql_query, conn)

    mapper = {}

    for i, rec in df.iterrows():
        value = rec[desc_field]

        if processor: 
            value = processor(value)

        mapper[int(rec[index_field])] = value

    return mapper



# class RawWFSView(QueryView):

#     def get(self, request):

#         log.warn(f'QUERY STRING: {request.GET}')

#         rg = request.GET.copy()
#         log.warn(f'COPIED QUERY STRING: {rg}')

#         if rg.get('cql_filter'):
#             rg['cql_filter'] = self._map_cql_filter(rg['cql_filter'])

#         log.warn(f'FIXED QUERY STRING: {rg}')

#         query = WFSQuery(**rg)
#         data = query.fetch_data()

#         return self._build_response(data)


#     def _map_cql_filter(self, cql): 
#         # NOTE: domain is MANUALLY mapped in strings because simpler than integrating into wfs_mappings
#         #
#         # E.G.: 'cql_filter': ['date_time DURING 1999-06-01T00:00:00Z/1999-06-01T23:59:59Z AND 
#         #     observed_variable IN (44,85) AND report_type=3 AND platform_type NOT IN (2,5) AND 
#         #     quality_flag=0 AND data_policy_licence=non_commercial']
#         to_map = wfs_mappings.keys()
#         to_map = ['intended_use', 'frequency', 'data_quality', 'variable']
#         cql=cql + ' '
# #        comps = cql.split()
        
#         r0 = '^(?P<start>.*)'
#         r1 = '(?P<end>.*)$'

#         for key in to_map:

#             mapper = wfs_mappings[key]
#             target_field = mapper['target']

#             res = []
            
# #            re.match('^(?P<start>.*)(?P<found>x in y)(?P<end>.*)$', 'hello x in y').groupdict()
#             regex = f'{r0}(?P<found>{key}=)(?P<value>[^ ]+)\s*{r1}'
#             log.warn(f'regex: {regex}')
#             m1 = re.match(regex, cql)

#             if m1:
#                 d = m1.groupdict()
#                 mapped_value = mapper['fields'][d['value']]
#                 value = f'{target_field}={mapped_value}'
#                 cql = d['start'] + value + ' ' + d['end'].strip()
#                 log.warn(f'UPDATE TO cql: {cql}')

#             regex = f'{r0}(?P<found>{key} IN )(?P<value>[^ ]+)\s*{r1}'
#             log.warn(f'regex: {regex}')
#             m1 = re.match(regex, cql)

#             if m1:
#                 d = m1.groupdict()
#                 items = d['value'].split('(')[1][:-1].split(',')
#                 mapped_value = ','.join([mapper['fields'][_] for _ in items])
#                 value = f'{target_field} IN ({mapped_value})'
#                 cql = d['start'] + value + ' ' + d['end'].strip()
#                 log.warn(f'UPDATE TO cql: {cql}')

#         regex = f'{r0}(?P<found>domain=)(?P<value>[^ ]+)\s*{r1}'
#         log.warn(f'regex: {regex}')
#         m1 = re.match(regex, cql)

#         if m1:
#             d = m1.groupdict()
#             if d['value'] == 'land':
#                 mapped_value = 'platform_type NOT IN (2,5)'
#             elif d['value'] == 'marine':
#                 mapped_value = 'platform_type IN (2,5)'
                
#             value = mapped_value
#             cql = d['start'] + value + ' ' + d['end'].strip()
#             log.warn(f'UPDATE TO cql: {cql}')

 
#         for comp in []: #comps:
#                 value = comp

#                 if value.startswith(f'{key}='):
#                     mapped_value = mapper['fields'][value.split('=')[1]]
#                     log.warn(f'{value} --> {target_field}={mapped_value}')
#                     res.append(f'{target_field}={mapped_value}')
#                 elif value.startswith(f'{key} IN ('):
#                     items = value.split('(')[1][:-1].split(',')
#                     mapped_value = ','.join([mapper['fields'][_] for _ in items])
#                     res.append(f'{target_field}={mapped_value}')
#                 elif value == 'domain=land':
#                     res.append('platform_type NOT IN (2,5)')
#                 elif value == 'domain=marine':
#                     res.append('platform_type IN (2,5)') 
#                 else:
#                     res.append(value)

# #            comps = res[:]

#         return cql.strip()


class ConstraintsView(View):

    def get(self, request, data_version, domain):

        log.warn(f'Requested constraints for: {domain}')
        domain = domain.lower()

        data_version = validate_data_version(data_version) 

        if domain not in ('land', 'marine'):
            return HttpResponse('Domain must be one of: "land", "marine".', status=400)

        content_type = "application/json"
        response_file_path = self._get_constraints_path(domain, data_version)
        response_file_name = os.path.basename(response_file_path)

        response = HttpResponse(open(response_file_path).read(), content_type=content_type)
        content_disposition = f"attachment; filename=\"{response_file_name}\""
        response["Content-Disposition"] = content_disposition

        return response

    def _get_constraints_path(self, domain, data_version):
        return f'{settings.STATIC_ROOT}/constraints/constraints-{domain}-{data_version}.json'


# class LayerView(QueryView):

#     DEFAULT_OUTPUT_FORMAT = "csv"
#     DEFAULT_COMPRESS_VALUE = "false"

#     layer_name = None
#     index_field = None

#     @property
#     def response_file_name(self):
#         return self.layer_name

#     def get(self, request, index=None):

#         query = LayerQuery(
#             self.layer_name,
#             index_field=self.index_field,
#             index=index,
#             count=self.request.GET.get("count"),
#             data_format=self.output_format.fetch_key
#         )
#         data = query.fetch_data(
#             self.output_format.output_method)

#         return self._build_response(data)


# class LiteRecordView(LayerView):
#     layer_name = "observations"
#     index_field = "observation_id"


# class ReportTypeView(LayerView):
#     layer_name = "report_type"
#     index_field = "type"


# class MeaningOfTimeStampView(LayerView):
#     layer_name = "meaning_of_time_stamp"
#     index_field = "meaning"


# class ObservedVariableView(LayerView):
#     layer_name = "observed_variable"
#     index_field = "variable"

# class UnitsView(LayerView):
#     layer_name = "units"
#     index_field = "units"


# class ObservationValueSignificanceView(LayerView):
#     layer_name = "observation_value_significance"
#     index_field = "significance"


# class DurationView(LayerView):
#     layer_name = "duration"
#     index_field = "duration"


# class PlatformTypeView(LayerView):
#     layer_name = "platform_type"
#     index_field = "type"


# class StationTypeView(LayerView):
#     layer_name = "station_type"
#     index_field = "type"


# class QualityFlagView(LayerView):
#     layer_name = "quality_flag"
#     index_field = "flag"


# class DataPolicyLicenceView(LayerView):
#     layer_name = "data_policy_licence"
#     index_field = "policy"
