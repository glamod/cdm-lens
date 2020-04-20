""" Utility functions for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import requests
import io
import csv

from django.conf import settings
from furl import furl
from pandas import DataFrame

import logging
logging.basicConfig()
log = logging.getLogger(__name__)



class WFSQuery:

    def __init__(self, **kwargs):

        self._wfs_url = settings.WFS_URL
        self._query_dict = kwargs

    def fetch_data(self):

        query = furl(self._wfs_url)

        for key, value in self._query_dict.items():
            query.args[key] = value

        result = requests.get(query)
        return result.text


class LayerQuery(WFSQuery):

    def __init__(self, layer_name, index_field=None, index=None, count=None, data_format='csv'):

        self._index_field = index_field
        self._data_format = data_format

        query_dict = {
            "request": "GetFeature",
            "typename": layer_name,
            "outputFormat": data_format,
        }

        if index is not None:
            query_dict['cql_filter'] = f'{index_field}={index}'

        if count:
            query_dict['count'] = count

        super().__init__(**query_dict)


    def fetch_data(self, output_method=None):

        raw_data = super().fetch_data()

        if self._data_format == 'json':

            # Convert the returned data into a DataFrame
            data = json.loads(raw_data)
            data = extract_json_records(data)
#            data = DataFrame.from_records(data['features'], index='id')
            data = DataFrame.from_records(data)

        else:

            data = extract_csv_records(raw_data, index_field=self._index_field)
        # Write to CSV
#        data.to_csv('/tmp/output.csv')

        if output_method:

            kwargs = {}
            if output_method == 'to_json':

                # Convert the data from to JSON
                kwargs['orient'] = 'records'

            return getattr(data, output_method)(**kwargs)

        else:
            return data


def extract_json_records(data):
#    with open('/tmp/in.json', 'w', encoding='utf-8') as writer:
#        writer.write(f'TYPE: {type(data)}\n\n{data}')

    # Extracts real records from complex data structure
    if type(data) is str: 
        data = json.loads(data)

    if type(data) is dict and 'features' in data:
        data = data['features']

    if type(data) is list and len(data) > 0 and 'properties' in data[0]:
        data = [_['properties'] for _ in data]

    return data


def extract_csv_records(input_data, index_field=None, mappers=None):

    # Convert the CSV file to a DataFrame
    if type(input_data) is str:
        input_data = csv.DictReader(io.StringIO(input_data))

    data = []
    FID = 'FID'

    log.warn(f'mappers: {mappers}')

    for row in input_data:
        # Remove FID column
        if FID in row: 
            row.pop(FID)

#        log.warn(f'Row: {row}')
        if mappers:
            for field, mapper in mappers:
                if field in row:
                    #log.warn(f'Row field: {row[field]}')
                    row[field] = mapper.get(row[field], '')

        data.append(dict(row))

    data = DataFrame.from_records(data, index=index_field)
    return data

