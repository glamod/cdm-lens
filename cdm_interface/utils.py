""" Utility functions for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import requests
import io
import csv
import re
import datetime

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
    to_remove = ['FID', 'location']

#    log.warn(f'mappers: {mappers}')

    for row in input_data:
        # Remove FID column

        for field in to_remove:
            if field in row: 
                row.pop(field)

#        log.warn(f'Row: {row}')
        if mappers:
            for field, mapper in mappers:
                if field in row:
                    #log.warn(f'Row field: {row[field]}')
                    row[field] = mapper.get(row[field], '')

        data.append(dict(row))

    data = DataFrame.from_records(data, index=index_field)
    return data


def decompose_datetime(dt, day_limit):
    match = re.match('^(\d{4})-(\d{2})-(\d{2})T?(\d{2})?:?(\d{2})?:?(\d{2})?$', dt)
    err_msg = f'Could not parse date/time from: "{dt}".'

    if not match: 
        raise Exception(err_msg)

    grps = list([_ for _ in match.groups() if _ != None])

    if day_limit == 'start':
        hms = ['00', '00', '00']
    elif day_limit == 'end':
        hms = ['23', '59', '59']
    else:
        raise Exception('"day_limit" parameter must be "start" or "end"')

    comps = grps + hms[:6 - len(grps)]

    try:
        resp = datetime.datetime(*[int(_) for _ in comps])
    except Exception as exc:
        raise Exception(err_msg)

    return resp


def test_decompose_datetime():
    dts_good = ('1999-01-01', '1999-01-01T00', '1999-01-01T00:00', '1999-01-01T00:00:00',
                '1600-01-01', '1755-01-01')
    dts_bad = ('19999', '1199-111', '1999-01-', '1999-01-01 00', '1999-01-01T0')

    for dt in dts_good:
        print('Succeeded:', dt, decompose_datetime(dt, 'start'))

    for dt in dts_bad:
        try:
            decompose_datetime(dt, 'start')
        except Exception as exc:
            print(f'Correct - failed for: {dt}')


test_all = False

if test_all:
    test_decompose_datetime()
