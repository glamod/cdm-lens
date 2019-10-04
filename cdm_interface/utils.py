""" Utility functions for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import requests
import io
import csv

from collections import namedtuple
from django.conf import settings
from furl import furl
from pandas import DataFrame


OutputFormat = namedtuple('OutputFormat', [
    'fetch_key', 'output_method', 'content_type', 'file_extension'
])
OUTPUT_FORMAT_MAP = {
    'csv': OutputFormat('csv', 'to_csv', 'text/csv', 'csv'),
    'json': OutputFormat('json', 'to_json', 'application/json', 'json'),
}


def fetch_data(layer_name, index_field=None, index=None, count=None, format_key='csv'):

    query = furl(settings.WFS_URL)

    query.args['request'] = 'GetFeature'
    query.args['typename'] = layer_name
    query.args['outputFormat'] = format_key

    if index is not None:
        query.args['cql_filter'] = f'{index_field}={index}'

    if count:
        query.args['count'] = count

    result = requests.get(query.url)

    if format_key == 'json':

        data = json.loads(result.text)
        data = DataFrame.from_records(data['features'], index='id')

    else:

        reader = csv.DictReader(io.StringIO(result.text))
        data = []
        for row in reader:

            # Remove FID column
            row.pop('FID')

            data.append(dict(row))

        data = DataFrame.from_records(data, index=index_field)

    return data


def get_output_format(format_name=None):

    if format_name not in OUTPUT_FORMAT_MAP:
        format_name = 'csv'

    return OUTPUT_FORMAT_MAP[format_name]


def format_data(data_frame, output_method='to_csv'):

    kwargs = {}

    if output_method == 'to_json':
        kwargs['orient'] = 'records'

    return getattr(data_frame, output_method)(**kwargs)
