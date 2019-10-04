""" Utility functions for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import requests

from collections import namedtuple
from django.conf import settings
from furl import furl
from pandas import DataFrame


OutputFormat = namedtuple('OutputFormat', [
    'output_method', 'content_type', 'file_extension'
])
OUTPUT_FORMAT_MAP = {
    'csv': OutputFormat('to_csv', 'application/csv', 'csv'),
    'json': OutputFormat('to_json', 'application/json', 'json'),
}


def fetch_data(layer_name, index_field=None, index=None, count=None):
    
    query = furl(settings.WFS_URL)

    query.args['request'] = 'GetFeature'
    query.args['typename'] = layer_name
    query.args['outputFormat'] = 'json'

    if index is not None:
        query.args['cql_filter'] = f'{index_field}={index}'

    if count:
        query.args['count'] = count

    result = requests.get(query.url)
    data = json.loads(result.text)

    return DataFrame.from_records(data['features'], index='id')


def get_output_format(format_name=None):

    if format_name not in OUTPUT_FORMAT_MAP:
        format_name = 'csv'

    return OUTPUT_FORMAT_MAP[format_name]


def format_data(data_frame, output_method='to_csv'):

    kwargs = {}

    if output_method == 'to_json':
        kwargs['orient'] = 'records'

    return getattr(data_frame, output_method)(**kwargs)
