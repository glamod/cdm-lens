""" Utility functions for the cdm_interface app. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


import json
import requests

from django.conf import settings
from furl import furl
from pandas import DataFrame

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
