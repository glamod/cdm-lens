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

            data = json.loads(raw_data)
            data = DataFrame.from_records(data['features'], index='id')

        else:

            reader = csv.DictReader(io.StringIO(raw_data))
            data = []
            for row in reader:

                # Remove FID column
                row.pop('FID')

                data.append(dict(row))

            data = DataFrame.from_records(data, index=self._index_field)

        if output_method:

            kwargs = {}
            if output_method == 'to_json':
                kwargs['orient'] = 'records'

            return getattr(data, output_method)(**kwargs)

        else:
            return data
