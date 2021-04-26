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



def extract_csv_records(input_data, index_field=None, mappers=None):

    # Convert the CSV file to a DataFrame
    if type(input_data) is str:
        input_data = csv.DictReader(io.StringIO(input_data))

    data = []
    to_remove = ['FID', 'location']


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
