"""
sql_mngr.py
===========

Manages construction of SQL query in SQLManager class.

Some example queries:

```
query = "SELECT * FROM lite.observations WHERE date_time < '1763-01-01';"
query = "SELECT * FROM lite.observations_1763_land_2 WHERE date_time < '1763-01-01';"
```

"""

import itertools
import datetime
import re

from cdm_interface.wfs_mappings import wfs_mappings
from cdm_interface.utils import decompose_datetime

import logging
logging.basicConfig()
log = logging.getLogger(__name__)


UTC = datetime.timezone.utc
SCHEMA = 'lite_2_0'


class SQLManager(object):

    tmpl = ("SELECT * FROM {SCHEMA}.observations_{year}_{domain}_{report_type} WHERE "
        "observed_variable IN {observed_variable} AND "
        "data_policy_licence IN {data_policy_licence} AND ")


    def _get_as_list(self, qdict, key):
        value = qdict.getlist(key)
        if len(value) == 1 and ',' in value[0]:
            return value[0].split(',')

        return value


    def _map_value(self, name, value, mapper, as_array=False):
        try: 
            if as_array:
                return '(' + ','.join([mapper[_] for _ in value]) + ')'
            else:
                return mapper[value] 
        except KeyError as exc:
            raise Exception(f'Cannot find value "{value}" in list of valid options for parameter: "{name}".')
 

    def _bbox_to_linestring(self, w, s, e, n, srid='4326'):
        # PREVIOUSLY:  return f"ST_Polygon('LINESTRING({w} {s}, {w} {n}, {e} {n}, {e} {s}, {w} {s})'::geometry, {srid})"
        return f"ST_MakeEnvelope({w}, {s}, {e}, {n}, {srid})"

    def _get_data_policy_licence(self, value):
        """Special treatment to map single value to list of values based on:
        - non_commercial --> '(0,1)'
        - open (i.e. for any/commercial use) --> '(0)'
        """
        if value == 'non_commercial':
            return '(0,1)'

        # Default is only open/commercial data
        return '(0)'

    def _generate_queries(self, qdict):

        tmpl = self.tmpl

        d = {'SCHEMA': SCHEMA}
        d['domain'] = qdict['domain']

        d['report_type'] = self._map_value('frequency', qdict['frequency'],
                                wfs_mappings['frequency']['fields'])

        if 'bbox' in qdict:
            bbox = [float(_) for _ in qdict['bbox'].split(',')]
            d['linestring'] = self._bbox_to_linestring(*bbox)
            tmpl += "ST_Intersects({linestring}, location::geometry) AND "

        d['observed_variable'] = self._map_value('variable', self._get_as_list(qdict, 'variable'),
                                     wfs_mappings['variable']['fields'],
                                     as_array=True)

#        d['data_policy_licence'] = self._map_value('intended_use', qdict['intended_use'],
#                                     wfs_mappings['intended_use']['fields'])
        d['data_policy_licence'] = self._get_data_policy_licence(qdict['intended_use'])

        if qdict.get('data_quality', None) == 'quality_controlled': 
            # Only include quality flag if set to QC'd data only
            d['quality_flag'] = '0'
            tmpl += "quality_flag = {quality_flag} AND "


        # If the request includes the "time" parameter then ignore other temporal parameters
        if qdict.get('time'):
            time_condition = self._get_time_range_condition(qdict['time'])
            # "year" is needed in template to match the partition
            d['year'] = qdict['time'][:4] 

        else:
            year = d['year'] = self._get_as_list(qdict, 'year')[0]
            months = self._get_as_list(qdict, 'month')
            months.sort()

            # Set defaults for days and hours
            days = None
            hours = None

            # Overwrite days and hours if relevant to the query
            if qdict['frequency'] in ('daily', 'sub_daily'):
                days = self._get_as_list(qdict, 'day')
                days.sort()

            if qdict['frequency'] == 'sub_daily':
                hours = self._get_as_list(qdict, 'hour')
                hours.sort()

            time_condition = self._get_time_condition([year], months, days, hours)

        return (tmpl + time_condition).format(**d)


    def _get_time_condition(self, years, months, days=None, hours=None):
        "date_trunc('month', date_time) = TIMESTAMP '{year}-{month}-01 00:00:00';"

        # Define period
        if days is None:
            period = 'month'
            time_iterators = [years, months, ['01'], ['00']]
        elif hours is None:
            period = 'day'
            time_iterators = [years, months, days, ['00']]
        else:
            period = 'hour'
            time_iterators = [years, months, days, hours]

        all_times = []

        for x in itertools.product(*time_iterators):
            # Use try/except to ignore any invalid time combinations
            try:
                all_times.append(datetime.datetime.strptime('{}-{}-{} {}'.format(*x), '%Y-%m-%d %H').astimezone(UTC))
            except Exception as err:
                pass

        # Check if any times found
        if not all_times:
            raise Exception('Could not generate any valid date/time values from the parameters provided.')

        time_condition = "date_trunc('{}', date_time) in ({});".format(period, 
                                ', '.join(["'{}'::timestamptz".format(x) for x in all_times])
                                )

        return time_condition


    def _get_time_range_condition(self, time_range):
        log.info(f'Parsing time range: "{time_range}"')

        if '/' not in time_range:
             raise Exception(f'Time range must be provided as "<start_time>/<end_time>", not "{time_range}".')

        start, end = time_range.split('/')
        start = decompose_datetime(start, 'start')
        end = decompose_datetime(end, 'end')

        if start.year != end.year:
            raise Exception('Time range selections must be a maximum of 1 year. Please modify your request.')

        start_time, end_time = [_.astimezone(UTC) for _ in (start, end)]

        time_condition = f"date_time BETWEEN '{start_time}'::timestamptz AND '{end_time}'::timestamptz;"
        return time_condition

        

