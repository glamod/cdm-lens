from django.http import QueryDict
from cdm_interface.sql_mngr import SQLManager


def test_get_time_condition_1():
    s = SQLManager()
    tc = s._get_time_condition(['2010'], ['01', '02'])

    expected = "date_trunc('month', date_time) in ('2010-01-01 00:00:00+00:00'::timestamptz," \
               " '2010-02-01 00:00:00+00:00'::timestamptz);"
    assert(tc == expected)

test_get_time_condition_1()

def test_sql_query_1():
    x = QueryDict('domain=land&frequency=sub_daily&variable=accumulated_precipitation,'
                  'air_temperature&intended_use=non_commercial&data_quality=quality_controlled'
                  '&column_selection=detailed_metadata&year=2019&month=03&day=25&hour=01&bbox='
                  '-1,50,10,59&compress=false')

    s = SQLManager()
    resp = s._generate_queries(x)

    expected = "SELECT * FROM lite.observations_2019_land_0 WHERE observed_variable IN (44,85) " \
               "AND data_policy_licence = 1 AND ST_Intersects(ST_MakeEnvelope(-1.0, 50.0, 10.0," \
               " 59.0, 4326), location::geometry) AND quality_flag = 0 AND date_trunc('hour', " \
               "date_time) in ('2019-03-25 01:00:00+00:00'::timestamptz);"
    assert(resp == expected)


test_sql_query_1()