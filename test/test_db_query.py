import os

import pandas as pd
import psycopg2


SCHEMA = 'lite_2_0'

pw = os.environ.get('PGPW_READER')
if not pw:
    raise Exception('No password!')

# Make connection
conn_str = f'dbname=cdm user=glamod_read host=localhost password={pw}'
conn = psycopg2.connect(conn_str)

# Open a cursor to perform database operations
cur = conn.cursor()

# Query the database and obtain data as Python objects
query = f"SELECT * FROM {SCHEMA}.observations WHERE date_time < '1763-01-01';"
query2 = f"SELECT * FROM {SCHEMA}.observations_1763_land_2 WHERE date_time < '1763-01-01';"

# select ST_AsText(location) from lite.observations_2011_land_2 where date_time > '2011-11-01' AND
# observed_variable = 85 AND
# ST_Intersects(ST_Polygon('LINESTRING(50 10, 60 10, 60 26, 50 26, 50 10)'::geometry, 4326), location);
# bbox: lon lat, lon lat, ...

def _bbox_to_linestring(w, s, e, n, srid='4326'):
    return f"ST_Polygon('LINESTRING({w} {s}, {w} {n}, {e} {n}, {e} {s}, {w} {s})'::geometry, {srid})"


bbox = (-1, 50, 10, 60)
ls = _bbox_to_linestring(*bbox)

fields = 'ST_AsText(location)'
query3 = f"SELECT {fields} FROM {SCHEMA}.observations_2011_land_2 WHERE date_time > '2011-11-01' AND ST_Intersects({ls}, location);" 
print(query3)
cur.execute(query3)
columns = [_.name for _ in cur.description]

fields = '*'
query4 = f"SELECT {fields} FROM {SCHEMA}.observations_2011_land_2 WHERE date_time > '2011-11-01' AND ST_Intersects({ls}, location);"

print(columns)
print(cur.fetchone())
#(1, 100, "abc'def")

print("""
pd.read_sql(sql, con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
""")
print(conn_str)

df = pd.read_sql(query4, conn)
df.drop(columns=['location'])
print(df.loc[0])

df.to_csv('out.csv', sep=',', index=False, float_format='%.3f',
          date_format='%Y-%m-%d %H:%M:%S%z')

print('Latitude selected:')
print(df.latitude.min(), df.latitude.max())

print('Longitude selected:')
print(df.longitude.min(), df.longitude.max())

print(f'BBOX: {bbox}')
