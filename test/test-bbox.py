import requests
import pandas as pd
import io
import math


TMPL = 'http://glamod2.ceda.ac.uk/select/?domain=land&frequency=monthly&variable=accumulated_precipitation,air_temperature&intended_use=non-commercial&data_quality=quality_controlled&column_selection=detailed_metadata&year=1974&month=03&bbox={w}.0,{s}.0,{e}.0,{n}.0&compress=false'


def _assert_in_range(df, w, s, e, n, to_nearest_degree=False):
   
    if len(df) == 0:
        print('Empty df')
        return

    lats, lons = df.latitude, df.longitude
    min_lat, max_lat = lats.min(), lats.max()
    min_lon, max_lon = lons.min(), lons.max()

    print(f'Wanted lons: {w} to {e};      lats: {s} to {n}')
    print(f'Actual lons: {min_lon} to {max_lon}; lats: {min_lat} to {max_lat}')

    def fix(n):
        if n < 0: 
            return math.ceil(n)
        else:
            return math.floor(n)

    if to_nearest_degree:
        min_lat, max_lat, min_lon, max_lon = [fix(_) for _ in [min_lat, max_lat, min_lon, max_lon]]

#    print(lats, lats.max(), lats.min())

    assert(min_lat >= s), 'min_lat >= s'
    assert(max_lat <= n), 'max_lat <= n'

    if min_lat == max_lat and min_lat == -90 or min_lat == 90:
        print('Longitude checks are meaningless at the north/south pole')
        return

    if 90 in list(lats) or -90 in list(lats):
        print('Some lats are north/south pole - so ignore longitude checks')

    assert(min_lon >= w), 'min_lon >= w'
    assert(max_lon <= e), 'max_lon <= e'


def _fetch_as_df(w, s, e, n):
    url = TMPL.format(**vars())
    print(f'{url}')

    content = requests.get(url).text

    if content.startswith('Exception raised'):
        print(f'[ERROR] Fetch error: {content}')
        return content

    return pd.read_csv(io.StringIO(content)) 


def test_bbox_in_range():

    for w in range(-180, 160, 30):
        e = w + 30

        for s in range(-90, 61, 30):
            n = s + 30

            df = _fetch_as_df(w, s, e, n)
            _assert_in_range(df, w, s, e, n, True)


def test_bbox_full_range():
    bboxes = ['-180,-90,180,90']  #, '-90,90,-180,180', '-90,-180,90,180']

    for bbox in bboxes:
        w, s, e, n = [int(_) for _ in bbox.split(',')]

        df = _fetch_as_df(w, s, e, n)

        if type(df) == str:
            continue

        _assert_in_range(df, w, s, e, n, True) 
        


if __name__ == '__main__':

    test_bbox_full_range()    
    test_bbox_in_range()
