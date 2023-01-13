import os
import random


from cdm_interface.data_versions import validate_data_version


class OutputFileNamer:

    def __init__(self, data_version, req):
        data_version = validate_data_version(data_version)
        self._suffix = self._get_suffix(data_version)
        self._build(req)

    def _build(self, req):
        
        domain = req.get('domain', 'land')
        frequency = req.get('frequency').replace('_', '-') 

        if req.get('bbox', None):
            area = 'subset'
        else:
            area = 'global'
       
        if 'time' in req:
            start_date, end_date = [_.split('T')[0] for _ in req.get('time').split('/')]
        elif 'year' in req:
            years, months, days = [self._parse_list(req, key, ['X']) for key in ('year', 'month', 'day')]

            mins = [min(item) for item in (years, months, days)]
            maxs = [max(item) for item in (years, months, days)]

            start_date = '-'.join(mins)
            end_date = '-'.join(maxs)
        else:
            start_date = 'start'
            end_date = 'end'

        self._base = f'surface-{domain}_{frequency}_{start_date}_{end_date}_{area}' 

    def _parse_list(self, req, key, default=None):
        "Parses both: x=1&x=2 and x=1,2 params in query string."
        items = req.getlist(key, [])
        resp = set()

        for item in items:
            for _ in item.split(','):
                resp.add(_)

        if not resp: return default or []
        return sorted(list(resp)) 

    def get_csv_name(self):
        return f'{self._base}_csv-obs_{self._suffix}.csv'

    def get_policy_name(self):
        return f'{self._base}_data-policy_{self._suffix}.txt'

    def get_zip_name(self):
        return f'{self._base}_{self._suffix}.zip'

    def _get_suffix(self, data_version):
        return ''.join([str(random.randint(0, 9)) for _ in range(8)]) + "_" + data_version



def test_file_namer():

    print('Run from correct location: top of cdm_lens')
    import os
    os.environ['DJANGO_SETTINGS_MODULE'] = 'cdm_lens_site.settings'
    import django
    django.setup()

    from django.http import QueryDict

    req = {'domain': 'land', 'frequency': 'monthly', 'time': '1999-01-01/2000-02-02', 'bbox': '0,0,0,0'}
    req = QueryDict('domain=land&frequency=monthly&time=1999-01-01/2000-02-02&bbox=0,0,0,0')
    exp = 'surface-land_monthly_1999-01-01_2000-02-02_subset_csv-obs'

    file_namer = OutputFileNamer(req)
    csv = file_namer.get_csv_name()

    assert csv.startswith(exp)
    assert csv.endswith('.csv')

    req = {'domain': 'marine', 'frequency': 'sub_daily', 'year': '1999,2000', 'month': '01,02', 'day': '01,02'}
    req = QueryDict('domain=marine&frequency=sub_daily&year=1999,2000&month=01,02&day=01,02')
    exp = 'surface-marine_sub-daily_1999-01-01_2000-02-02_global'

    file_namer = OutputFileNamer(req)
    txt = file_namer.get_policy_name()
    zipf = file_namer.get_zip_name()

    assert txt.startswith(exp + '_data-policy_')
    assert txt.endswith('.txt')
    assert zipf.startswith(exp)
    assert zipf.endswith('.zip')

    req = QueryDict('domain=marine&frequency=sub_daily&year=1999,2000&month=01,02&day=01&day=02')
    exp = 'surface-marine_sub-daily_1999-01-01_2000-02-02_global'

    file_namer = OutputFileNamer(req)
    txt = file_namer.get_policy_name()
    zipf = file_namer.get_zip_name()

    assert txt.startswith(exp + '_data-policy_')
    assert txt.endswith('.txt')
    assert zipf.startswith(exp)
    assert zipf.endswith('.zip')



if __name__ == '__main__':

    test_file_namer()
