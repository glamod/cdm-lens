import os
import random


class OutputFileNamer:

    def __init__(self, req):
        self._hash = self._get_hash()
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
            mins = [min(req.get(item, 'X').split(',')) for item in ('year', 'month', 'day')]
            maxs = [max(req.get(item, 'X').split(',')) for item in ('year', 'month', 'day')]
            start_date = '-'.join(mins)
            end_date = '-'.join(maxs)
        else:
            start_date = 'start'
            end_date = 'end'

        self._base = f'surface-{domain}_{frequency}_{start_date}_{end_date}_{area}' 

    def get_csv_name(self):
        return f'{self._base}_csv-obs_{self._hash}.csv'

    def get_policy_name(self):
        return f'{self._base}_data-policy_{self._hash}.txt'

    def get_zip_name(self):
        return f'{self._base}_{self._hash}.zip'

    def _get_hash(self):
        return f'{os.getpid():05d}' + ''.join([str(random.randint(0, 9)) for _ in range(3)])



def test_file_namer():
    req = {'domain': 'land', 'frequency': 'monthly', 'time': '1999-01-01/2000-02-02', 'bbox': '0,0,0,0'}
    exp = 'surface-land_monthly_1999-01-01_2000-02-02_subset_csv-obs'

    file_namer = OutputFileNamer(req)
    csv = file_namer.get_csv_name()

    assert csv.startswith(exp)
    assert csv.endswith('.csv')

    req = {'domain': 'marine', 'frequency': 'sub_daily', 'year': '1999,2000', 'month': '01,02', 'day': '01,02'}
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
