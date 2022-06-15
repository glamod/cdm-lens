from utils import decompose_datetime
import datetime
UTC = datetime.timezone.utc


def n(time_range):

#        import pdb; pdb.set_trace()
        if '/' not in time_range:
             raise Exception(f'Time range must be provided as "<start_time>/<end_time>", not "{time_range}".')

        start, end = time_range.split('/')
        start = decompose_datetime(start, 'start')
        end = decompose_datetime(end, 'end')

        if start.year != end.year:
            raise Exception('Time range selections must be a maximum of 1 year. Please modify your request.')

        start_time, end_time = [_.astimezone(UTC) for _ in (start, end)]
        start_time2, end_time2 = [f'{_}+00:00' for _ in (start, end)]

        print( str(start_time), start_time2)
        assert str(start_time) == start_time2
        assert str(end_time) == end_time2
        print(start_time)
        time_condition = f"date_time BETWEEN '{start_time}'::timestamptz AND '{end_time}'::timestamptz;"
        return time_condition


n('1900-01-01/1900-02-01')

n('1900-01-01T12:23:31/1900-02-01T14')
