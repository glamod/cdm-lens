fpath = '/gws/nopw/j04/c3s311a_lot2/data/level2/land/r202005/station_configuration/station_configuration.psv'

with open(fpath) as reader:

 try:
   for _, i in enumerate(reader):
     pass

 except Exception as exc:
   print(f'Failed at: {_}')
