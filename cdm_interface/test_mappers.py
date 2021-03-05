from views import _get_mapper, _get_mappers

def test_get_mapper():
    column, code_table, index_field, desc_field = 'report_type', 'report_type', 'type', 'abbreviation'
    mapper = _get_mapper(code_table, index_field, desc_field)
    assert mapper[0] == 'SYNOP'

def test_get_mappers():
    mappers = _get_mappers()
    #import pdb; pdb.set_trace()
    assert dict(mappers)['report_type'][0] == 'SYNOP'

test_get_mapper()
test_get_mappers()