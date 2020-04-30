import calendar

# Rules on "wfs_mappings"
# For each form field:
# - if no "indent", then default is 3 x tab (or 4 spaces)
# - if no "target", then "target" == field key
# - if no "labels" then "labels" matches "fields" keys
# - if "labels" == "__standard__": then "labels" are .replace('_', ' ').capitalize() of fields

wfs_mappings = {
    "frequency": {
        "target": "report_type",
        "fields": {
            "monthly": "2",
            "daily": "3",
            "sub-daily": "0"
        }, 
        "labels": '__standard__'
    },
    "variable": {
        "target": "observed_variable",
        "fields": {
            'accumulated_precipitation': "44",
            'air_pressure': "57",
            'air_pressure_at_sea_level': "58",
            'air_temperature': "85",
            'dew_point_temperature': "36",
            'fresh_snow': "45",
            'snow_depth': "53",
            'snow_water_equivalent': "55",
            'wind_from_direction': "106",
            'wind_speed': "107"
        },
        "labels": '__standard__'
    },
    "intended_use": {
        "target": "data_policy_licence",
        "fields": {
            "non-commercial": "1",
            "commercial": "0"
        },
        "labels": "__standard__"
    },
    "data_quality": {
        "target": "quality_flag",
        "fields": {
            "quality_controlled": "0",
            "all_data": "1"
        },
        "labels": '__standard__'
    },
    "column_selection": {
        "fields": {
            "basic_metadata": "basic_metadata",
            "detailed_metadata": "detailed_metadata"
        },
        "labels": '__standard__'
    },
    "year": {
        "fields": {
            "__auto__": [str(_) for _ in range(1763, 2019)]
        }
    },
    "month": {
        "fields": {
            "__auto__": [calendar.month_name[i] for i in range(1, 13)]
        }
    },
    "day": {
        "fields": {
            "__auto__": [f'{_:02d}' for _ in range(1, 32)]
        }
    },
    "hour": {
        "fields": {
            "__auto__": [f'{_:02d}' for _ in range(0, 24)]
        }
    },
    "format": {
        "fields": {
            "csv_zipped": "csv"
        },
        "labels": {
            "csv_zipped": "CSV (Zipped)"
        }
    }
}

EXPANDED = False


def _auto_generate(items):
    d = dict([(_, _) for _ in items])
    return d


def _expand_mappings(mappings):

    global EXPANDED
    if EXPANDED: return

    for key, value in mappings.items():

        if isinstance(value['fields'], list):
            value['fields'] = dict([(_, _) for _ in value['fields']])

        if list(value['fields'].keys()) == ['__auto__']:
            value['fields'] = _auto_generate(value['fields'].popitem()[1])

        if not value.get('labels', None):
            value['labels'] = dict([(_, _) for _ in value['fields'].keys()])
        elif value['labels'] == '__standard__':
            value['labels'] = dict([(_, _.replace('_', ' ').capitalize()) for _ in value['fields'].keys()])

        value['values'] = [_ for _ in value['fields'].keys()]

        if not value.get('indent', None):
            value['indent'] = 3

        if not value.get('target', None):
            value['target'] = key

    EXPANDED = True


_expand_mappings(wfs_mappings)


def _obj_as_str(obj, indent):
    s = str(obj)
    mid = (',\n' + indent + '   ').join([_ for _ in s[1:-1].split(',')]).replace("'", '"')
    return s[0] + '\n' + indent + '    ' + mid + '\n' + indent + s[-1]


def _insert_content(key, field_name, content, indent, tmpl):
    indent = '    ' * indent
    c = f'{indent}"{field_name}": ' + _obj_as_str(content, indent)
#    print('INDENT', indent, content)
    return tmpl.replace(key, c)


def write_form(fpath, tmpl, mappings):
    mappers = {}
    for key, mapper in mappings.items():
        indent = mapper['indent']

        for field_name, content in mapper.items():
            if field_name == 'indent': continue

            replace_key = f'__{key}__.__{field_name}__'
            tmpl = _insert_content(replace_key, field_name, content, indent, tmpl)

    with open(fpath, 'w') as writer:
        writer.write(tmpl)

    print(f'Wrote: {fpath}')


def main():
    write_form('./land-form.json', LAND_TMPL, wfs_mappings)


LAND_TMPL = """[
    {
        "css": "todo",
        "help": "Selecting this will constrain the time selections shown below",
        "label": "Time frequency",
        "name": "frequency",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 3,
            "default": [
                null
            ],
            "id": 0,
__frequency__.__labels__,
__frequency__.__values__
        }
    },
    {
        "css": "todo",
        "help": "You can select as many variables as you like",
        "label": "Variable",
        "name": "variable",
        "required": true,
        "type": "StringListWidget",
        "details": {
            "columns": 2,
            "default": [],
            "id": 3,
__variable__.__labels__,
__variable__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Selecting this will reduce the number of observations available, due to usage restrictions",
        "label": "Intended use",
        "name": "intended_use",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 3,
            "default": [
                "Non-commercial"
            ],
            "id": 7,
__intended_use__.__labels__,
__intended_use__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Selecting &quot;Quality controlled&quot; will only return observations that have passed a certain level of QC (e.g. World Weather Record checks).",
        "label": "Data Quality",
        "name": "data_quality",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 3,
            "default": [
                "Quality controlled"
            ],
            "id": 9,
__data_quality__.__labels__,
__data_quality__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Select records with basic or detailed metadata",
        "label": "Column selection",
        "name": "column_selection",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 1,
            "default": [],
            "id": 11,
__column_selection__.__labels__,
__column_selection__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Select one option",
        "label": "Year",
        "name": "year",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 3,
            "default": [
                null
            ],
            "id": 4,
__year__.__labels__,
__year__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Months required",
        "label": "Month",
        "name": "month",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 3,
            "default": [
                null
            ],
            "id": 5,
__month__.__labels__,
__month__.__values__
        }
    },
    {
        "css": "todo",
        "help": "This selection is ONLY AVAILABLE for &quot;Daily&quot; and &quot;Sub-daily&quot; time-steps.",
        "label": "Day",
        "name": "day",
        "required": true,
        "type": "StringListWidget",
        "details": {
            "columns": 3,
            "default": [
                null
            ],
            "id": 6,
__day__.__labels__,
__day__.__values__
        }
    },
    {
        "css": "todo",
        "help": "This selection is ONLY AVAILABLE for &quot;Sub-daily&quot; time-steps.",
        "label": "Hour",
        "name": "hour",
        "required": true,
        "type": "StringListWidget",
        "details": {
            "columns": 3,
            "default": [
                null
            ],
            "id": 6,
__hour__.__labels__,
__hour__.__values__
        }
    },
    {
        "css": "todo",
        "help": "Selecting some areas of the globe will generate an empty result set",
        "label": "Bounding box",
        "name": "area",
        "required": true,
        "type": "GeographicExtentMapWidget",
        "details": {
            "id": 2,
            "accordion": false,
            "fullheight": true,
            "precision": 0,
            "range": {
                "e": 180,
                "n": 90,
                "s": -90,
                "w": -180
            },
            "wrapping": true
        }
    },
    {
        "css": "todo",
        "help": "Zipped outputs will be smaller in volume, they may contain multiple data files.",
        "label": "Format",
        "name": "format",
        "required": true,
        "type": "StringChoiceWidget",
        "details": {
            "columns": 1,
            "default": [
                "csv_zipped"
            ],
            "id": 10,
__format__.__labels__,
__format__.__values__
        }
    }
]
"""


if __name__ == '__main__':

    main()

