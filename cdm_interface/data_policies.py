import pandas as pd
from io import StringIO


NATIONAL_POLICIES_FILE = '/usr/local/cdm_lens/tables/national_data_policies.psv' 
SOURCE_CONFIG_FILE = '/usr/local/cdm_lens/tables/source_configuration.psv'

ndp = None
source_config = None


def get_national_data_policies():
    global ndp

    if ndp is None:
        ndp = pd.read_csv(NATIONAL_POLICIES_FILE, sep='|')

    return ndp


def get_source_config():
    global source_config

    if source_config is None:
        source_config = pd.read_csv(SOURCE_CONFIG_FILE, sep='|')         

    return source_config


def _select(sql_query, conn_str):
    """
    Issue a SELECT statement to the DB and return result
    as a DataFrame.
    """
    _conn = psycopg2.connect(conn_str)
    df = pd.read_sql(sql_query, self._conn)
    return df


def _get_policies_by_source(df):
    source_ids = list(df['source_id'].unique())
    req_fields = ['source_id', 'product_name', 'product_references', 'product_citation']

    if not source_ids:
        return pd.DataFrame(columns=req_fields)

#    source_sql = 'SELECT source_id, product_name, product_references, product_citation' \
#                 f' FROM source_configuration WHERE source_id IN ({source_ids_string});'
#    df = _select(source_sql)

    source_config = get_source_config()
    source_df = source_config[source_config['source_id'].isin(source_ids)]

    # Return filtered df containing only the required fields
    return source_df[req_fields]


def _title(n):
    return n.replace('_', ' ').title()


def _value(v):
    if str(v).lower() == 'nan':
        return '-'
    else:
        return v


def _get_policies_by_nation(df):
    country_ids = set([_[:2] for _ in df['observation_id'].unique()])
    
    ndps = get_national_data_policies()
    countries_with_policies = set(ndps.country_id.unique())
    
    # Get countries with a policy that we have found data for
    countries = countries_with_policies & country_ids 

    # Extract and return records for countries with matching policies
    national_policies = ndps[ndps['country_id'].isin(list(countries))]
    return national_policies


def _render_data_policies(pols):
    """
    Render a dictionary of data policies and return as a string.
    """
    res = '# Data Policy Information for Observation Records\n\n'

    # Render by source first
    res += '## Data Policies derived from Sources\n\n'
    by_source = ''

    for _, row in pols['by_source'].iterrows():
        by_source += f'Source: {row["product_name"]} (Source ID: {row["source_id"]}):\n' 
        
        found = False
        for key in ('product_name', 'product_references', 'product_citation'):
            if key in row:
                found = True
                by_source += f'\t{_title(key)}: {_value(row[key])}\n'

        if not found:
            by_source += '\tNo additional information available.\n'

        by_source += '\n'

    if by_source:
        res += by_source
    else:
        res += 'No data policy information derived from Source information.\n'

    # Now render by nation
    res += '\n## National Data Policies\n\n'
    by_nation = ''

    for _, row in pols['by_nation'].iterrows():
        by_nation += f'National Data Policy Information: {row["country"]} (Nation ID: {row["country_id"]}):\n'

        found = False
        for key in ('data_policy_link', 'institute'):
            if key in row:
                found = True
                by_nation += f'\t{_title(key)}: {_value(row[key])}\n'

        if not found:
            raise Exception(f'Error when retrieving National Data Policy info for: {row["country_id"]}')

        by_nation += '\n'

    if by_nation:
        res += by_nation
    else:
        res += 'No data policy information derived from national policies.\n'

    return res 


def get_data_policies(df, rendered=True):
    """
    Return a data policies based on a set of observations (in a
    DataFrame).

    if `rendered` is True: return as a single string.
    if `rendered` is False: return as a dictionary.
    """
    pols = {}
    pols['by_source'] = _get_policies_by_source(df)
    pols['by_nation'] = _get_policies_by_nation(df)
        
    if rendered:
        pols = _render_data_policies(pols)

    return pols 


def test_get_national_data_policies():
    _ = get_national_data_policies()
    assert(len(_) == 8)
    assert(_.loc[7].country == 'United States')

    _data = """observation_id,data_policy_licence,date_time,date_time_meaning,observation_duration,longitude,latitude,report_type,height_above_surface,observed_variable,units,observation_value,value_significance,platform_type,station_type,primary_station_id,station_name,quality_flag,source_id
NLM00006253-1-1999-03-02-00:00-85-12,WMO essential,1999-03-02 00:00:00+00:00,beginning,instantaneous,2.067,56.4,SYNOP,2.0,air temperature,K,279.35,Instantaneous value of observed parameter,,Land station,NLM00006253,AUK-ALFA,Passed,251
NOM00006253-1-1999-03-02-00:00-85-12,WMO essential,1999-03-02 00:00:00+00:00,beginning,instantaneous,2.067,56.4,SYNOP,2.0,air temperature,K,279.35,Instantaneous value of observed parameter,,Land station,NLM00006253,AUK-ALFA,Passed,225
EIM00006253-1-1999-03-02-00:00-85-12,WMO essential,1999-03-02 00:00:00+00:00,beginning,instantaneous,2.067,56.4,SYNOP,2.0,air temperature,K,279.35,Instantaneous value of observed parameter,,Land station,NLM00006253,AUK-ALFA,Passed,291"""

    df = pd.read_csv(StringIO(_data), sep=',')
    pols = get_data_policies(df)
    print(pols)

    all_columns = ['observation_id', 'data_policy_licence', 'date_time', 'date_time_meaning', 'observation_duration', 'longitude', 'latitude', 'report_type', 'height_above_surface', 'observed_variable', 'units', 'observation_value', 'value_significance', 'platform_type', 'station_type', 'primary_station_id', 'station_name', 'quality_flag', 'source_id']
    empty_df = pd.DataFrame(columns=all_columns)

    pols = get_data_policies(empty_df)
    assert pols == '# Data Policy Information for Observation Records\n\n## Data Policies derived from Sources\n\nNo data policy information derived from Source information.\n\n## National Data Policies\n\nNo data policy information derived from national policies.\n'
    print('Empty data policy for no data works!')


if __name__ == '__main__':

    test_get_national_data_policies()
