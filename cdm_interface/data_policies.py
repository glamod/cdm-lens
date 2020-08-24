import pandas as pd
from io import StringIO

from cdm_interface._loader import local_conn_str


_data = """
country_id|country|institute|data_policy_link|data_policy
FI|Finland|Finnish Met Institute|https://en.ilmatieteenlaitos.fi/open-data|5
GM|Germany|DWD|https://opendata.dwd.de/climate_environment/CDC/Terms_of_use.pdf|0
EI|Ireland|Met ▒ireann|https://www.met.ie/climate/available-data/historical-data|5
LU|Luxembourg|Meteolux|https://www.meteolux.lu/fr/aide/aspects-legaux/?lang=fr, no additional data - https://community.wmo.int/notifications|5
NL|Netherlands|KNMI|https://www.knmi.nl/copyright|5
NO|Norway|MET Norway|https://www.met.no/en/free-meteorological-data/licensing-and-crediting|5
SW|Sweden|SMHI|https://www.smhi.se/omsmhi/policys/datapolicy/mer-och-mer-oppna-data-1.8138|5
US|United States|NOAA NCEI|https://www.ncdc.noaa.gov/wdcmet, https://obamawhitehouse.archives.gov/sites/default/files/omb/memoranda/2013/m-13-13.pdf|0
""".strip()


ndp = None


def get_national_data_policies():
    global ndp

    if not ndp:
        ndp = pd.read_csv(StringIO(_data), sep='|')

    return ndp


def _select(sql_query, conn_str=local_conn_str):
    """
    Issue a SELECT statement to the DB and return result
    as a DataFrame.
    """
    _conn = psycopg2.connect(conn_str)
    df = pandas.read_sql(sql_query, self._conn)
    return df


def _get_policies_by_source(df):
    source_ids = list(df['source_id'].unique())

    if not source_ids:
        return []

    source_ids_string = ', '.join(source_ids)
    source_sql = 'SELECT source_id, product_name, product_references, product_citation' \
                 f' FROM source_configuration WHERE source_id IN ({source_ids_string});'
    df = _select(source_sql)
    return df


def _get_policies_by_nation(df):
    country_ids = set([_[:2] for _ in df['observation_id']unique()])
    
    ndps = get_national_data_policies()
    countries_with_policies = set(ndps.country_id.unique())
    
    # Get countries with a policy that we have found data for
    countries = countries_with_policies & country_ids 

    # Extract and return records for countries with matching policies
    return df[df['observation_id'].str.slice(0, 2).str.contains('|'.join(countries), regex=True)]


def _render_data_policies(pols):
    """
    Render a dictionary of data policies and return as a string.
    """
    res = '# Data Policy Information for Observation Records\n\n'

    # Render by source first
    res += '## Data Policies derived from Sources\n\n'

    for _, row in pols['by_source'].iterrows():
        res += f'Source: {row["product_name"]} (ID: {row["source_id"]}):\n' 
        
        for key in ('product_name', 'product_references', 'product_citation'):
            if key in row:
                res += f'\t{key}: {row[key]}\n'

        else:
            res += '\tNo additional information available.\n'

        res += '\n'

    else:
        res += 'No data policy information derived from Source information.\n'

    # Now render by nation
    res += '\n## National Data Policies\n\n'

    for _, row in pols['by_nation'].iterrows():
        res += f'National Data Policy Information: {row["country"]} (ID: {row["country_id"]}):\n'

        for key in ('data_policy_link', 'institute'):
            if key in row:
                res += f'\t{key}: {row[key]}\n'

        else:
            raise Exception(f'Error when retrieving National Data Policy info for: {row["country_id"]}')

        res += '\n'

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


if __name__ == '__main__':

    test_get_national_data_policies()
