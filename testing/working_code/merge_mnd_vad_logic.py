import sys

import psycopg2
import pandas as pd

URL = "postgresql://localhost/postgres?user=postgres&password=postgres&port=5433"

'postgresql://postgres:postgres@localhost:5433/postgres'

# MNR SQL
MNR_sql = """
        SELECT * FROM public."BEL_MNR_ASF_Log"

         """

# VAD SQL
VAD_sql = """
        SELECT * FROM public."BEL_VAD_ASF_Log"

        """


def postgres_db_connection(db_url):
    """
    :param db_url: Postgres Server
    :return: DB Connection
    """
    try:
        return psycopg2.connect(db_url)
    except Exception as error:
        print("Oops! An exception has occured:", error)
        print("Exception TYPE:", type(error))


# MNR Connection
mnr_SQLdata = pd.read_sql_query(MNR_sql, postgres_db_connection(URL))
mnr_schema = mnr_SQLdata.add_prefix("mnr_")
# MNR Drop Columns
df_mnr_schema = mnr_schema.drop(columns=['mnr_hnr_match', 'mnr_street_name_match', 'mnr_place_name_match',
                                         'mnr_postal_code_name_match', 'mnr_hnr_match%',
                                         'mnr_street_name_match%', 'mnr_place_name_match%',
                                         'mnr_postal_code_name_match%', 'mnr_Stats_Result']
                                )

df_mnr_schema['SRID'] = df_mnr_schema['mnr_SRID']
# VAD Connection
vad_SQLdata = pd.read_sql_query(MNR_sql, postgres_db_connection(URL))
vad_schema = vad_SQLdata.add_prefix("vad_")
# VAD Drop Columns
df_vad_schema = vad_schema.drop(columns=['vad_hnr_match', 'vad_street_name_match', 'vad_place_name_match',
                                         'vad_postal_code_name_match', 'vad_hnr_match%',
                                         'vad_street_name_match%', 'vad_place_name_match%',
                                         'vad_postal_code_name_match%', 'vad_Stats_Result']
                                )

df_vad_schema['SRID'] = df_vad_schema['vad_SRID']


mnr_vad_merge = pd.merge(df_mnr_schema, df_vad_schema, on=['SRID'])

outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/Postal_fix/'

mnr_vad_merge.to_csv(outputpath + "Merge_MNR_VAD.csv", mode='w', index=False)
df_mnr_schema.to_csv(outputpath + "df_mnr_schema.csv", mode='w', index=False)
df_vad_schema.to_csv(outputpath + "df_vad_schema.csv", mode='w', index=False)