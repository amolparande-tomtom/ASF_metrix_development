import sys

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

URL = "postgresql://localhost/postgres?user=postgres&password=postgres&port=5433"

'postgresql://postgres:postgres@localhost:5433/postgres'

# writing DB
engine = create_engine('postgresql://postgres:postgres@localhost:5433/postgres')
# MNR SQL
MNR_sql = """
        SELECT * FROM public."{MNR_ASF_pg_table}"

         """
# VAD SQL
VAD_sql = """
        SELECT * FROM public."{VAD_ASF_pg_table}"

        """


# def postgres_db_connection(db_url):
#     """
#     :param db_url: Postgres Server
#     :return: DB Connection
#     """
#     try:
#         return psycopg2.connect(db_url)
#     except Exception as error:
#         print("Oops! An exception has occured:", error)
#         print("Exception TYPE:", type(error))

def merge_mnr_vad_pg_table(pg_connection, MNR_ASF_pg_table, VAD_ASF_pg_table, outputpath):
    MNR_sql = """
            SELECT * FROM public."{MNR_ASF_pg_table}"

            """
    # VAD SQL
    VAD_sql = """
            SELECT * FROM public."{VAD_ASF_pg_table}"

            """
    # MNR Connection
    MNR_sql_new = MNR_sql.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table)

    mnr_SQLdata = pd.read_sql_query(MNR_sql_new, con=pg_connection)

    mnr_schema = mnr_SQLdata.add_prefix("mnr_")
    # MNR Drop Columns
    df_mnr_schema = mnr_schema.drop(columns=['mnr_hnr_match', 'mnr_street_name_match', 'mnr_place_name_match',
                                             'mnr_postal_code_name_match', 'mnr_hnr_match%',
                                             'mnr_street_name_match%', 'mnr_place_name_match%',
                                             'mnr_postal_code_name_match%', 'mnr_Stats_Result']
                                    )
    df_mnr_schema['SRID'] = df_mnr_schema['mnr_SRID']
    # VAD Connection

    VAD_sql_new = VAD_sql.replace("{VAD_ASF_pg_table}", VAD_ASF_pg_table)

    vad_SQLdata = pd.read_sql_query(VAD_sql_new, con=pg_connection)
    vad_schema = vad_SQLdata.add_prefix("vad_")
    # VAD Drop Columns
    df_vad_schema = vad_schema.drop(columns=['vad_hnr_match', 'vad_street_name_match', 'vad_place_name_match',
                                             'vad_postal_code_name_match', 'vad_hnr_match%',
                                             'vad_street_name_match%', 'vad_place_name_match%',
                                             'vad_postal_code_name_match%', 'vad_Stats_Result']
                                    )
    df_vad_schema['SRID'] = df_vad_schema['vad_SRID']
    mnr_vad_merge = pd.merge(df_mnr_schema, df_vad_schema, on='SRID', how='inner')
    # Writing to Postgres
    mnr_vad_merge.to_sql('merge_MNR_VAD_ASF', engine, if_exists='append')
    #
    mnr_vad_merge.to_csv(outputpath + "Merge_MNR_VAD.csv", mode='w', index=False)

MNR_ASF_pg_table = 'BEL_MNR_ASF_Log'
VAD_ASF_pg_table = 'MAX_BEL_VAD_ASF_Log'
outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/Postal_fix/'

merge_mnr_vad_pg_table(engine,MNR_ASF_pg_table, VAD_ASF_pg_table, outputpath)

