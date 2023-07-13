import os
import pandas as pd
import psycopg2
import geopandas as gpd
from shapely.wkt import loads

# Connect to PostgreSQL
def postgres_db_connection():
    """
    :param db_url: Postgres Server
    :return: DB Connection
    """
    try:
        con = psycopg2.connect(
            host="caprod-cpp-pgmnr-001.flatns.net",
            port="5432",
            database="mnr",
            user="mnr_ro",
            password="mnr_ro"
        )
        return con
    except Exception as error:
        print("Oops! An exception has occured:", error)
        print("Exception TYPE:", type(error))


sql_query = """
                set search_path to {schema_name},public;
                SELECT mnr_netw2nameset.netw_id,mnr_netw2nameset.line_side,mnr_netw_route_link.form_of_way,mnr_netw2nameset.nt_standard,mnr_netw2nameset.nt_alternate, mnr_name."name",
                mnr_name.normalized_name,
                ST_AsText(mnr_netw_geo_link.geom) as geom
                FROM mnr_netw2nameset mnr_netw2nameset, mnr_nameset mnr_nameset, mnr_name mnr_name,mnr_netw_route_link,mnr_netw_geo_link
                WHERE 
                mnr_netw2nameset.nameset_id = mnr_nameset.nameset_id
                AND mnr_netw2nameset.primary_name_id = mnr_name.name_id
                and mnr_netw2nameset.netw_id = mnr_netw_route_link.feat_id
                and mnr_netw_route_link.netw_geo_id = mnr_netw_geo_link.feat_id
                and mnr_netw2nameset.nt_standard  = 'true'
                --and mnr_name.normalized_name is not null
                and mnr_netw_route_link.form_of_way in ('3','2','15')
                and mnr_name."name" = '{streetName}'
            """

def geokageFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")

def geopackageFileWriter(geoDataFrame, layer_name, filepath):
    geoDataFrame.to_file(filepath, layer=layer_name, driver='GPKG', append=True)

schema_name = '_2023_06_008_eur_gbr_gbr'
csv_path = "/Users/parande/Documents/8_Adoc/GBR_Noname_R.csv"


filepath = '/Users/parande/Documents/8_Adoc/StreetName.gpkg'

csvData = pd.read_csv(csv_path, encoding="utf-8")
for i, row in csvData.iterrows():
    STNSQL = sql_query.replace("{schema_name}", schema_name).replace('{streetName}', row['Name_SN'])
    SQlCalling = pd.read_sql_query(STNSQL, postgres_db_connection())
    #Create new geometry column from the "way" column #
    SQlCalling['geometry'] = SQlCalling['geom'].apply(lambda way: loads(way.split(';')[0]))
    # Specify the column name to be removed
    column_to_remove = 'geom'
    # Drop the specified column
    SQlCallingFinal = SQlCalling.drop('geom', axis=1)
    StreetName = gpd.GeoDataFrame(SQlCallingFinal, geometry='geometry')
    geopackageFileWriter(StreetName, 'StreetName', filepath)
    print(row['Name_SN'])









