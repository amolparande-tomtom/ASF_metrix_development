import numpy as np
import psycopg2
import pandas as pd
from thefuzz import fuzz
from shapely.geometry import Point
import geopandas as gpd


# MNR DB schema name
from testing.DB_Connection_MNR import Buffer_ST_DWithin_mnr_osm_intersect_sql

mnr_schema_name = "nam"

inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/usa_tt_om_osm_logs_20220524.csv'

# DB Connections

db_connection_url_5 = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
# db_connection_url_2 = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
# MNR_schema name

db_connection_MNR = psycopg2.connect(db_connection_url_5)

df = pd.read_csv(inputcsv, encoding="utf-8")

# Replace Nan or Empty or Null values with 0.0 because it flot
df['provider_distance_orbis'] = df['provider_distance_orbis'].fillna(30.0)
df['provider_distance_genesis'] = df['provider_distance_genesis'].fillna(30.0)




# creating a geometry column
df['geometry'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]

# Creating a Geographic data frame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")




for i, r in csv_gdf.iterrows():
    buffer = r.provider_distance_genesis * 0.00001
    print("SR_ID:",r.SR_ID,"distance_genesis:" ,r.provider_distance_genesis, "And", buffer)
    print("Geometry:", r.geometry)


    new_mnr_osm_intersect_sql = Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry))\
                                                                            .replace("{schema_name}", mnr_schema_name)\
                                                                            .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, db_connection_MNR)

    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis

    # Fizzy Matching logic
    hnr_mt = []
    sn_mt = []
    pln_mt = []
    pcode_mt = []
    for n, j in schema_data.iterrows():
        # House Number
        hnr_mt.append(fuzz.token_set_ratio(j.hsn, j.searched_query))
        # Street Name
        sn_mt.append(fuzz.token_set_ratio(j.street_name, j.searched_query))
        # Place Name
        pln_mt.append(fuzz.token_set_ratio(j.place_name, r.searched_query))
        # Postal Code
        pcode_mt.append(fuzz.token_set_ratio(j.postal_code, r.searched_query))

    schema_data['hnr_match'] = hnr_mt
    schema_data['street_name_match'] = sn_mt
    schema_data['place_name_match'] = pln_mt
    schema_data['postal_code_name_match'] = pcode_mt
    # Statistics calculation
    schema_data['hnr_match%'] = (schema_data['hnr_match']/100)
    schema_data['street_name_match%'] = (schema_data['street_name_match']/100)
    schema_data['place_name_match%'] = (schema_data['place_name_match']/100)
    schema_data['postal_code_name_match%'] = (schema_data['postal_code_name_match']/100)

    # Addition

    schema_data['Stats_Result'] = (schema_data['hnr_match%']+
                                   schema_data['street_name_match%']+
                                   schema_data['place_name_match%']+
                                   schema_data['postal_code_name_match%'])
    # Percentage
    schema_data['Percentage'] = ((schema_data['Stats_Result']/4)*100)


    if not schema_data.empty :
        # print("Done Processing for" + r.searched_query, "Buffer Distance: ", r.provider_distance_genesis)

        if i == 0:
            schema_data.to_csv(
                "/Users/parande/Documents/4_ASF_Metrix/2_output/MNR_Distance_Fuzzy_ASF_output_USA.csv", index=False,
                mode='w')
        else:
            schema_data.to_csv(
                "/Users/parande/Documents/4_ASF_Metrix/2_output/MNR_Distance_Fuzzy_ASF_output_USA.csv", index=False,
                mode='a',
                header=False)





