import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from thefuzz import fuzz

from testing.DB_Connection_MNR import Buffer_ST_DWithin_VAD_intersect_sql

schema_name_vad = 'eur_bel_20220521_cw20'

inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/1_BEL/BEL_ASF_logs.csv'

# DB Connections

db_connection_url_VAD = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"

db_connection_VAD = psycopg2.connect(db_connection_url_VAD)

df = pd.read_csv(inputcsv)
# creating a geometry column
df['geometry'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]

# Replace Nan or Empty or Null values with 0.0 because it flot
df['provider_distance_orbis'] = df['provider_distance_orbis'].fillna(0.0)
df['provider_distance_genesis'] = df['provider_distance_genesis'].fillna(0.0)

# Creating a Geographic data frame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")

# new_buffer_gdf = buffer_gdf.drop(['point_geom'], axis = 1)

# new_buffer_gdf.to_file("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/2_Buffer_output.gpkg", layer='Input_Buffer', driver="GPKG", crs="EPSG:4326")


output_df = []
new_df = pd.DataFrame()

# Amol Data code
for i, r in csv_gdf.iterrows():

    buffer = r.provider_distance_orbis * 0.00001
    print("SR_ID:", r.SR_ID, "distance_genesis:", r.provider_distance_genesis, "And", buffer)
    print("Geometry:", r.geometry)

    new_VAD_intersect_sql = Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name_vad}", schema_name_vad) \
        .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_VAD_intersect_sql, db_connection_VAD)
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_osm
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis

    # Fizzy Matching logic
    hnr_mt = []
    sn_mt = []
    pln_mt = []
    pcode_mt = []
    for n, j in schema_data.iterrows():
        # House Number
        hnr_mt.append(fuzz.token_set_ratio(j.housenumber, j.searched_query))
        # Street Name
        sn_mt.append(fuzz.token_set_ratio(j.streetname, j.searched_query))
        # Place Name
        pln_mt.append(fuzz.token_set_ratio(j.postalcode, r.searched_query))
        # Postal Code
        pcode_mt.append(fuzz.token_set_ratio(j.placename, r.searched_query))

    schema_data['hnr_match'] = hnr_mt
    schema_data['street_name_match'] = sn_mt
    schema_data['place_name_match'] = pln_mt
    schema_data['postal_code_name_match'] = pcode_mt
    # Statistics calculation
    schema_data['hnr_match%'] = (schema_data['hnr_match'] / 100)
    schema_data['street_name_match%'] = (schema_data['street_name_match'] / 100)
    schema_data['place_name_match%'] = (schema_data['place_name_match'] / 100)
    schema_data['postal_code_name_match%'] = (schema_data['postal_code_name_match'] / 100)

    # Addition

    schema_data['Stats_Result'] = (schema_data['hnr_match%'] +
                                   schema_data['street_name_match%'] +
                                   schema_data['place_name_match%'] +
                                   schema_data['postal_code_name_match%'])
    # Percentage
    schema_data['Percentage'] = ((schema_data['Stats_Result'] / 4) * 100)

    if not schema_data.empty:
        print("Done Processing for" + r.searched_query)

        if i == 0:
            schema_data.to_csv(
                "/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/VAD.csv", index=False,
                mode='w')
        else:
            schema_data.to_csv(
                "/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/VAD.csv", index=False,
                mode='a',
                header=False)
