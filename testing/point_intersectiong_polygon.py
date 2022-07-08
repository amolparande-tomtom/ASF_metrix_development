import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from shapely import wkt

from testing.DB_Connection_MNR import mnr_osm_intersect_sql

inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/aus_om_osm_logs_20220524.csv'

# Postgres DB URL
# db_connection_url_5 = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
db_connection_url_2 = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
# Read Postgres DB Connections
db_connection_MNR = psycopg2.connect(db_connection_url_2)
# Read Input CSV
df = pd.read_csv(inputcsv)
# creating a geometry column from existing latitude and longitude values
geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
# Creating a GeoDataFrame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)
# Reproject GCS to PCS
reproject_meter = csv_gdf.to_crs(epsg=900913)
# Point to buffer
buffer = reproject_meter.buffer(30)
# Reproject PCS to GCS
reproject_wgs84 = buffer.to_crs(epsg=4326)
# Creating a GeoDataFrame for buffer
buffer_gdf = gpd.GeoDataFrame(csv_gdf, geometry=reproject_wgs84.geometry)
# convert geometry to WKT
buffer_gdf['wkt_geom'] = buffer_gdf.geometry.apply(lambda x: wkt.dumps(x))

output_df = []

for i, r in buffer_gdf[:10].iterrows():
    mnr_osm_intersect_sql_1 = mnr_osm_intersect_sql.replace('{polygon_geometry}', r.wkt_geom)
    schema_data = pd.read_sql_query(mnr_osm_intersect_sql_1, db_connection_MNR)
    if not schema_data.empty:
        # appending dataframe
        # new_df = new_df.append(schema_data, ignore_index=True)
        # new_df = new_df.append(schema_data)
        j = 0
        query_column1 = []
        query_column2 = []
        query_column3 = []
        query_column4 = []
        query_column5 = []

        while j < len(schema_data):
            query_column1.append(r.searched_query)
            query_column2.append(r.provider_formatted_address_genesis)
            query_column3.append(r.lat)
            query_column4.append(r.lon)
            query_column5.append(r.SR_ID)
            j = j + 1
        schema_data.insert(schema_data.columns.__len__(), "searched_query", query_column1, True)
        schema_data.insert(schema_data.columns.__len__(), "provider_formatted_address_genesis", query_column2, True)
        schema_data.insert(schema_data.columns.__len__(), "lat", query_column3, True)
        schema_data.insert(schema_data.columns.__len__(), "lon", query_column4, True)
        schema_data.insert(schema_data.columns.__len__(), "lon", query_column5, True)

    # output_df.append(schema_data)
    # pd.concat(output_df)

    if i == 0:
        schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/0_input_csv/New_ASF_output_test.csv", index=False, mode='w')
    else:
        schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/0_input_csv/New_ASF_output_test.csv", index=False, mode='a',
                           header=False)


    print("Done Processing for" + r.searched_query)

output_df

# new_df.to_csv('/Users/parande/Documents/4_ASF_Metrix/0_input_csv/Newoutput.csv')
