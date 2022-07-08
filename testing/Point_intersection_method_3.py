import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from shapely import wkt

from testing.DB_Connection_MNR import ST_DWithin_mnr_osm_intersect_sql

inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/aus_om_osm_logs_20220524.csv'

# DB Connections
# db_connection_url_5 = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
db_connection_url_2 = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

db_connection_MNR = psycopg2.connect(db_connection_url_2)

df = pd.read_csv(inputcsv)
# creating a geometry column
df['geometry'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]

# Creating a Geographic data frame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")

# new_buffer_gdf = buffer_gdf.drop(['point_geom'], axis = 1)

# new_buffer_gdf.to_file("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/2_Buffer_output.gpkg", layer='Input_Buffer', driver="GPKG", crs="EPSG:4326")


output_df = []
new_df = pd.DataFrame()

#Amol Data code
for i, r in csv_gdf[:10].iterrows():

    new_mnr_osm_intersect_sql = ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry))
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, db_connection_MNR)
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID

    if not schema_data.empty:
        print("Done Processing for" + r.searched_query)

        if i == 0:
            schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/ST_DWithin_ASF_output.csv", index=False,
                               mode='w')
        else:
            schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/ST_DWithin_ASF_output.csv", index=False,
                               mode='a',
                               header=False)


db_connection_MNR.close()

