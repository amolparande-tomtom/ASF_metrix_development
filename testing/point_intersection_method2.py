import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from shapely import wkt

from testing.DB_Connection_MNR import ST_DWithin_mnr_osm_intersect_sql, ST_DWithin_mnr_osm_intersect_sql

inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/aus_om_osm_logs_20220524.csv'

# DB Connections
# db_connection_url_5 = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
db_connection_url_2 = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

db_connection_MNR = psycopg2.connect(db_connection_url_2)

df = pd.read_csv(inputcsv)
# creating a geometry column
df['point_geom'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]

# Creating a Geographic data frame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry=df['point_geom'])

reproject_meter = csv_gdf.to_crs(epsg=900913)
buffer = reproject_meter.buffer(30)
reproject_wgs84 = buffer.to_crs(epsg=4326)
buffer_gdf = gpd.GeoDataFrame(csv_gdf, geometry=reproject_wgs84.geometry)

buffer_gdf['wkt_geom'] = buffer_gdf.geometry.apply(lambda x: wkt.dumps(x))

new_buffer_gdf = buffer_gdf.drop(['point_geom'], axis = 1)


new_buffer_gdf.to_file("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/2_Buffer_output.gpkg", layer='Input_Buffer', driver="GPKG", crs="EPSG:4326")

# buffer_gdf.to_file("/Users/parande/Documents/4_ASF_Metrix/0_input_csv/00_ASF_output.shp")
output_df = []
new_df = pd.DataFrame()

for i, r in buffer_gdf[:10].iterrows():
    new_mnr_osm_intersect_sql = ST_DWithin_mnr_osm_intersect_sql.replace('{polygon_geometry}', r.wkt_geom)
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, db_connection_MNR)
    schema_data['searched_query'] = r.searched_query
    schema_data['wkt_geom'] = r.wkt_geom
    schema_data['SRID'] = r.SR_ID

    if not schema_data.empty:
        # appending dataframe
        # new_df = new_df.append(schema_data, ignore_index=True)
        # output_df.append(schema_data)
        print("Done Processing for" + str(i) + r.searched_query)

        if i == 0:
            schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/0_Intersection_ASF_output.csv", index=False,
                               mode='w')
        else:
            schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/0_Intersection_ASF_output.csv", index=False,
                               mode='a',
                               header=False)

        # j = 0
        # query_column1 = []
        # query_column2 = []
        # query_column3 = []
        #
        #
        #
        # while j < len(schema_data):
        #     query_column1.append(r.searched_query)
        #     query_column2.append(r.provider_formatted_address_genesis)
        #     query_column3.append(r.SR_ID)
        #     j = j + 1
        # schema_data.insert(schema_data.columns.__len__(), "searched_query", query_column1, True)
        # schema_data.insert(schema_data.columns.__len__(), "provider_formatted_address_genesis", query_column2, True)
        # schema_data.insert(schema_data.columns.__len__(), "SR_ID", query_column3, True)
        #
        # output_df.append(schema_data)
        # pd.concat(output_df)

    # if i == 0:
    #     schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/0_input_csv/output_test.csv", index=False, mode='w')
    # else:
    #     schema_data.to_csv("/Users/parande/Documents/4_ASF_Metrix/0_input_csv/output_test.csv", index=False, mode='a',
    #                        header=False)

    # print("Done Processing for" + r.searched_query)

# merge_DataFrame= pd.concat(output_df, ignore_index=True)
# # Create Geometry for intersecting APT Geometry
# merge_DataFrame['apt_geom']= gpd.GeoSeries.from_wkt(merge_DataFrame.geom)
# # Create Geometry for input APT Points
# APT_gdf = gpd.GeoDataFrame(merge_DataFrame, crs="EPSG:4326", geometry='apt_geom')
#
# APT_gdf = APT_gdf.drop(['geom'], axis = 1)
# APT_gdf.to_file("/Users/parande/Documents/4_ASF_Metrix/1_geometry_validation/1_APT_output.gpkg", layer='APToutput', driver="GPKG", crs="EPSG:4326")

db_connection_MNR.close()

# new_df.to_csv('/Users/parande/Documents/4_ASF_Metrix/0_input_csv/Newoutput.csv')
