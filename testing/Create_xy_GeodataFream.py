import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from shapely import wkt

inputcsv= '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/aus_om_osm_logs_20220524.csv'

df = pd.read_csv(inputcsv)
# creating a geometry column
geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]

# Creating a Geographic data frame
csv_gdf = gpd.GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)

reproject_meter = csv_gdf.to_crs(epsg=900913)
buffer = reproject_meter.buffer(50)
reproject_wgs84 = buffer.to_crs(epsg=4326)
buffer_gdf = gpd.GeoDataFrame(csv_gdf,geometry=reproject_wgs84.geometry)

buffer_gdf['wkt_geom'] = buffer_gdf.geometry.apply(lambda x: wkt.dumps(x))

for i , r in buffer_gdf.iterrows():
    print(r.wkt_geom)

csv_gdf.to_file('/Users/parande/Documents/4_ASF_Metrix/Australia_Point.shp')





