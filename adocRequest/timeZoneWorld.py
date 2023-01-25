import geopandas as gpd
from sqlalchemy import create_engine

url = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

sqlTimeZone = """
                select b2.country_code_char3 as country_code,
                b2.feat_id::text,
                b2.name,                
                b2.feat_type, 
                CASE b2.feat_type
                           WHEN 1111 THEN 'Country'
                           WHEN 1112 THEN 'Order1Area'
                           WHEN 1113 THEN 'Order2Area'
                           WHEN 1114 THEN 'Order3Area'
                           WHEN 1115 THEN 'Order4Area'
                           WHEN 1116 THEN 'Order5Area'
                           WHEN 1117 THEN 'Order6Area'
                           WHEN 1118 THEN 'Order7Area'
                           WHEN 1119 THEN 'Order8Area'
                           WHEN 1120 THEN 'Order9Area'
                       END admin_level,

                b2.time_zone, a2.summer_time,c2.validity, b2.geom as geom
                from
                (select maa.feat_id,feat_type, maa.country_code_char3, ma2.value_varchar as time_zone,geom, name
                from "{schema_name}".mnr_admin_area maa
                join "{schema_name}".mnr_admin_area2attribute maaa on maaa.admin_area_id = maa.feat_id
                join "{schema_name}".mnr_attribute ma2 on ma2.attribute_id = maaa.attribute_id
                where attribute_type in ('TZ')) b2
                left join (
                select admin_area_id, value_varchar as summer_time
                from "{schema_name}".mnr_admin_area2attribute maaa
                join "{schema_name}".mnr_attribute ma2 on ma2.parent_attribute_id = maaa.attribute_id
                where attribute_type in ('SU')) a2
                on b2.feat_id= a2.admin_area_id
                left join (select admin_area_id, value_varchar as validity
                from "{schema_name}".mnr_admin_area2attribute maaa
                join "{schema_name}".mnr_attribute ma2 on ma2.parent_attribute_id = maaa.attribute_id
                where attribute_type in ('VP')) c2
                on b2.feat_id= c2.admin_area_id

"""

schema_name = 'nam'

sqlTimeZoneNew = sqlTimeZone.replace("{schema_name}", schema_name)

con = create_engine(url)

gdf = gpd.GeoDataFrame.from_postgis(sqlTimeZoneNew, con)
gdf.to_file('/Users/parande/Documents/7_Adoc/0_TimeZoneWorld/nam_TimeZoneWorld.gpkg', driver='GPKG',
            layer='TimeZoneWorld', crs=4326)

print("##########################################################################")
print("Geometry Created")
print("##########################################################################")