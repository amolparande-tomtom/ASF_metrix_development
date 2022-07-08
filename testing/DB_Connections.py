import psycopg2
import pandas as pd

# 3G DB Connections
db_connection_url_3G_DB = "postgresql://3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"


# VD / ORBIS DB Connections
db_connection_url_ORBIS = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"

db_connection_ORBIS = psycopg2.connect(db_connection_url_ORBIS)

sql = """  
            SELECT
            osm_id,
            tags ->'addr:housenumber:en' as HouseNumber,
            tags -> 'addr:street:en' as StreetName,
            tags ->'addr:postcode:en' as PostalCode,
            tags -> 'addr:city:en' as PlaceName,
            way,
            tags as points_allTags
            from nam_usa_20220521_cw20.planet_osm_point
            WHERE osm_id BETWEEN 1000000000000000 AND 1999999999999999
        """

schema_data = pd.read_sql_query(sql, db_connection_ORBIS)

print(schema_data.housenumber)