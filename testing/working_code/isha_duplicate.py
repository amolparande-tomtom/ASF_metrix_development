import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from thefuzz import fuzz

# MNR SQL Query
Buffer_ST_DWithin_mnr_osm_intersect_sql = """
                                            select
                                            code as country,
                                            mnr_apt.feat_id::text,
                                            mnr_address.lang_code,
                                            mnr_address.iso_lang_code,
                                            mnr_address.notation,
                                            mnr_address.iso_script,
                                            state_province_name.name as state_province_name,
                                            place_name.name as place_name,
                                            street_name.name as street_name,
                                            postal_code,
                                            building_name.name as building_name,
                                            hsn,
                                            round(ST_Distance(ST_Transform(mnr_apt.geom,900913),ST_Transform(ST_GeomFromText('{point_geometry}', 4326),900913)))as Distance,
                                            ST_AsText(mnr_apt.geom) as geom
                                            from
                                            "{schema_name}".mnr_apt
                                            inner join "{schema_name}".mnr_apt2addressset
                                            on mnr_apt2addressset.apt_id = mnr_apt.feat_id
                                            inner join "{schema_name}".mnr_addressset
                                            using (addressset_id)
                                            inner join "{schema_name}".mnr_address
                                            on "{schema_name}".mnr_address.addressset_id = "{schema_name}".mnr_addressset.addressset_id
                                            inner join "{schema_name}".mnr_address_scheme
                                            using(address_scheme_id)
                                            left join "{schema_name}".mnr_postal_point
                                            on "{schema_name}".mnr_postal_point.feat_id = "{schema_name}".mnr_address.postal_code_id
                                            left join "{schema_name}".mnr_hsn
                                            on mnr_hsn.hsn_id in ("{schema_name}".mnr_address.house_number_id, "{schema_name}".mnr_address.last_house_number_id)
                                            left join "{schema_name}".mnr_name as building_name
                                            on building_name.name_id = mnr_address.building_name_id
                                            left join "{schema_name}".mnr_name as place_name
                                            on place_name.name_id = mnr_address.place_name_id
                                            left join "{schema_name}".mnr_name as state_province_name
                                            on state_province_name.name_id = mnr_address.state_province_name_id
                                            left join "{schema_name}".mnr_name as street_name
                                            on street_name.name_id = mnr_address.street_name_id
                                            inner join "{schema_name}".mnr_apt_entrypoint
                                            on "{schema_name}".mnr_apt_entrypoint.apt_id = "{schema_name}".mnr_apt.feat_id
                                            inner join "{schema_name}".mnr_netw2admin_area
                                            using(netw_id)
                                            where "{schema_name}".mnr_apt_entrypoint.ep_type_postal
                                            and "{schema_name}".mnr_netw2admin_area.feat_type = 1111                  
                                            and ST_DWithin("{schema_name}".mnr_apt.geom, ST_GeomFromText('{point_geometry}',4326), {Buffer_in_Meter})

                                            """

# VAD SQL Query
Buffer_ST_DWithin_VAD_intersect_sql = """
                                        select 
                                        osm_id,
                                        tags ->'addr:housenumber:nl' as HouseNumber,
                                        tags -> 'addr:street:nl' as StreetName,
                                        tags ->'addr:postcode:nl' as PostalCode,
                                        tags -> 'addr:city:nl' as PlaceName,
                                        round(ST_Distance(ST_Transform(way,900913),ST_Transform(ST_GeomFromText('{point_geometry}', 4326),900913)))as Distance,
                                        ST_AsText(way) as way
                                        from "{schema_name_vad}".planet_osm_point
                                        WHERE osm_id BETWEEN 1000000000000000 AND 1999999999999999
                                        and 
                                        ST_DWithin("{schema_name_vad}".planet_osm_point.way, 
                                        ST_GeomFromText('{point_geometry}',4326), {Buffer_in_Meter})
                                     """


# Postgres Database connection

def postgres_db_connection(db_url):
    """
    :param db_url: Postgres Server
    :return: DB Connection
    """
    try:
        return psycopg2.connect(db_url)
    except Exception as error:
        print("Oops! An exception has occured:", error)
        print("Exception TYPE:", type(error))


def create_points_from_input_csv(csv_path):
    """
    info:create point GeoDataFrame from input ASF CSV.
    :param csv_path: input ASF CSV Path
    :return: Point GeoDataFrame
    """
    df = pd.read_csv(csv_path, encoding="utf-8")
    # creating a geometry column
    df['geometry'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]
    # Replace Nan or Empty or Null values with 0.0 because it flot
    df['provider_distance_orbis'] = df['provider_distance_orbis'].fillna(30.0)
    df['provider_distance_genesis'] = df['provider_distance_genesis'].fillna(30.0)
    # Creating a Geographic data frame
    return gpd.GeoDataFrame(df, crs="EPSG:4326")


def mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdf, schema_name, db_url, outputpath, filename):
    """

    :param csv_gdf:
    :param sql:
    :param schema_name:
    :param db_url:
    :param outputpath:
    :param filename:
    :return:
    """
    for i, r in csv_gdf.iterrows():

        add_header = True
        if i != 0:
            add_header = False
        schema_data = mnr_query_for_one_record(db_url, r, schema_name)

        # Fizzy Matching logic
        schema_data['hnr_match'] = 0
        schema_data['street_name_match'] = 0
        schema_data['place_name_match'] = 0
        schema_data['postal_code_name_match'] = 0
        # Statistics calculation
        schema_data['hnr_match%'] = 0
        schema_data['street_name_match%'] = 0
        schema_data['place_name_match%'] = 0
        schema_data['postal_code_name_match%'] = 0
        # Addition
        schema_data['Stats_Result'] = 0
        # Percentage
        schema_data['Percentage'] = 0

        # fuzzy MNR function
        mnr_calculate_fuzzy_values(r, schema_data)

        # Null, Empty, Missing Value Mapping
        schema_data['hsn'] = schema_data['hsn'].fillna(0)
        schema_data['street_name'] = schema_data['street_name'].fillna('NODATA')
        schema_data['postal_code'] = schema_data['postal_code'].fillna(0)
        schema_data['place_name'] = schema_data['place_name'].fillna('NODATA')

        # Writing CSV MNR function
        if not schema_data.empty:
            print("Done Processing for MNR" + r.searched_query)
            # Writing
            mnr_parse_schema_data(add_header, schema_data, outputpath, filename)


def mnr_parse_schema_data(add_header, schema_data, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.hsn != 0 and row.street_name != 'NODATA' and row.postal_code != 0 and row.place_name != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            if add_header:
                new_df.to_csv(outputpath + filename,mode='w', index=False)
            else:
                new_df.to_csv(outputpath + filename, mode='a', header=False, index=False)


def mnr_calculate_fuzzy_values(r, schema_data):
    for n, j in schema_data.iterrows():
        # House Number
        hnr_mt = (fuzz.token_set_ratio(j.hsn, j.searched_query))
        # Street Name
        sn_mt = (fuzz.token_set_ratio(j.street_name, j.searched_query))
        # Place Name
        pln_mt = (fuzz.token_set_ratio(j.place_name, r.searched_query))
        # Postal Code
        pcode_mt = (fuzz.token_set_ratio(j.postal_code, r.searched_query))
        schema_data.loc[n, 'hnr_match'] = hnr_mt
        schema_data.loc[n, 'street_name_match'] = sn_mt
        schema_data.loc[n, 'place_name_match'] = pln_mt
        schema_data.loc[n, 'postal_code_name_match'] = pcode_mt
        # Statistics calculation
        schema_data.loc[n, 'hnr_match%'] = (schema_data['hnr_match'][n] / 100)
        schema_data.loc[n, 'street_name_match%'] = (schema_data['street_name_match'][n] / 100)
        schema_data.loc[n, 'place_name_match%'] = (schema_data['place_name_match'][n] / 100)
        schema_data.loc[n, 'postal_code_name_match%'] = (schema_data['postal_code_name_match'][n] / 100)
        # Addition
        schema_data.loc[n, 'Stats_Result'] = (schema_data['hnr_match%'][n] +
                                              schema_data['street_name_match%'][n] +
                                              schema_data['place_name_match%'][n] +
                                              schema_data['postal_code_name_match%'][n])
        # Percentage
        schema_data.loc[n, 'Percentage'] = ((schema_data['Stats_Result'][n] / 4) * 100)


def mnr_query_for_one_record(db_url, r, schema_name):
    buffer = r.provider_distance_genesis * 0.00001
    print("SR_ID:", r.SR_ID, "distance_genesis:", r.provider_distance_genesis, "And", buffer)
    print("Geometry:", r.geometry)
    new_mnr_osm_intersect_sql = Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name}", schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, postgres_db_connection(db_url))
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    return schema_data


def vad_csv_buffer_db_apt_fuzzy_matching(csv_gdf, vad_schema_name, db_url, outputpath, filename):
    for i, r in csv_gdf.iterrows():
        add_header = True
        if i != 0:
            add_header = False
        schema_data = vad_query_for_one_record(db_url, r, vad_schema_name)

        # Fizzy Matching logic

        schema_data['hnr_match'] = 0
        schema_data['street_name_match'] = 0
        schema_data['place_name_match'] = 0
        schema_data['postal_code_name_match'] = 0
        # Statistics calculation
        schema_data['hnr_match%'] = 0
        schema_data['street_name_match%'] = 0
        schema_data['place_name_match%'] = 0
        schema_data['postal_code_name_match%'] = 0

        schema_data['Stats_Result'] = 0
        # Percentage
        schema_data['Percentage'] = 0
        # fuzzy VAD function
        vad_calculate_fuzzy_values(r, schema_data)

        # Null, Empty, Missing Value Mapping
        schema_data['housenumber'] = schema_data['housenumber'].fillna(0)
        schema_data['streetname'] = schema_data['streetname'].fillna('NODATA')
        schema_data['postalcode'] = schema_data['postalcode'].fillna(0)
        schema_data['placename'] = schema_data['placename'].fillna('NODATA')

        # Writing csv
        if not schema_data.empty:
            print("Done Processing for VAD" + r.searched_query)
            vad_parse_schema_data(schema_data, add_header, outputpath, filename)
            # if (schema_data.housenumber != 0 and schema_data.streetname != 'NODATA' and schema_data.postalcode != 0 and schema_data.placename != 'NODATA'):


def vad_calculate_fuzzy_values(r, schema_data):
    for n, j in schema_data.iterrows():
        # House Number
        hnr_mt = (fuzz.token_set_ratio(j.housenumber, r.searched_query))
        # Street Name
        sn_mt = (fuzz.token_set_ratio(j.streetname, r.searched_query))
        # Place Name
        pln_mt = (fuzz.token_set_ratio(j.postalcode, r.searched_query))
        # Postal Code
        pcode_mt = (fuzz.token_set_ratio(j.placename, r.searched_query))

        schema_data.loc[n, 'hnr_match'] = hnr_mt
        schema_data.loc[n, 'street_name_match'] = sn_mt
        schema_data.loc[n, 'place_name_match'] = pln_mt
        schema_data.loc[n, 'postal_code_name_match'] = pcode_mt
        # Statistics calculation
        schema_data.loc[n, 'hnr_match%'] = (schema_data['hnr_match'][n] / 100)
        schema_data.loc[n, 'street_name_match%'] = (schema_data['street_name_match'][n] / 100)
        schema_data.loc[n, 'place_name_match%'] = (schema_data['place_name_match'][n] / 100)
        schema_data.loc[n, 'postal_code_name_match%'] = (schema_data['postal_code_name_match'][n] / 100)

        # Addition

        schema_data.loc[n, 'Stats_Result'] = (schema_data['hnr_match%'][n] +
                                              schema_data['street_name_match%'][n] +
                                              schema_data['place_name_match%'][n] +
                                              schema_data['postal_code_name_match%'][n])
        # Percentage
        schema_data.loc[n, 'Percentage'] = ((schema_data['Stats_Result'][n] / 4) * 100)


def vad_query_for_one_record(db_url, r, vad_schema_name):
    buffer = r.provider_distance_orbis * 0.00001
    print("SR_ID:", r.SR_ID, "provider_distance_orbis:", r.provider_distance_orbis, "And", buffer)
    print("Geometry:", r.geometry)
    new_VAD_intersect_sql = Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name_vad}", vad_schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_VAD_intersect_sql, postgres_db_connection(db_url))
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    return schema_data


def vad_parse_schema_data(schema_data, add_header, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.housenumber != 0 and row.streetname != 'NODATA' and row.postalcode != 0 and row.placename != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            if add_header:
                new_df.to_csv(outputpath + filename, mode='w', index=False)
            else:
                new_df.to_csv(outputpath + filename, mode='a', header=False, index=False)


# Input CSV
inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/1_BEL/BEL_ASF_logs.csv'
outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/BEL/'
mnrfilename = 'MNR_Distance_Fuzzy_ASF_output_BRA_NL.csv'
vad_filename = 'VAD_Distance_Fuzzy_ASF_output_BRA_NL.csv'

# MNR DB URL


EUR_SO_NAM_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
LAM_MEA_OCE_SEA_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-006.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

# VAD DB URL
VAD_DB_Connections = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"

# SQL Query
# mnr_sql = Buffer_ST_DWithin_mnr_osm_intersect_sql

# schema
MNR_schema_name = 'eur_cas'
VAD_schema_name = 'eur_bel_20220521_cw20'

if __name__ == '__main__':
    csv_gdb = create_points_from_input_csv(inputcsv)
    # MNR calling
    mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdb, MNR_schema_name, EUR_SO_NAM_MNR_DB_Connections, outputpath, mnrfilename)
    # VAD calling
    vad_csv_buffer_db_apt_fuzzy_matching(csv_gdb, VAD_schema_name, VAD_DB_Connections, outputpath, vad_filename)
