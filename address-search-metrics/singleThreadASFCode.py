import os
import re
import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from thefuzz import fuzz
from sqlalchemy import create_engine
import unidecode
from datetime import datetime
import logging
import time

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
                                        tags ->'addr:housenumber:{language_code}' as HouseNumber,
                                        tags -> 'addr:street:{language_code}' as StreetName,
                                        tags ->'addr:postcode:{language_code}' as PostalCode,
                                        tags -> 'addr:city:{language_code}' as PlaceName,
                                        round(ST_Distance(ST_Transform(way,900913),ST_Transform(ST_GeomFromText('{point_geometry}', 4326),900913)))as Distance,
                                        ST_AsText(way) as way
                                        from "{schema_name_vad}".planet_osm_point
                                        WHERE (CAST(planet_osm_point.tags AS TEXT) LIKE '%address_point%')
                                        and 
                                        ST_DWithin("{schema_name_vad}".planet_osm_point.way, 
                                        ST_GeomFromText('{point_geometry}',4326), {Buffer_in_Meter})
                                     """

# Local Postgres VAD SQL for MAX

VAD_sql = """
                SELECT * FROM public."{vad_table}"     
          """
# --1) present in MNR Only
SQL_present_in_MNR_only = """
                        select "{MNR_ASF_pg_table}".* ,"{VAD_ASF_pg_table}"."SRID" 

                        from public."{MNR_ASF_pg_table}" 
                        left outer join public."{VAD_ASF_pg_table}"

                        on "{MNR_ASF_pg_table}"."SRID" = "{VAD_ASF_pg_table}"."SRID" 
                        where "{VAD_ASF_pg_table}"."SRID" is null 

                      """

# --2) present in VAD Only
SQL_present_in_vad_only = """
                            select "{MNR_ASF_pg_table}"."SRID"  ,"{VAD_ASF_pg_table}".* 

                            from "{MNR_ASF_pg_table}" 
                            right outer join "{VAD_ASF_pg_table}"

                            on "{MNR_ASF_pg_table}"."SRID" = "{VAD_ASF_pg_table}"."SRID" 
                            where "{MNR_ASF_pg_table}"."SRID" is null 
                        """


def input_csv_to_postgres(csv_path, pg_connection, csvfilename):
    df = pd.read_csv(csv_path, encoding="utf-8")
    # Replace Nan or Empty or Null values with 0.0 because it flot
    df['provider_distance_orbis'] = df['provider_distance_orbis'].fillna(30.0)
    df['provider_distance_genesis'] = df['provider_distance_genesis'].fillna(30.0)
    # # create unique serial numbers pandas
    # df.insert(1, 'SR_ID_new', range(1, 1 + len(df)))
    # Dump into Postgres
    df.to_sql(csvfilename, pg_connection, if_exists='append')


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
        if os.path.exists(outputpath + filename):
            add_header = False
        # add_header = True
        # if i != 0:
        #     add_header = False
        schema_data = mnr_query_for_one_record(db_url, r, schema_name)

        # # Fizzy Matching logic
        # schema_data['hnr_match'] = 0
        # schema_data['street_name_match'] = 0
        # schema_data['place_name_match'] = 0
        # schema_data['postal_code_name_match'] = 0
        # # Statistics calculation
        # schema_data['hnr_match%'] = 0
        # schema_data['street_name_match%'] = 0
        # schema_data['place_name_match%'] = 0
        # schema_data['postal_code_name_match%'] = 0
        # # Addition
        # schema_data['Stats_Result'] = 0
        # # Percentage
        # schema_data['Percentage'] = 0

        # fuzzy MNR function
        mnr_calculate_fuzzy_values(r, schema_data, mnr_hnr, mnr_street_name, mnr_place_name, mnr_postal_code,
                                   hnr_street_name, integer_hnr, integer_postal_code, distance)

        # Null, Empty, Missing Value Mapping
        # schema_data['hsn'] = schema_data['hsn'].fillna(0)
        # schema_data['street_name'] = schema_data['street_name'].fillna('NODATA')
        # schema_data['postal_code'] = schema_data['postal_code'].fillna(0)
        # schema_data['place_name'] = schema_data['place_name'].fillna('NODATA')

        if schema_data.empty:
            print("Data Empty")

        # Writing CSV MNR function
        if not schema_data.empty:
            print("MNR_SRID:", schema_data.SRID)

            # Writing to Postgres
            # schema_data = schema_data.drop(['geometry'], axis=1)
            # schema_data.to_sql('BEL_MNR_ASF_Log', engine, if_exists='append')

            # Writing CSV
            # mnr_parse_schema_data(add_header, schema_data, outputpath, filename)
            mnr_parse_schema_data_old(schema_data, outputpath, filename)


def mnr_query_for_one_record(db_url, r, schema_name):
    buffer = r.provider_distance_genesis * 0.00001
    # print("SR_ID:", r.SR_ID, "distance_genesis:", r.provider_distance_genesis, "And", buffer)
    # print("Geometry:", r.geometry)
    new_mnr_osm_intersect_sql = Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name}", schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, postgres_db_connection(db_url))
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    # replace "distance" less than 1 value with 1
    schema_data.loc[schema_data['distance'] < 1, 'distance'] = 1
    return schema_data


def mnr_calculate_fuzzy_values(r, schema_data, mnr_hnr, mnr_street_name, mnr_place_name, mnr_postal_code,
                               hnr_street_name, integer_hnr, integer_postal_code, distance):
    for n, j in schema_data.iterrows():
        searched_query = str(r.searched_query)
        hsn = str(j.hsn)
        street_name = str(j.street_name)
        place_name = str(j.place_name)
        postal_code = str(j.postal_code)

        # 1 # Concatenate House Number + Street Name
        hnr_stn = hsn + " " + street_name

        hnr_mt = 0
        sn_mt = 0
        pln_mt = 0
        pcode_mt = 0

        # House Number
        if hsn != 'None':
            hnr_mt = fuzz.token_set_ratio(unidecode.unidecode(hsn).lower(),
                                          unidecode.unidecode(searched_query).lower())
        # Street Name
        if street_name != 'None':
            sn_mt = fuzz.token_set_ratio(unidecode.unidecode(street_name).lower(),
                                         unidecode.unidecode(searched_query).lower())
        # Place Name
        if place_name != 'None':
            pln_mt = fuzz.token_set_ratio(unidecode.unidecode(place_name).lower(),
                                          unidecode.unidecode(searched_query).lower())
        # Postal Code
        if postal_code != 'None':
            pcode_mt = fuzz.token_set_ratio(unidecode.unidecode(postal_code).lower(),
                                            unidecode.unidecode(searched_query).lower())
        # House Number Street Name

        hnr_stn_mt = fuzz.token_set_ratio(unidecode.unidecode(hnr_stn).lower(),
                                          unidecode.unidecode(searched_query).lower())

        # remove Alphabet and remove space start and end

        # remove Alphabet from searched_query
        alph_searched_query = (re.sub("[a-zA-Z]", '', unidecode.unidecode(searched_query))).strip()

        # remove House Number  from searched_query
        alph_hsn = (re.sub("[a-zA-Z]", '', unidecode.unidecode(hsn))).strip()

        # remove Postal code  from searched_query
        alph_postal_code = (re.sub("[a-zA-Z]", '', unidecode.unidecode(postal_code))).strip()

        # integer house number
        new_hnr_mt = fuzz.token_set_ratio(unidecode.unidecode(alph_hsn), unidecode.unidecode(alph_searched_query))
        # integer postal code
        new_pcode_mt = fuzz.token_set_ratio(unidecode.unidecode(alph_postal_code),
                                            unidecode.unidecode(alph_searched_query))

        schema_data.loc[n, 'hnr_match'] = hnr_mt
        schema_data.loc[n, 'street_name_match'] = sn_mt
        schema_data.loc[n, 'place_name_match'] = pln_mt
        schema_data.loc[n, 'postal_code_name_match'] = pcode_mt

        # Concatenate Attributes
        schema_data.loc[n, 'hnr_street_name_match'] = hnr_stn_mt

        # New added
        schema_data.loc[n, 'new_hnr_match'] = new_hnr_mt
        schema_data.loc[n, 'new_postal_code_name_match'] = new_pcode_mt

        # distance percentage
        schema_data.loc[n, 'distance_match'] = (100 / schema_data['distance'][n])

        # Missing record

        # Percentage
        schema_data.loc[n, 'Percentage'] = ((schema_data['hnr_match'][n] / 100) * mnr_hnr +
                                            (schema_data['street_name_match'][n] / 100) * mnr_street_name +
                                            (schema_data['place_name_match'][n] / 100) * mnr_place_name +
                                            (schema_data['postal_code_name_match'][n] / 100) * mnr_postal_code +
                                            (schema_data['hnr_street_name_match'][n] / 100) * hnr_street_name +
                                            (schema_data['new_hnr_match'][n] / 100) * integer_hnr +
                                            (schema_data['new_postal_code_name_match'][n] / 100) * integer_postal_code +
                                            (schema_data['distance_match'][n] / 100) * distance
                                            ).round(4)


def mnr_parse_schema_data_old(schema_data, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            csvFileWriter(new_df, filename, outputpath)


def mnr_parse_schema_data(add_header, schema_data, outputpath, filename):
    column_names = ["SRID", "country", "feat_id", "lang_code", "iso_lang_code", "notation",
                    "iso_script", "state_province_name", "place_name", "street_name", "postal_code",
                    "building_name", "hsn", "distance", "geom", "searched_query", "geometry",
                    "provider_distance_orbis", "provider_distance_genesis", "hnr_match",
                    "street_name_match", "place_name_match", "postal_code_name_match",
                    "hnr_street_name_match", "new_hnr_match", "new_postal_code_name_match",
                    "distance_match", "Percentage"]
    schema_data = schema_data.reindex(columns=column_names)
    group_max = schema_data.groupby('SRID')['Percentage'].max()
    pd_group_max = pd.DataFrame(group_max)
    mx_apt_delta = pd.merge(schema_data, pd_group_max, on=['SRID', 'Percentage'])

    # Check group has more than one values then take minimum distance

    if mx_apt_delta['Percentage'].value_counts().values.max() > 1:
        min_distance = mx_apt_delta['distance'].min()
        distance_mx_apt_delta = mx_apt_delta.loc[mx_apt_delta['distance'] == min_distance]
        # get First Values
        if distance_mx_apt_delta['Percentage'].value_counts().values.max() != 1:
            distance_mx_apt_delta = distance_mx_apt_delta.head(1)
            # Writing to Postgres
            distance_mx_apt_delta = distance_mx_apt_delta.drop(['geometry'], axis=1)
            # distance_mx_apt_delta.to_sql(filename, engine, if_exists='append')

            # Writing to CSV
            csvFileWriter(distance_mx_apt_delta, filename + ".csv", outputpath)

        else:
            # Writing to Postgres
            distance_mx_apt_delta = distance_mx_apt_delta.drop(['geometry'], axis=1)
            # distance_mx_apt_delta.to_sql(filename, engine, if_exists='append')
            # Writing to CSV
            csvFileWriter(distance_mx_apt_delta, filename + ".csv", outputpath)


    else:
        for indx, row in mx_apt_delta.iterrows():
            if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
                new_df = pd.DataFrame(row).transpose()

                # Writing to Postgres
                new_df = new_df.drop(['geometry'], axis=1)
                # new_df.to_sql(filename, engine, if_exists='append')

                # Writing to CSV
                csvFileWriter(new_df, filename + ".csv", outputpath)


def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")


def vad_csv_buffer_db_apt_fuzzy_matching(csv_gdf, vad_schema_name, db_url, outputpath, filename, language_code):
    for i, r in csv_gdf.iterrows():
        add_header = True
        if os.path.exists(outputpath + filename):
            add_header = False
        # add_header = True
        # if i != 0:
        #     add_header = False
        schema_data = vad_query_for_one_record(db_url, r, vad_schema_name, language_code)

        schema_data['language_code'] = language_code

        # fuzzy VAD function

        vad_calculate_fuzzy_values(r, schema_data, mnr_hnr, mnr_street_name, mnr_place_name, mnr_postal_code,
                                   hnr_street_name, integer_hnr, integer_postal_code, distance)
        # Null, Empty, Missing Value Mapping
        # schema_data['housenumber'] = schema_data['housenumber'].fillna(0)
        # schema_data['streetname'] = schema_data['streetname'].fillna('NODATA')
        # schema_data['postalcode'] = schema_data['postalcode'].fillna(0)
        # schema_data['placename'] = schema_data['placename'].fillna('NODATA')

        # Writing csv Empty ASF
        if schema_data.empty:
            print("Data Empty")

        if not schema_data.empty:
            # print("VAD_SRID:", schema_data.SRID)

            # Writing to the Postgres

            # Writing CSV
            vad_parse_schema_data(schema_data, add_header, outputpath, filename)

            # vad_parse_schema_data_old(schema_data, outputpath, filename)


def vad_query_for_one_record(db_url, r, vad_schema_name, language_code):
    buffer = r.provider_distance_orbis * 0.00001
    # print("SR_ID:", r.SR_ID, "language_code:", language_code, "provider_distance_orbis:", r.provider_distance_orbis,
    #       "And", buffer)
    # print("Geometry:", r.geometry)
    new_VAD_intersect_sql = Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name_vad}", vad_schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer)) \
        .replace("{language_code}", language_code)
    schema_data = pd.read_sql_query(new_VAD_intersect_sql, postgres_db_connection(db_url))
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    # replace "distance" less than 1 value with 1
    schema_data.loc[schema_data['distance'] < 1, 'distance'] = 1
    return schema_data


def vad_calculate_fuzzy_values(r, schema_data, mnr_hnr, mnr_street_name, mnr_place_name, mnr_postal_code,
                               hnr_street_name, integer_hnr, integer_postal_code, distance):
    for n, j in schema_data.iterrows():
        searched_query = str(r.searched_query)
        hsn = str(j.housenumber)
        street_name = str(j.streetname)
        place_name = str(j.placename)
        postal_code = str(j.postalcode)

        hnr_mt = 0
        sn_mt = 0
        pln_mt = 0
        pcode_mt = 0

        # 1 # Concatenate House Number + Street Name
        hnr_stn = hsn + " " + street_name

        # House Number
        if hsn != 'None':
            hnr_mt = fuzz.token_set_ratio(unidecode.unidecode(hsn).lower(), unidecode.unidecode(searched_query).lower())
        # Street Name
        if street_name != 'None':
            sn_mt = fuzz.token_set_ratio(unidecode.unidecode(street_name).lower(),
                                         unidecode.unidecode(searched_query).lower())
        # Place Name
        if place_name != 'None':
            pln_mt = fuzz.token_set_ratio(unidecode.unidecode(place_name).lower(),
                                          unidecode.unidecode(searched_query).lower())
        # Postal Code
        if postal_code != 'None':
            pcode_mt = fuzz.token_set_ratio(unidecode.unidecode(postal_code).lower(),
                                            unidecode.unidecode(searched_query).lower())

        # House Number Street Name
        hnr_stn_mt = fuzz.token_set_ratio(unidecode.unidecode(hnr_stn).lower(),
                                          unidecode.unidecode(searched_query).lower())
        # remove Alphabet and remove space start and end

        # remove Alphabet from searched_query
        alph_searched_query = (re.sub("[a-zA-Z]", '', unidecode.unidecode(searched_query))).strip()

        # remove House Number  from searched_query
        alph_hsn = (re.sub("[a-zA-Z]", '', unidecode.unidecode(hsn))).strip()

        # remove Postal code  from searched_query
        alph_postal_code = (re.sub("[a-zA-Z]", '', unidecode.unidecode(postal_code))).strip()

        # integer house number
        new_hnr_mt = fuzz.token_set_ratio(unidecode.unidecode(alph_hsn), unidecode.unidecode(alph_searched_query))
        # integer postal code
        new_pcode_mt = fuzz.token_set_ratio(unidecode.unidecode(alph_postal_code),
                                            unidecode.unidecode(alph_searched_query))

        schema_data.loc[n, 'hnr_match'] = hnr_mt
        schema_data.loc[n, 'street_name_match'] = sn_mt
        schema_data.loc[n, 'place_name_match'] = pln_mt
        schema_data.loc[n, 'postal_code_name_match'] = pcode_mt

        # Concatenate Attributes
        schema_data.loc[n, 'hnr_street_name_match'] = hnr_stn_mt

        # New added
        schema_data.loc[n, 'new_hnr_match'] = new_hnr_mt
        schema_data.loc[n, 'new_postal_code_name_match'] = new_pcode_mt

        # distance percentage
        schema_data.loc[n, 'distance_match'] = (100 / schema_data['distance'][n])

        # Percentage

        schema_data.loc[n, 'Percentage'] = ((schema_data['hnr_match'][n] / 100) * mnr_hnr +
                                            (schema_data['street_name_match'][n] / 100) * mnr_street_name +
                                            (schema_data['place_name_match'][n] / 100) * mnr_place_name +
                                            (schema_data['postal_code_name_match'][n] / 100) * mnr_postal_code +
                                            (schema_data['hnr_street_name_match'][n] / 100) * hnr_street_name +
                                            (schema_data['new_hnr_match'][n] / 100) * integer_hnr +
                                            (schema_data['new_postal_code_name_match'][n] / 100) * integer_postal_code +
                                            (schema_data['distance_match'][n] / 100) * distance
                                            ).round(4)


def vad_parse_schema_data_old(schema_data, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            # Writing to Postgres
            new_df = new_df.drop(['geometry'], axis=1)
            new_df.to_sql(filename, engine, if_exists='append')
            print("##############Printed DATA######################")
            print(schema_data.SRID)
            # Writing to CSV
            # csvFileWriter(new_df, filename, outputpath)

            # Old Logic

            # if add_header:
            #     new_df.to_csv(outputpath + filename, mode='w', index=False)
            #     print("##############Printed DATA######################")
            #     add_header = False
            # else:
            #     new_df.to_csv(outputpath + filename, mode='a', header=False, index=False)
            #     print("##############Printed DATA######################")

        else:
            print(row.SRID, "Blank rows")


def vad_parse_schema_data(schema_data, add_header, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            # Writing to Postgres
            new_df = new_df.drop(['geometry'], axis=1)
            new_df.to_sql(filename, engine, if_exists='append')
            print("##############Printed DATA######################")
            print(schema_data.SRID)
            # Writing to CSV
            csvFileWriter(new_df, filename, outputpath)

            # Old Logic

            # if add_header:
            #     new_df.to_csv(outputpath + filename, mode='w', index=False)
            #     print("##############Printed DATA######################")
            #     add_header = False
            # else:
            #     new_df.to_csv(outputpath + filename, mode='a', header=False, index=False)
            #     print("##############Printed DATA######################")

        else:
            print(row.SRID, "Blank rows")


def vad_parse_schema_data_postgres_max(pg_connection, vad_table):
    vad_table_sql = VAD_sql.replace("{vad_table}", vad_table)
    schema_data = pd.read_sql(vad_table_sql, con=pg_connection)
    group_max = schema_data.groupby('SRID')['Percentage'].max()
    pd_group_max = pd.DataFrame(group_max)
    mx_apt_delta_merge = pd.merge(schema_data, pd_group_max, on=['SRID', 'Percentage']).sort_values('SRID')
    SRID = list(mx_apt_delta_merge["SRID"].unique())

    for i in SRID:
        mx_apt_delta_new = mx_apt_delta_merge.loc[mx_apt_delta_merge['SRID'] == i]
        if mx_apt_delta_new['Percentage'].value_counts().values.max() > 1:
            min_distance = mx_apt_delta_new['distance'].min()
            distance_mx_apt_delta = mx_apt_delta_new.loc[mx_apt_delta_new['distance'] == min_distance]
            # get First Values
            if distance_mx_apt_delta['Percentage'].value_counts().values.max() != 1:
                distance_mx_apt_delta = distance_mx_apt_delta.head(1)
                # Writing to Postgres
                distance_mx_apt_delta.to_sql('MAX_' + vad_table, engine, if_exists='append')
            else:
                # Writing to Postgres
                distance_mx_apt_delta.to_sql('MAX_' + vad_table, engine, if_exists='append')

        else:
            # Writing to Postgres
            mx_apt_delta_new.to_sql('MAX_' + vad_table, engine, if_exists='append')


def merge_mnr_vad_pg_table(pg_connection, MNR_ASF_pg_table, VAD_ASF_pg_table, outputpath):
    MNR_sql = """
            SELECT * FROM public."{MNR_ASF_pg_table}"

            """
    # VAD SQL
    VAD_sql = """
            SELECT * FROM public."{VAD_ASF_pg_table}"

            """
    # MNR Connection
    MNR_sql_new = MNR_sql.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table)

    mnr_SQLdata = pd.read_sql_query(MNR_sql_new, con=pg_connection)

    mnr_schema = mnr_SQLdata.add_prefix("mnr_")
    # MNR Drop Columns
    df_mnr_schema = mnr_schema.drop(columns=['mnr_hnr_match', 'mnr_street_name_match', 'mnr_place_name_match',
                                             'mnr_postal_code_name_match', 'mnr_hnr_match%',
                                             'mnr_street_name_match%', 'mnr_place_name_match%',
                                             'mnr_postal_code_name_match%', 'mnr_Stats_Result']
                                    )
    df_mnr_schema['SRID'] = df_mnr_schema['mnr_SRID']
    # VAD Connection

    VAD_sql_new = VAD_sql.replace("{VAD_ASF_pg_table}", 'MAX_' + VAD_ASF_pg_table)

    vad_SQLdata = pd.read_sql_query(VAD_sql_new, con=pg_connection)
    vad_schema = vad_SQLdata.add_prefix("vad_")
    # VAD Drop Columns
    df_vad_schema = vad_schema.drop(columns=['vad_hnr_match', 'vad_street_name_match', 'vad_place_name_match',
                                             'vad_postal_code_name_match', 'vad_hnr_match%',
                                             'vad_street_name_match%', 'vad_place_name_match%',
                                             'vad_postal_code_name_match%', 'vad_Stats_Result']
                                    )
    df_vad_schema['SRID'] = df_vad_schema['vad_SRID']
    mnr_vad_merge = pd.merge(df_mnr_schema, df_vad_schema, on='SRID', how='inner')
    # Writing to Postgres
    mnr_vad_merge.to_sql('merge_MNR_VAD_ASF', engine, if_exists='append')
    #
    mnr_vad_merge.to_csv(outputpath + "1_ASF_Merge_MNR_VAD.csv", mode='w', index=False)


def ASF_present_in_MNR_only(pg_connection, MNR_ASF_pg_table, VAD_ASF_pg_table, outputpath):
    SQL_present_in_MNR_only_new = SQL_present_in_MNR_only.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table) \
        .replace("{VAD_ASF_pg_table}", VAD_ASF_pg_table)

    mnr_only = pd.read_sql_query(SQL_present_in_MNR_only_new, con=pg_connection)
    if not mnr_only.empty:
        mnr_only.to_csv(outputpath + "2_ASF_MNR_present_only.csv", mode='w', index=False)
        print("mnr_only Not Empty")
    else:
        print("mnr_only is empty ")


def ASF_present_in_VAD_only(pg_connection, MNR_ASF_pg_table, VAD_ASF_pg_table, outputpath):
    SQL_present_in_vad_only_new = SQL_present_in_vad_only.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table) \
        .replace("{VAD_ASF_pg_table}", VAD_ASF_pg_table)

    VAD_only = pd.read_sql_query(SQL_present_in_vad_only_new, con=pg_connection)

    if not VAD_only.empty:
        VAD_only.to_csv(outputpath + "3_ASF_VAD_present_only.csv", mode='w', index=False)
        print("Not Empty")
    else:
        print("VAD_only is empty ")


def pgDBToCsvMnr(pg_connection, MNR_ASF_pg_table, outputpath):
    MNR_sql = """
            SELECT * FROM public."{MNR_ASF_pg_table}"

            """
    # MNR Connection
    MNR_sql_new = MNR_sql.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table)

    mnr_SQLdata = pd.read_sql_query(MNR_sql_new, con=pg_connection)

    mnr_schema = mnr_SQLdata.add_prefix("mnr_")

    mnr_schema.to_csv(outputpath + MNR_ASF_pg_table + ".csv", mode='w', index=False)


def pgDBToCsvVad(pg_connection, MNR_ASF_pg_table, outputpath):
    MNR_sql = """
            SELECT * FROM public."{MNR_ASF_pg_table}"

            """
    # MNR Connection
    MNR_sql_new = MNR_sql.replace("{MNR_ASF_pg_table}", MNR_ASF_pg_table)

    mnr_SQLdata = pd.read_sql_query(MNR_sql_new, con=pg_connection)

    mnr_schema = mnr_SQLdata.add_prefix("vad_")

    mnr_schema.to_csv(outputpath + MNR_ASF_pg_table + ".csv", mode='w', index=False)


##########################################################################
##########################################################################
######################### Input Area #####################################
##########################################################################
##########################################################################

# INPUT
inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/4_Adoc/fra_asf_sample_.csv'

outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/FRA/'

# MNR DB URL
EUR_SO_NAM_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
# LAM_MEA_OCE_SEA_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-006.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
LAM_MEA_OCE_SEA_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

# VAD DB URL
VAD_DB_Connections = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"
# Amedias
# VAD_DB_Connections = "postgresql://10.137.173.72/ggg?user=ggg&password=ok"

# schemas
MNR_schema_name = '_2022_09_007_lam_glp_glp'

VAD_schema_name = 'eur_fra_20220903_cw35'

# language_code
country_language_code = ['nl-Latn', 'fr-Latn', 'de-Latn']

# user weightage
mnr_hnr = 15
mnr_street_name = 20
mnr_place_name = 5
mnr_postal_code = 2.5
hnr_street_name = 25
integer_hnr = 5
integer_postal_code = 2.5
distance = 25

# Local DB connection

Host = "localhost"
DataBase = "postgres"
Port = "5433"
UserID = "postgres"
PassWord = "postgres"

# Local DB connection
engine = "postgresql://" + UserID + ":" + PassWord + "@" + Host + ":" + Port + "/" + DataBase

if __name__ == '__main__':

    # input files
    # WindowS
    # inputfilename = os.path.basename(inputcsv).split('\\')[-1].split('.')[0]
    # MAC
    inputfilename = inputcsv.split('/')[-1].split('.')[0]

    mnrfilename = "MNR_" + inputfilename + ".csv"
    vad_filename = "VAD_" + inputfilename + ".csv"

    # create log file
    logging.basicConfig(filename=outputpath + 'log' + inputfilename, level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s")
    # Script start time
    mnrStartTime = datetime.now()


    # Dump INPUT CSV to Postgres
    # input_csv_to_postgres(inputcsv, engine, inputfilename)
    # Create GeoDataFrame form CSV
    csv_gdb = create_points_from_input_csv(inputcsv)
    # # MNR calling
    mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdb, MNR_schema_name, LAM_MEA_OCE_SEA_MNR_DB_Connections, outputpath,
                                         mnrfilename)

    mnrEndTime = datetime.now()


    # MNR to CSV
    # pgDBToCsvMnr(engine, mnrfilename, outputpath)

    # file execution time calculation
    mnrTotalTime = mnrEndTime - mnrStartTime


    vadStartTime = datetime.now()

    # VAD calling
    for i in country_language_code:
        vad_csv_buffer_db_apt_fuzzy_matching(csv_gdb, VAD_schema_name, VAD_DB_Connections, outputpath, vad_filename, i)
    # # # VAD MAX
    # vad_parse_schema_data_postgres_max(engine, vad_filename)
    # print("vad_parse_schema_data_postgres_max..............Done !")
    #
    # # VAD to CSV
    # pgDBToCsvVad(engine, 'MAX_' + vad_filename, outputpath)

    # # Merge MNR, VAD
    # merge_mnr_vad_pg_table(engine, mnrfilename, vad_filename, outputpath)
    # print("merge_mnr_vad_pg_table..............Done !")
    #
    # ASF_present_in_MNR_only(engine, mnrfilename, vad_filename, outputpath)
    #
    # print("ASF_present_in_MNR_only..............Done !")
    #
    # ASF_present_in_VAD_only(engine, mnrfilename, vad_filename, outputpath)

    # end Time calculation
    vadEnd_time = datetime.now()

    vadTotalTime = vadEnd_time - vadStartTime

    # logfile message input
    logging.warning('MNR execution time:{},VAD execution time:{}'.format(mnrTotalTime, vadTotalTime))

    print("ASF_present_in_VAD_only..............Done !")

    print("-------------------------------------------")
    print("-------------------------------------------")
    print("-------------------------------------------")

    print("ASF tool run !")
