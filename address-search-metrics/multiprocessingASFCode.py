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
from multiprocessing import Pool

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


def create_points_from_input_csv(csv_path, schema_name, db_url, outputpath, filename):
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
    df['schema_name'] = schema_name
    df['db_url'] = db_url
    df['outputpath'] = outputpath
    df['filename'] = filename

    # Creating a Geographic data frame
    return gpd.GeoDataFrame(df, crs="EPSG:4326")


def create_points_from_input_csv_VAD(csv_path, schema_name, db_url, outputpath, filename):
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
    df['schema_name'] = schema_name
    df['db_url'] = db_url
    df['outputpath'] = outputpath
    df['filename'] = filename

    # Creating a Geographic data frame
    return gpd.GeoDataFrame(df, crs="EPSG:4326")


def mnr_csv_buffer_db_apt_fuzzy_matching(r):
    schema_data = mnr_query_for_one_record(r.db_url, r, r.schema_name)

    # fuzzy MNR function
    mnr_calculate_fuzzy_values(r, schema_data, mnr_hnr, mnr_street_name, mnr_place_name, mnr_postal_code,
                               hnr_street_name, integer_hnr, integer_postal_code, distance)

    if schema_data.empty:
        print("Data Empty")

    # Writing CSV MNR function
    if not schema_data.empty:
        print("MNR_SRID:", schema_data.SRID)

        # Writing CSV
        mnr_parse_schema_data(schema_data, r.outputpath, r.filename)


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


def mnr_parse_schema_data(schema_data, outputpath, filename):
    column_names = ["SRID", "country", "feat_id", "lang_code", "iso_lang_code", "notation",
                    "iso_script", "state_province_name", "place_name", "street_name", "postal_code",
                    "building_name", "hsn", "distance", "geom", "searched_query", "geometry",
                    "provider_distance_orbis", "provider_distance_genesis", "hnr_match",
                    "street_name_match", "place_name_match", "postal_code_name_match",
                    "hnr_street_name_match", "new_hnr_match", "new_postal_code_name_match",
                    "distance_match", "Percentage"]
    schema_data = schema_data.reindex(columns=column_names)
    # Adding count each SRID group
    schema_data["SRID_count"] = schema_data.SRID.size
    # Dump Pre Data
    csvFileWriter(schema_data, "MNR_intersection_" + filename + ".csv", outputpath)
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
            csvFileWriter(distance_mx_apt_delta, "MNR_MAX_" + filename + ".csv", outputpath)

        else:
            # Writing to Postgres
            distance_mx_apt_delta = distance_mx_apt_delta.drop(['geometry'], axis=1)
            # distance_mx_apt_delta.to_sql(filename, engine, if_exists='append')
            # Writing to CSV
            csvFileWriter(distance_mx_apt_delta, "MNR_MAX_" + filename + ".csv", outputpath)


    else:
        for indx, row in mx_apt_delta.iterrows():
            if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
                new_df = pd.DataFrame(row).transpose()

                # Writing to Postgres
                new_df = new_df.drop(['geometry'], axis=1)
                # new_df.to_sql(filename, engine, if_exists='append')

                # Writing to CSV
                csvFileWriter(new_df, "MNR_MAX_" + filename + ".csv", outputpath)


def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")

def vad_csv_buffer_db_apt_fuzzy_matching(r, language_code):

    schema_data = vad_query_for_one_record(r.db_url, r, language_code)
    # schema_data['language_code'] = language_code
    # fuzzy VAD function
    if not schema_data.empty:
        vad_calculate_fuzzy_values(r, schema_data)

        vad_parse_schema_data(schema_data, r.outputpath, r.filename)

        # # Writing csv Empty ASF
        # if schema_data.empty:
        #     print("Data Empty")
        #
        # if not schema_data.empty:
        #     # Writing CSV
        #     vad_parse_schema_data(schema_data, r.outputpath, r.filename)
    else:
        print("Errorr Empty")



def vad_query_for_one_record(db_url,r,language_code):
    DataFrame = []
    # This Need to Be create Parameter
    # language_code
    # language_code = ['nl-Latn', 'fr-Latn', 'de-Latn']
    for l in language_code:
        buffer = r.provider_distance_genesis * 0.00001

        new_VAD_intersect_sql = Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
            .replace("{schema_name_vad}", r.schema_name) \
            .replace("{Buffer_in_Meter}", str(buffer)) \
            .replace("{language_code}", l)

        schemadata = pd.read_sql_query(new_VAD_intersect_sql, postgres_db_connection(db_url))
        schemadata['searched_query'] = r.searched_query
        schemadata['geometry'] = r.geometry
        schemadata['SRID'] = r.SR_ID
        schemadata['provider_distance_orbis'] = r.provider_distance_orbis
        schemadata['provider_distance_genesis'] = r.provider_distance_genesis
        # replace "distance" less than 1 value with 1
        schemadata.loc[schemadata['distance'] < 1, 'distance'] = 1

        msk = schemadata[['housenumber', 'streetname', 'postalcode', 'placename']].notnull().all(axis=1)
        newSchemaData = schemadata[msk]
        if not newSchemaData.empty:
            DataFrame.append(newSchemaData)
    if len(DataFrame) != 0:
        return pd.concat(DataFrame)
    else:
        return pd.DataFrame(DataFrame)


def vad_query_for_one_recordold(db_url, r,language_code):
    buffer = r.provider_distance_orbis * 0.00001
    # print("SR_ID:", r.SR_ID, "language_code:", language_code, "provider_distance_orbis:", r.provider_distance_orbis,
    #       "And", buffer)
    # print("Geometry:", r.geometry)
    new_VAD_intersect_sql = Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name_vad}", r.schema_name) \
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


def vad_calculate_fuzzy_values(r, schema_data):
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



def vad_parse_schema_data(schema_data, outputpath, filename):
    schema_data["SRID_count"] = schema_data.SRID.size
    # VAD_intersection CSV Dump Area
    csvFileWriter(schema_data, "VAD_intersection_" + filename + ".csv", outputpath)

    group_max = schema_data.groupby('SRID')['Percentage'].max()
    pd_group_max = pd.DataFrame(group_max)
    mx_apt_delta = pd.merge(schema_data, pd_group_max, on=['SRID', 'Percentage']).sort_values('SRID')

    if mx_apt_delta['Percentage'].value_counts().values.max() > 1:
            min_distance = mx_apt_delta['distance'].min()
            distance_mx_apt_delta = mx_apt_delta.loc[mx_apt_delta['distance'] == min_distance]
            # get First Values
            if distance_mx_apt_delta['Percentage'].value_counts().values.max() != 1:
                distance_mx_apt_delta = distance_mx_apt_delta.head(1)
                # Writing to CSV
                print("##############Printed VAD DATA######################")
                print(distance_mx_apt_delta.SRID)
                csvFileWriter(distance_mx_apt_delta, "VAD_MAX_" + filename + ".csv", outputpath)

            else:
                # Writing to CSV
                print("##############Printed VAD DATA######################")
                print(distance_mx_apt_delta.SRID)
                csvFileWriter(distance_mx_apt_delta, "VAD_MAX_" + filename + ".csv", outputpath)
    else:
        for indx, row in mx_apt_delta.iterrows():
            if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
                new_df = pd.DataFrame(row).transpose()
                print("##############Printed VAD DATA######################")
                print(new_df.SRID)
                csvFileWriter(new_df, "VAD_MAX_" + filename + ".csv", outputpath)


def vad_parse_schema_dataOLD(schema_data, outputpath, filename):
    schema_data["SRID_count"] = schema_data.SRID.size
    for indx, row in schema_data.iterrows():
        if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            # Writing to Postgres
            new_df = new_df.drop(['geometry'], axis=1)
            # new_df.to_sql(filename, engine, if_exists='append')
            print("##############Printed DATA######################")
            print(schema_data.SRID)
            # Writing to CSV
            csvFileWriter(new_df, "VAD_intersection_" + filename + ".csv", outputpath)
        else:
            print(row.SRID, "Blank rows")


def vad_parse_schema_data_csv_max(outputpath, fileNameWindowS, inputfilename):
    path = outputpath + fileNameWindowS
    schema_data = pd.read_csv(path, encoding="utf-8")
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
                # Writing to CSV
                csvFileWriter(distance_mx_apt_delta, "VAD_MAX_" + inputfilename + ".csv", outputpath)
            else:

                # Writing to CSV
                csvFileWriter(distance_mx_apt_delta, "VAD_MAX_" + inputfilename + ".csv", outputpath)

        else:

            # Writing to CSV
            csvFileWriter(mx_apt_delta_new, "VAD_MAX_" + inputfilename + ".csv", outputpath)


##########################################################################
##########################################################################
######################### Input Area #####################################
##########################################################################
##########################################################################

# INPUT
inputcsv = '/Users/parande/Documents/4_ASF_Metrix/6_Multiprocessing/0_input/bra_asf_sample_.csv'

outputpath = '/Users/parande/Documents/4_ASF_Metrix/6_Multiprocessing/1_Output/'

# MNR DB URL
EUR_SO_NAM_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
# LAM_MEA_OCE_SEA_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-006.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
LAM_MEA_OCE_SEA_MNR_DB_Connections = "postgresql://caprod-cpp-pgmnr-002.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

# VAD DB URL
# VAD_DB_Connections = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"
# Amedias
VAD_DB_Connections = "postgresql://10.137.173.72/ggg?user=ggg&password=ok"

# schemas
MNR_schema_name = '_2022_09_009_lam_bra_bra'

VAD_schema_name = 'ade_amedias_0_22_41_sam_bra'

# language_code
country_language_code = ['pt-Latn', 'es-Latn']

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
# engine = "postgresql://" + UserID + ":" + PassWord + "@" + Host + ":" + Port + "/" + DataBase

if __name__ == '__main__':
    # input files
    # WindowS
    # inputfilename = os.path.basename(inputcsv).split('\\')[-1].split('.')[0]
    # MAC
    inputfilename = inputcsv.split('/')[-1].split('.')[0]

    mnrfilename = "MNR_" + inputfilename + ".csv"
    vad_filename = "VAD_" + inputfilename + ".csv"

    # create log file
    logging.basicConfig(filename=outputpath + 'log' + inputfilename + '.txt', level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s")

    ################## MNR calling ######################
    # Script start time
    mnrStartTime = datetime.now()

    # MNR Create GeoDataFrame form CSV
    # csv_gdbMNR = create_points_from_input_csv(inputcsv, MNR_schema_name, LAM_MEA_OCE_SEA_MNR_DB_Connections, outputpath,
    #                                           inputfilename)
    #
    # code = [r for i, r in csv_gdbMNR.iterrows()]
    # p = Pool()
    # result = p.map(mnr_csv_buffer_db_apt_fuzzy_matching, code)
    # p.close()
    # p.join()

    mnrEndTime = datetime.now()

    # file execution time calculation
    mnrTotalTime = mnrEndTime - mnrStartTime

    ################## VAD calling ######################

    # Multiprocessing VAD
    vadStartTime = datetime.now()

    # VAD Point create calling

    csv_gdbVAD = create_points_from_input_csv_VAD(inputcsv, VAD_schema_name, VAD_DB_Connections,
                                                  outputpath, inputfilename)
    para = []

    for i, r in csv_gdbVAD.iterrows():
        para.append([r, country_language_code])

        # vad_csv_buffer_db_apt_fuzzy_matching(r, country_language_code)

    pvad = Pool()
    resultVAD = pvad.starmap(vad_csv_buffer_db_apt_fuzzy_matching, para)
    pvad.close()
    pvad.join()
    # VAD MAX

    fileNameWindowS = "VAD_intersection_" + inputfilename + ".csv"

    # path = outputpath + fileNameWindowS

    # end Time calculation
    vadEnd_time = datetime.now()

    vadTotalTime = vadEnd_time - vadStartTime

    # logfile message input
    logging.warning(
        '\n 1. Input CSV Path : {} \n 2. output CSV Path : {} \n 3. MNR Schema Name : {} \n 4. VAD Schema Name : {}'.format(
            inputcsv, outputpath, MNR_schema_name, VAD_schema_name))

    logging.warning('MNR execution time:{},VAD execution time:{}'.format(mnrTotalTime, vadTotalTime))

    print("ASF_present_in_VAD_only..............Done !")

    print("-------------------------------------------")
    print("-------------------------------------------")
    print("-------------------------------------------")

    print("ASF tool run !")
