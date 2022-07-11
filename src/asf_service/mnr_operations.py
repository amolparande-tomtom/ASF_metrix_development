import os

import pandas as pd
from thefuzz import fuzz

from src.asf_service.mnr_details import MNR_details


def mnr_csv_buffer_db_apt_fuzzy_matching(csv_gdf, mnr_schema_name, output_path, mnr_filename, mnr_conn):
    """
    """
    for i, r in csv_gdf.iterrows():
        add_header = True
        if os.path.exists(output_path + mnr_filename):
            add_header = False
        # add_header = True
        # if i != 0:
        #     add_header = False
        schema_data = mnr_query_for_one_record(r, mnr_schema_name, mnr_conn)

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
        if schema_data.empty:
            print("MNR empty SR_ID", schema_data.SRID)
        if not schema_data.empty:
            print("MNR_SRID:", schema_data.SRID, "Done Processing for MNR" + r.searched_query)
            # Writing
            mnr_parse_schema_data(add_header, schema_data, output_path, mnr_filename)


def mnr_query_for_one_record(r, mnr_filename, mnr_conn):
    buffer = r.provider_distance_genesis * 0.00001
    # print("SR_ID:", r.SR_ID, "distance_genesis:", r.provider_distance_genesis, "And", buffer)
    # print("Geometry:", r.geometry)
    new_mnr_osm_intersect_sql = MNR_details \
        .Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name}", mnr_filename) \
        .replace("{Buffer_in_Meter}", str(buffer))
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, mnr_conn)
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    return schema_data


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


def mnr_parse_schema_data(add_header, schema_data, output_path, mnr_schema_name):
    for indx, row in schema_data.iterrows():
        if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            if add_header:
                new_df.to_csv(output_path + mnr_schema_name, mode='w', index=False)
                add_header = False
            else:
                new_df.to_csv(output_path + mnr_schema_name, mode='a', header=False, index=False)
