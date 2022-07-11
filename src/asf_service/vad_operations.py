import os

import pandas as pd
from thefuzz import fuzz

from src.asf_service.mnr_details import MNR_details


def vad_csv_buffer_db_apt_fuzzy_matching(csv_gdf, vad_schema_name, output_path, vad_filename,
                                         language_code, conn):
    for i, r in csv_gdf.iterrows():
        add_header = True
        if os.path.exists(output_path + vad_filename):
            add_header = False
        # add_header = True
        # if i != 0:
        #     add_header = False
        schema_data = vad_query_for_one_record(r, vad_schema_name, language_code, conn)

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
        if schema_data.empty:
            print("VAD empty SR_ID", schema_data.SRID)
        if not schema_data.empty:
            print("VAD_SRID:", schema_data.SRID, "Done Processing for MNR" + r.searched_query)
            vad_parse_schema_data(schema_data, add_header, output_path, vad_filename)
            # if (schema_data.housenumber != 0 and schema_data.streetname != 'NODATA' and
            # schema_data.postalcode != 0 and schema_data.placename != 'NODATA'):


def vad_query_for_one_record(r, vad_schema_name, language_code, conn):
    buffer = r.provider_distance_orbis * 0.00001
    print("SR_ID:", r.SR_ID, "language_code:", language_code, "provider_distance_orbis:", r.provider_distance_orbis,
          "And", buffer)
    # print("Geometry:", r.geometry)
    new_vad_intersect_sql = MNR_details.Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}",
                                                                                    str(r.geometry)) \
        .replace("{schema_name_vad}", vad_schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer)) \
        .replace("{language_code}", language_code)
    schema_data = pd.read_sql_query(new_vad_intersect_sql, conn)
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_orbis'] = r.provider_distance_orbis
    schema_data['provider_distance_genesis'] = r.provider_distance_genesis
    return schema_data


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


def vad_parse_schema_data(schema_data, add_header, output_path, vad_filename):
    for indx, row in schema_data.iterrows():
        if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            if add_header:
                new_df.to_csv(output_path + vad_filename, mode='w', index=False)
                print("##############Printed DATA######################")
                add_header = False
            else:
                new_df.to_csv(output_path + vad_filename, mode='a', header=False, index=False)
                print("##############Printed DATA######################")
