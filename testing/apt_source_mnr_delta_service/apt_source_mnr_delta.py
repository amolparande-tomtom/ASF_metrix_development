import os

import geopandas as gpd
import pandas as pd
import psycopg2
from thefuzz import fuzz

delta_Buffer_ST_DWithin_mnr_osm_intersect_sql = """
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
                                                geographic_code.name as geographic_code,
                                                round(ST_Distance(ST_Transform(mnr_apt.geom,900913),ST_Transform(ST_GeomFromText('MULTIPOINT (2.792575000000056 48.372966000000076)', 4326),900913)))as Distance,
                                                ST_AsText(mnr_apt.geom) as geom
                                                from
                                                "{schema_name}".mnr_apt
                                                inner join "{schema_name}".mnr_apt2addressset
                                                on mnr_apt2addressset.apt_id = mnr_apt.feat_id
                                                inner join "{schema_name}".mnr_addressset
                                                using (addressset_id)
                                                inner join "{schema_name}".mnr_address
                                                on mnr_address.addressset_id = mnr_addressset.addressset_id
                                                inner join "{schema_name}".mnr_address_scheme
                                                using(address_scheme_id)
                                                left join "{schema_name}".mnr_postal_point
                                                on mnr_postal_point.feat_id = mnr_address.postal_code_id
                                                left join "{schema_name}".mnr_hsn
                                                on mnr_hsn.hsn_id in (mnr_address.house_number_id, mnr_address.last_house_number_id)
                                                left join "{schema_name}".mnr_name as building_name
                                                on building_name.name_id = mnr_address.building_name_id
                                                left join "{schema_name}".mnr_name as place_name
                                                on place_name.name_id = mnr_address.place_name_id
                                                left join "{schema_name}".mnr_name as state_province_name
                                                on state_province_name.name_id = mnr_address.state_province_name_id
                                                left join "{schema_name}".mnr_name as street_name
                                                on street_name.name_id = mnr_address.street_name_id
                                                left join "{schema_name}".mnr_name as geographic_code
                                                on geographic_code.name_id = mnr_address.geographic_code_id
                                                inner join "{schema_name}".mnr_apt_entrypoint
                                                on mnr_apt_entrypoint.apt_id = mnr_apt.feat_id
                                                inner join "{schema_name}".mnr_netw2admin_area
                                                using(netw_id)
                                                where mnr_apt_entrypoint.ep_type_postal
                                                and mnr_netw2admin_area.feat_type = 1111
                                                and ST_DWithin("{schema_name}".mnr_apt.geom, ST_GeomFromText('{point_geometry}',4326), {Buffer_in_Meter})
                                                """

# MNR DB URL
db_connection_url_5 = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"

# Read geopackage Input
input_path = '/Users/parande/Documents/5_APT_source_mnr_delta_service/1_Source/FRA_batch1_31052022.gpkg'
countries_gdf = gpd.read_file(input_path, layer='104', encoding="utf-8")
# postgres Connection
db_connection_MNR = psycopg2.connect(db_connection_url_5)

schema_name = 'eur_cas'

outputpath = '/Users/parande/Documents/5_APT_source_mnr_delta_service/2_Output/'
filename = 'MAX_FRA_batch1_Delta_Source_104.csv'

# Data Empty
empty_data = []

for i, r in countries_gdf.iterrows():
    add_header = True
    if os.path.exists(outputpath + filename):
        add_header = False
    # one meter

    buffer = 0.00001

    new_mnr_osm_intersect_sql = delta_Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}",
                                                                                      str(r.geometry)) \
        .replace("{schema_name}", schema_name) \
        .replace("{Buffer_in_Meter}", str(0.00001))
    mnr_schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, db_connection_MNR)

    #
    if mnr_schema_data.empty:
        empty_data.append(gpd.GeoDataFrame(r).transpose())
        # appended_data = pd.concat(empty_data).transpose()

    for n, c in mnr_schema_data.iterrows():
        # mnr_schema_data['HOUSE_NUMBER'] = " "
        # mnr_schema_data['STREET_NAME'] = " "
        # mnr_schema_data['POSTAL_CODE'] = 0
        # mnr_schema_data['MUN_CODE'] = 0

        mnr_schema_data.loc[n, 'I_UNIQUE_ID'] = r.UNIQUE_ID
        mnr_schema_data.loc[n, 'I_HOUSE_NUMBER'] = r.HOUSE_NUMBER
        mnr_schema_data.loc[n, 'I_STREET_NAME'] = r.STREET_NAME
        mnr_schema_data.loc[n, 'I_POSTAL_CODE'] = r.POSTAL_CODE
        mnr_schema_data.loc[n, 'I_MUN_CODE'] = r.MUN_CODE

        # Fuzzy Matching algo
        # House Number
        hnr_match = (fuzz.token_set_ratio(c.hsn, r.HOUSE_NUMBER))
        # Street Name
        stn_match = (fuzz.token_set_ratio(c.street_name, r.STREET_NAME))
        # Postal Code
        pcode_match = (fuzz.token_set_ratio(c.postal_code, r.POSTAL_CODE))
        # geographic_code
        geocode_match = (fuzz.token_set_ratio(c.geographic_code, r.MUN_CODE))

        # Fuzzy attribute
        mnr_schema_data.loc[n, 'hnr_match'] = hnr_match
        mnr_schema_data.loc[n, 'street_name_match'] = stn_match
        mnr_schema_data.loc[n, 'postal_code_name_match'] = pcode_match
        mnr_schema_data.loc[n, 'geographic_code_name_match'] = geocode_match

        # individual percentage calculation
        mnr_schema_data.loc[n, 'hnr_match%'] = (mnr_schema_data['hnr_match'][n] / 100)
        mnr_schema_data.loc[n, 'street_name_match%'] = (mnr_schema_data['street_name_match'][n] / 100)
        mnr_schema_data.loc[n, 'place_name_match%'] = (mnr_schema_data['postal_code_name_match'][n] / 100)
        mnr_schema_data.loc[n, 'geographic_code_name_match%'] = (
                mnr_schema_data['geographic_code_name_match'][n] / 100)

        # Addition
        mnr_schema_data.loc[n, 'Stats_Result'] = (mnr_schema_data['hnr_match%'][n] +
                                                  mnr_schema_data['street_name_match%'][n] +
                                                  mnr_schema_data['place_name_match%'][n] +
                                                  mnr_schema_data['geographic_code_name_match%'][n])
        # Percentage
        mnr_schema_data.loc[n, 'Percentage'] = ((mnr_schema_data['Stats_Result'][n] / 4) * 100)
        # change datatype to integer
        # mnr_schema_data = mnr_schema_data.astype({'hnr_match': int,
        #                         'street_name_match': int,
        #                         'postal_code_name_match': int,
        #                         'geographic_code_name_match': int,
        #                         'hnr_match%': int,
        #                         'street_name_match%': int,
        #                         'place_name_match%': int,
        #                         'geographic_code_name_match%': int,
        #                         'Stats_Result': int,
        #                         'Percentage': int})
    if not mnr_schema_data.empty:
        print("Processing UNIQUE_ID:", r.UNIQUE_ID)
        group_max = mnr_schema_data.groupby('I_UNIQUE_ID')['Percentage'].max()
        pd_group_max = pd.DataFrame(group_max)
        mx_apt_delta = pd.merge(mnr_schema_data, pd_group_max, on=['I_UNIQUE_ID', 'Percentage'])

        for indx, row in mx_apt_delta.iterrows():
            if row.Percentage != 100:
                if add_header:
                    mx_apt_delta.to_csv(outputpath + filename, mode='w', index=False)
                    add_header = False
                else:
                    mx_apt_delta.to_csv(outputpath + filename, mode='a', header=False, index=False)
    else:
        (print("empty values:", r.UNIQUE_ID))

# create Geopakage Delta_or_Missing_APT
if len(empty_data) > 0:
    emptyDataFrame = pd.concat(empty_data)
    missing_data = gpd.GeoDataFrame(emptyDataFrame, geometry=emptyDataFrame.geometry, crs="EPSG:4326")
    missing_data.to_file(outputpath + 'MAX_FRA_batch1_Delta_Source_104.gpkg', layer='cities', driver="GPKG")
