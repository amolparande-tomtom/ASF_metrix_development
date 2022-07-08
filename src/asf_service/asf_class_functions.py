import os

import pandas as pd
import geopandas as gpd
import psycopg2
from shapely.geometry import Point
from thefuzz import fuzz

from src.asf_service import mnr_details


class AsfMnrProcess:
    def __init__(self, mnr_db_url, vad_db_url, csv_path, mnr_schema_name, vad_schema_name,
                 outputpath, mnr_filename, vad_filename, language_code):
        self.vad_db_url = mnr_db_url
        self.vad_db_url = vad_db_url
        self.csv_path = csv_path
        self.outputpath = outputpath
        self.mnr_filename = mnr_filename
        self.vad_filename = vad_filename
        self.mnr_schema_name = mnr_schema_name
        self.vad_schema_name = vad_schema_name
        self.language_code = language_code

    def postgres_db_connection(self, db_url):
        """
        :param db_url: Postgres Server
        :return: DB Connection
        """
        try:
            return psycopg2.connect(db_url)
        except Exception as error:
            print("Oops! An exception has occured:", error)
            print("Exception TYPE:", type(error))

    def create_points_from_input_csv(self, csv_path):
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

    def mnr_csv_buffer_db_apt_fuzzy_matching(self, csv_gdf, mnr_schema_name, mnr_db_url, outputpath, mnr_filename):
        """


        """
        for i, r in csv_gdf.iterrows():
            add_header = True
            if os.path.exists(outputpath + mnr_filename):
                add_header = False
            # add_header = True
            # if i != 0:
            #     add_header = False
            schema_data = self.mnr_query_for_one_record(mnr_db_url, r, mnr_schema_name)

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
            self.mnr_calculate_fuzzy_values(r, schema_data)

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
                self.mnr_parse_schema_data(add_header, schema_data, outputpath, mnr_filename)

    def mnr_query_for_one_record(self, mnr_db_url, r, mnr_filename):
        buffer = r.provider_distance_genesis * 0.00001
        # print("SR_ID:", r.SR_ID, "distance_genesis:", r.provider_distance_genesis, "And", buffer)
        # print("Geometry:", r.geometry)
        new_mnr_osm_intersect_sql = mnr_details.MNR_details.Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
            .replace("{schema_name}", mnr_filename) \
            .replace("{Buffer_in_Meter}", str(buffer))
        schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, self.postgres_db_connection(mnr_db_url))
        schema_data['searched_query'] = r.searched_query
        schema_data['geometry'] = r.geometry
        schema_data['SRID'] = r.SR_ID
        schema_data['provider_distance_orbis'] = r.provider_distance_orbis
        schema_data['provider_distance_genesis'] = r.provider_distance_genesis
        return schema_data

    def mnr_calculate_fuzzy_values(self, r, schema_data):
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

    def mnr_parse_schema_data(self, add_header, schema_data, outputpath, mnr_schema_name):
        for indx, row in schema_data.iterrows():
            if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
                new_df = pd.DataFrame(row).transpose()
                if add_header:
                    new_df.to_csv(outputpath + mnr_schema_name, mode='w', index=False)
                    add_header = False
                else:
                    new_df.to_csv(outputpath + mnr_schema_name, mode='a', header=False, index=False)

    def vad_csv_buffer_db_apt_fuzzy_matching(self, csv_gdf, vad_schema_name, vad_db_url, outputpath, vad_filename, language_code):
        for i, r in csv_gdf.iterrows():
            add_header = True
            if os.path.exists(outputpath + vad_filename):
                add_header = False
            # add_header = True
            # if i != 0:
            #     add_header = False
            schema_data = self.vad_query_for_one_record(vad_db_url, r, vad_schema_name, language_code)

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

            self.vad_calculate_fuzzy_values(r, schema_data)
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
                self.vad_parse_schema_data(schema_data, add_header, outputpath, filename)
                # if (schema_data.housenumber != 0 and schema_data.streetname != 'NODATA' and schema_data.postalcode != 0 and schema_data.placename != 'NODATA'):

    def vad_query_for_one_record(self, vad_db_url, r, vad_schema_name, language_code):
        buffer = r.provider_distance_orbis * 0.00001
        print("SR_ID:", r.SR_ID, "language_code:", language_code, "provider_distance_orbis:", r.provider_distance_orbis,
              "And", buffer)
        # print("Geometry:", r.geometry)
        new_VAD_intersect_sql = mnr_details.MNR_details.Buffer_ST_DWithin_VAD_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
            .replace("{schema_name_vad}", vad_schema_name) \
            .replace("{Buffer_in_Meter}", str(buffer)) \
            .replace("{language_code}", language_code)
        schema_data = pd.read_sql_query(new_VAD_intersect_sql, self.postgres_db_connection(vad_db_url))
        schema_data['searched_query'] = r.searched_query
        schema_data['geometry'] = r.geometry
        schema_data['SRID'] = r.SR_ID
        schema_data['provider_distance_orbis'] = r.provider_distance_orbis
        schema_data['provider_distance_genesis'] = r.provider_distance_genesis
        return schema_data

    def vad_calculate_fuzzy_values(self, r, schema_data):
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

    def vad_parse_schema_data(self,schema_data, add_header, outputpath, vad_filename):
        for indx, row in schema_data.iterrows():
            if row.housenumber != 0 or row.streetname != 'NODATA' or row.postalcode != 0 or row.placename != 'NODATA':
                new_df = pd.DataFrame(row).transpose()
                if add_header:
                    new_df.to_csv(outputpath + vad_filename, mode='w', index=False)
                    print("##############Printed DATA######################")
                    add_header = False
                else:
                    new_df.to_csv(outputpath + vad_filename, mode='a', header=False, index=False)
                    print("##############Printed DATA######################")



