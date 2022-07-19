import os
import psycopg2
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from thefuzz import fuzz

Buffer_ST_DWithin_mnr_osm_intersect_sql = """
                                        select          
                                        hsn,
                                        street_name,
                                        place_name ,
                                        postal_code                
                                        from
                                        "{schema_name}"."{table_name}"          
                                        where                 
                                        ST_DWithin("{schema_name}"."{table_name}".geom, ST_GeomFromText('{point_geometry}',4326), {Buffer_in_Meter})
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
    df['provider_distance_source'] = df['provider_distance_source'].fillna(30.0)
    # Creating a Geographic data frame
    return gpd.GeoDataFrame(df, crs="EPSG:4326")


def source_csv_buffer_db_apt_fuzzy_matching(csv_gdf, schema_name, db_url, outputpath, filename, table_name):
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
        schema_data = mnr_query_for_one_record(db_url, r, schema_name, table_name)

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
            mnr_parse_schema_data(add_header, schema_data, outputpath, filename)


def mnr_query_for_one_record(db_url, r, schema_name, table_name):
    buffer = r.provider_distance_source * 0.00001

    new_mnr_osm_intersect_sql = Buffer_ST_DWithin_mnr_osm_intersect_sql.replace("{point_geometry}", str(r.geometry)) \
        .replace("{schema_name}", schema_name) \
        .replace("{Buffer_in_Meter}", str(buffer)).replace("{table_name}", table_name)
    schema_data = pd.read_sql_query(new_mnr_osm_intersect_sql, postgres_db_connection(db_url))
    schema_data['searched_query'] = r.searched_query
    schema_data['geometry'] = r.geometry
    schema_data['SRID'] = r.SR_ID
    schema_data['provider_distance_source'] = r.provider_distance_source
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


def mnr_parse_schema_data(add_header, schema_data, outputpath, filename):
    for indx, row in schema_data.iterrows():
        if row.hsn != 0 or row.street_name != 'NODATA' or row.postal_code != 0 or row.place_name != 'NODATA':
            new_df = pd.DataFrame(row).transpose()
            if add_header:
                new_df.to_csv(outputpath + filename, mode='w', index=False)
                add_header = False
            else:
                new_df.to_csv(outputpath + filename, mode='a', header=False, index=False)


inputcsv = '/Users/parande/Documents/5_APT_source_mnr_delta_service/2_Germany/0_Input/api_pbf_asf_genesis_logs.csv'
sourceDB_C = "postgresql://caprod-cpp-pgmnr-005.flatns.net/mnr?user=mnr_ro&password=mnr_ro"
schema_name = "public"
outputpath = '/Users/parande/Documents/5_APT_source_mnr_delta_service/2_Germany/2_output/'
table_name = 'deu_source'
filename = "source_check_DUE.csv"

csv_gdb = create_points_from_input_csv(inputcsv)

source_csv_buffer_db_apt_fuzzy_matching(csv_gdb, schema_name, sourceDB_C, outputpath, filename, table_name)
