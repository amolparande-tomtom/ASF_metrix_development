from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd

input_path = "/Users/parande/Documents/4_ASF_Metrix/0_input_csv/0_DEU/ZSHH_2022_normalized_final.gdb"

# create DataFrame
apt_source = gpd.read_file(input_path, driver='FileGDB', layer='ZSHH_APTS_2021_10')
apt_source.rename(columns={'HouseNumber': 'hsn', 'StreetName': 'street_name',
                           'PlaceName': 'place_name', 'PostalCode': 'postal_code'}, inplace=True)
# DB Connection URL   {db_connection_url_5 = "postgresql://username:password@localhost:5433/database"}
# db_connection_url_5 = "postgresql://username:password@localhost:5433/database"

engine = create_engine('postgresql://postgres:postgres@localhost:5433/asf')

# DataFream to Posgres

# if_exists : {'fail', 'replace', 'append'}
apt_source.to_sql('asf_apt', engine, if_exists='append')
