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



# INPUT
inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/4_Adoc/fra_asf_sample_.csv'

outputpath = '/Users/parande/Documents/4_ASF_Metrix/2_output/FRA/'

VAD_DB_Connections = "postgresql://vad3g-prod.openmap.maps.az.tt3.com/ggg?user=ggg_ro&password=ggg_ro"


VAD_schema_name = 'eur_fra_20221008_cw40'

# MAC
inputfilename = inputcsv.split('/')[-1].split('.')[0]




csv_gdbVAD = create_points_from_input_csv_VAD(inputcsv, VAD_schema_name, VAD_DB_Connections,
                                              outputpath, inputfilename)

country_language_code = ['nl-Latn', 'fr-Latn', 'de-Latn']

para = []


for i, r in csv_gdbVAD.iterrows():
    para.append([r, country_language_code])