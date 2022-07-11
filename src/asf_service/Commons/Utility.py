import gpd as gpd
import pandas as pd
from shapely.geometry import Point


class Utility:

    @staticmethod
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
