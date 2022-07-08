import pandas as pd
from shapely.geometry import Point
inputcsv = '/Users/parande/Documents/4_ASF_Metrix/0_input_csv/1_BEL/test_BEL_ASF_.csv'

df = pd.read_csv(inputcsv, encoding="utf-8")
# creating a geometry column
df['geometry'] = [Point(xy) for xy in zip(df['lon'], df['lat'])]


