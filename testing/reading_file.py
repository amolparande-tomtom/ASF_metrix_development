import pandas as pd
import geopandas as gpd

from thefuzz import fuzz



searched_query = '100 Heather Brook Circle, 28390, Spring Lake, US'

hnr = '100'
street_name ='Heather Brook Cir'
place_name = 'Spring Lake'
postal_code = '28390'
print(fuzz.ratio(place_name, searched_query))
print(fuzz.partial_ratio(place_name, searched_query))
print(fuzz.token_set_ratio(place_name, searched_query))
print(fuzz.token_sort_ratio(place_name, searched_query))















# if substr is not None and substr.lower() in (c.lower()):
#     print("Yes Found !")
#     print(c.index(substr))
# else:
#     print("No Match Found")
