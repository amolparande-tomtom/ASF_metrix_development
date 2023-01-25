import re
import unidecode
from thefuzz import fuzz

string = "UL.   STOKR OTK I5O LSZTYN 11041 POL"
sting2 = ' STOKR OTK LSZTYN'
searched_query = 'ULICA WITOLDA MAŁCUŻYŃSKIEGO 17 JELENIA GORA 58-500 POL'

hsn = '17'
postal_code = '58-506'

# remove Alphabet and remove space start and end
searched_query = (re.sub("[a-zA-Z]", '', unidecode.unidecode(searched_query))).strip()

new_hnr_mt = fuzz.token_set_ratio(unidecode.unidecode(hsn),unidecode.unidecode(searched_query))

new_pcode_mt = fuzz.token_set_ratio(unidecode.unidecode(postal_code),unidecode.unidecode(searched_query))


# Statistics calculation


print(searched_query)

print(new_hnr_mt)
print(new_pcode_mt)
