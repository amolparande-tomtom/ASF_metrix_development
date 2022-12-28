import pandas as pd
import psycopg2
import re
import psycopg2 as pg


engine = pg.connect("dbname='ggg' user='ggg_ro' host='3g-prod.openmap.maps.az.tt3.com' port='5432' "
                    "password='ggg_ro'")


listSchemaInDB = pd.read_sql('SELECT schema_name FROM information_schema.schemata;', con=engine)

def listSchema():
    """
    :input : DataFrame
    :return: list Schema
    """
    tagretSchema = []
    for i, row in listSchemaInDB.iterrows():
        # ending with 2 or 1 digit
        if (row['schema_name'][-2:].isdigit() or row['schema_name'][-1:].isdigit()):
            tagretSchema.append(row['schema_name'])
    return tagretSchema


def uniqCountyCode(dataSchema):
    """
    : list schema
    :return: unique country code
    """
    countyCode = set()
    for c in dataSchema:
        countyCode.add(c.split('_')[1])
    return countyCode

def latestSchema(isoCountryCode):
    """
    :param isoCountryCode: List
    :return: latest Schema
    """
    finalSchema = []
    for iso in isoCountryCode:
        uniqe = dict()
        for Schema in enumerate(dataSchema):
            if iso in Schema[1]:
                # num = int(Schema[1][-2:])
                uniqe[Schema[1]] = int(Schema[1][-2:])
                # get max week schema
                r = zip(uniqe.values(), uniqe.keys())
        maxvalue = max(zip(uniqe.values(), uniqe.keys()))[1]
        finalSchema.append(maxvalue)
    return finalSchema

dataSchema = listSchema()

# unique country code
isoCountryCode = uniqCountyCode(dataSchema)
finalSchema = latestSchema(isoCountryCode)


# pattern = "\w{3}_"+f"{country}"+"_\d{8}_\w{2}\d{2}"
# findStr = re.findall(pattern, str(newlatestSchema))







