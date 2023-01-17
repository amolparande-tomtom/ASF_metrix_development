import os
import pandas as pd
import psycopg2 as pg
import re
from datetime import datetime

mnrStartTime = datetime.now()
engine = pg.connect("dbname='ggg' user='ggg_ro' host='orbis3g-prod.openmap.maps.az.tt3.com' port='5432' "
                    "password='ggg_ro'")


def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")


def streetNameRegexOrbisEnter(listSchemaInDB):
    streetName = []
    for i, c in listSchemaInDB.iterrows():
        for k, v in c['tagsjson'].items():
            r1 = 'name:[a-z]{2}-[A-Z]{1}[a-z]+'
            r2 = 'name:[a-z]+:[a-z]{2}-[A-z]{1}[a-z]+'
            if re.findall(r1, k) or re.findall(r2, k):
                print(c['osm_id'], k, ":", v, ":", c['name'], ":", c['way'])
                streetName.append(c)
                break
    stn = pd.DataFrame(streetName)
    return stn


dbSchemaSql = pd.read_sql('SELECT schema_name FROM information_schema.schemata;', con=engine)
regexSqlOld = """
        select
        osm_id,
        cast(st_length(way::geography) / 1000 AS DOUBLE PRECISION)as namedroad_km,
        cntry_code , 
        country, 
        name,
        tags,
        hstore_to_json(tags) as tagsjson,
        ST_AsText(way) as way
        from
        "{schema}".planet_osm_line
        where highway in('motorway','motorway_link','trunk','trunk_link',
        'primary','primary_link','secondary','secondary_link','tertiary',
        'tertiary_link','residential','unclassified','service','living_street',
        'track') and name is not null
        """
regexSql = """
        select
        osm_id,
        cast(st_length(way::geography)/1000 AS DOUBLE PRECISION) as nameroadkm,
        cntry_code, 
        name,
        tags,
        hstore_to_json(tags) as tagsjson
        from
        "{schema}".planet_osm_line
        where highway in('motorway','motorway_link','trunk','trunk_link',
        'primary','primary_link','secondary','secondary_link','tertiary',
        'tertiary_link','residential','unclassified','service','living_street',
        'track') and name is not null
        """

country_list = """AND,AUT,BEL,BGR,BIH,BLR,CHE,CYP,CZE,DEU,DNK,ESP,FIN,FRA,FRO,GBR,GEO,GRC,HRV,HUN,IRL,ITA,LUX,LVA,MCO,MDA,MKD,NLD,NOR,POL,PRT,ROU,RUS,SRB,SVK,SVN,SWE,TUR,UKR,XKS,IND,ARG,BRA,CHL,COL,ECU,GLP,GUF,MTQ,PER,PRY,URY,ARE,BDI,BEN,BFA,BHR,CAF,COD,DZA,GAB,GNB,JOR,KWT,LBN,MAR,MDG,MLI,MOZ,NAM,NER,NGA,QAT,REU,SAU,SLE,ZAF,CAN,MEX,USA,AUS,FJI,NCL,NZL,BRN,HKG,IDN,KHM,LAO,MAC,MDV,MMR,MYS,PHL,SGP,THA,TWN,VNM"""
# country_list = """GAB,MDG"""
# String to List
newCountryList = country_list.split(",")


newCountryList.sort()

outputpath = '/Users/parande/Documents/7_Adoc/4_OrbisEnterpriseOSMKM/1_98Countries/'
for indx, row in dbSchemaSql.iterrows():

    if row.schema_name.endswith("cw2"):
        region, country, date, week = row['schema_name'].split("_")
        if country.upper() in newCountryList:
            newRegexSql = regexSql.replace("{schema}", row['schema_name'])
            dfRegexSql = pd.read_sql(newRegexSql, con=engine)
            if not dfRegexSql.empty:
                # calling Regex Funcition
                streetName = []
                for i, c in dfRegexSql.iterrows():
                    for k, v in c['tagsjson'].items():
                        r1 = 'name:[a-z]{2}-[A-Z]{1}[a-z]+'
                        r2 = 'name:[a-z]+:[a-z]{2}-[A-z]{1}[a-z]+'
                        if re.findall(r1, k) or re.findall(r2, k):
                            streetName.append(c)
                            break
                pdDfRegExStn = pd.DataFrame(streetName)
                sumDat = sum(pdDfRegExStn["nameroadkm"])
                se = pd.Series(sumDat)
                df = pd.DataFrame(se)
                df.rename(columns={0: 'name'}, inplace=True)
                df['country_code'] = country
                csvFileWriter(df, 'RegExStreetOrbisEnterprise.csv', outputpath)
                print(country, "Done")

            else:
                author = country
                emptySr = pd.Series(author)
                dfEmpty = pd.DataFrame(emptySr)
                dfEmpty.rename(columns={0: 'emptyCountry'}, inplace=True)
                csvFileWriter(dfEmpty, 'EmptyRegExStreetOrbisEnterprise.csv', outputpath)


