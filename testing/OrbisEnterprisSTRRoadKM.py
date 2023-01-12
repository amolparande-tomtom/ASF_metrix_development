import os

import pandas as pd
import psycopg2 as pg

diffNameTotal = """
                select a.highway,b.namedkm,a.TotalhighwayKm
                from
                    (select
                    highway,
                    cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as TotalhighwayKm 
                    from {schema}.planet_osm_line

                    where highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track')
                    group by highway) as a
                left join 
                    (select
                    highway,
                    cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as namedkm
                    from {schema}.planet_osm_line

                    where highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track')
                    and name is not null
                    group by highway) as b
                    on a.highway = b.highway

                    """


def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")


engine = pg.connect("dbname='ggg' user='ggg_ro' host='orbis3g-prod.openmap.maps.az.tt3.com' port='5432' "
                    "password='ggg_ro'")

listSchemaInDB = pd.read_sql('SELECT schema_name FROM information_schema.schemata;', con=engine)

# dfDiffNameUnnamed = pd.read_sql(diffNameUnnamed, con=engine)

country_list = """AND,AUT,BEL,BGR,BIH,BLR,CHE,CYP,CZE,DEU,DNK,ESP,FIN,FRA,FRO,GBR,GEO,GRC,HRV,HUN,IRL,ITA,LUX,LVA,MCO,MDA,MKD,NLD,NOR,POL,PRT,ROU,RUS,SRB,SVK,SVN,SWE,TUR,UKR,XKS,IND,ARG,BRA,CHL,COL,ECU,GLP,GUF,MTQ,PER,PRY,URY,ARE,BDI,BEN,BFA,BHR,CAF,COD,DZA,GAB,GNB,JOR,KWT,LBN,MAR,MDG,MLI,MOZ,NAM,NER,NGA,QAT,REU,SAU,SLE,ZAF,CAN,MEX,USA,AUS,FJI,NCL,NZL,BRN,HKG,IDN,KHM,LAO,MAC,MDV,MMR,MYS,PHL,SGP,THA,TWN,VNM"""
# country_list = """GAB,MDG"""
# String to List
newCountryList = country_list.split(",")

newCountryList.sort()

for indx, row in listSchemaInDB.iterrows():

    if row.schema_name.endswith("52"):
        region, country, date, week = row['schema_name'].split("_")
        if country.upper() in newCountryList:
            # update SQL fix
            newDiffNameTotal = diffNameTotal.replace("{schema}", row['schema_name'])
            dfDiffNameTotal = pd.read_sql(newDiffNameTotal, con=engine)
            dfDiffNameTotal['schema'] = row['schema_name']
            dfDiffNameTotal['country'] = country
            dfDiffNameTotal['namedkm'] = dfDiffNameTotal['namedkm'].fillna(0)
            dfDiffNameTotal['unnamedkm'] = dfDiffNameTotal['totalhighwaykm'] - dfDiffNameTotal['namedkm']
            column_names = ["highway", "namedkm", "unnamedkm", "totalhighwaykm", "schema", "country"]
            dfDiffNameTotal = dfDiffNameTotal.reindex(columns=column_names)
            csvFileWriter(dfDiffNameTotal, "OrbisEnterprisSTRRoadKM.csv", "/Users/parande/Documents/7_Adoc/1_SQL_Stat/")
