import os

import pandas as pd
import psycopg2 as pg

highWayRoadStreetN = """
                    select
                    highway,
                    sum(st_length(way::geography)) / 1000 as NamedRoad_km
                    from asa_ind_20221217_cw50.planet_osm_line
                    where name is not null
                    and (highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track') or route in ('ferry'))
                    group by highway
                    having highway is not null
                    order by highway
                """

allHighWayRoad = """
                    select 
                    sum(st_length(way::geography)) / 1000 as ALLRoad_km
                    from asa_ind_20221217_cw50.planet_osm_line
                    where 
                    (highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track') or route in ('ferry'))
                    """

diffNameUnnamedold = """
            select
            highway,
            cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as NamedRoad_km,

            cast((select 
            cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION) 
            from {schema}.planet_osm_line
            where 
            (highway in('motorway','motorway_link','trunk','trunk_link',
            'primary','primary_link','secondary','secondary_link','tertiary',
            'tertiary_link','residential','unclassified','service','living_street',
            'track') or route in ('ferry')))- (sum(st_length(way::geography)) / 1000 )AS DOUBLE PRECISION)  as UnNamedRoad_km,

            (select 
            cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION) 
            from {schema}.planet_osm_line
            where 
            (highway in('motorway','motorway_link','trunk','trunk_link',
            'primary','primary_link','secondary','secondary_link','tertiary',
            'tertiary_link','residential','unclassified','service','living_street',
            'track') or route in ('ferry')))as ALLRoad_km

        from {schema}.planet_osm_line
        where name is not null
        and (highway in('motorway','motorway_link','trunk','trunk_link',
        'primary','primary_link','secondary','secondary_link','tertiary',
        'tertiary_link','residential','unclassified','service','living_street',
        'track') or route in ('ferry'))
        group by highway
        having highway is not null
        order by NamedRoad_km
        """

allCoverageKM = """
                with                  
                namedRoad
                as 
                (
                select highway,
                cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as namedroad_km,
                cntry_code,
                country 
                from 
                {schema}.planet_osm_line
                where name is not null
                group by highway, cntry_code,country 
                order by namedroad_km desc ),
                
                unnamedRoad 
                as
                (select highway,
                cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as unnamedroad_km
                from 
                {schema}.planet_osm_line
                where name isnull
                group by highway
                order by unnamedroad_km desc )
                              
                select nr.highway,nr.namedroad_km, unr.unnamedroad_km, (nr.namedroad_km + unr.unnamedroad_km) as Total_km, unr.highway as unnroad_h
                from namedRoad nr
                full outer JOIN unnamedRoad unr
                ON nr.highway = unr.highway;

                """

diffNameUnnamed = """
                    with  
                    namedRoad
                    as 
                    (select
                    highway,
                    cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as NamedRoad_km
                    from {schema}.planet_osm_line
                    where name is not null
                    and (highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track') or route in ('ferry'))
                    group by highway
                    having highway is not null
                    order by NamedRoad_km),
                    
                    unnamedRoad 
                    as
                    (select 
                    highway,
                    cast(sum(st_length(way::geography)) / 1000 AS DOUBLE PRECISION)as unNamedRoad_km
                    
                    from {schema}.planet_osm_line
                    where 
                    (highway in('motorway','motorway_link','trunk','trunk_link',
                    'primary','primary_link','secondary','secondary_link','tertiary',
                    'tertiary_link','residential','unclassified','service','living_street',
                    'track') or route in ('ferry')) and "name" isnull 
                    group by highway)
                    
                    
                    select nr.highway,nr.namedroad_km, unr.unnamedroad_km,(nr.namedroad_km + unr.unnamedroad_km) as total_km, unr.highway as unnroad
                    from namedRoad nr
                    full outer JOIN unnamedRoad unr
                    ON nr.highway = unr.highway;
                    
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
            # Named and UnNamed road Coverage
            newDiffNameUnnamed = diffNameUnnamed.replace("{schema}", row['schema_name'])
            dfDiffNameUnnamed = pd.read_sql(newDiffNameUnnamed, con=engine)
            dfDiffNameUnnamed['schema'] = row['schema_name']
            dfDiffNameUnnamed['country'] = country
            dfDiffNameUnnamed['namedroad_km']= dfDiffNameUnnamed['namedroad_km'].fillna(0)
            dfDiffNameUnnamed['unnamedroad_km']= dfDiffNameUnnamed['unnamedroad_km'].fillna(0)
            dfDiffNameUnnamed['total_km'] = dfDiffNameUnnamed['namedroad_km']+ dfDiffNameUnnamed['unnamedroad_km']

            # ovarall Coverage
            #
            newAllCoverageKM = allCoverageKM.replace("{schema}", row['schema_name'])
            dfAllCoverageKM = pd.read_sql(newAllCoverageKM, con=engine)
            dfAllCoverageKM['schema'] = row['schema_name']
            dfAllCoverageKM['country'] = country

            # Named and UnNamed
            # csvFileWriter(dfDiffNameUnnamed, "OrbisEnterprisSTRRoadKM.csv","/Users/parande/Documents/7_Adoc/1_SQL_Stat/")
            
            #
            csvFileWriter(dfDiffNameUnnamed, "Coverage_OrbisEnterprisSTRRoadKM.csv","/Users/parande/Documents/7_Adoc/1_SQL_Stat/")


