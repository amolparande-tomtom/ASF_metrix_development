import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
import os

path= "/Users/parande/Documents/2_KQL/csv/"
allSearchDump= "AllSearchMetrixPowerBIProvider.csv"
searchMetrix = "SearchMetrixPowerBIProvider.csv"

searchPowerBI = os.path.join(path, searchMetrix)
allData = os.path.join(path, allSearchDump)


def deleteFilesIfExist(path):
    try:
        # Check if the files exist and delete them
        if os.path.exists(os.path.join(path, "SearchMetrixPowerBIProvider.csv")):
            os.remove(os.path.join(path, "SearchMetrixPowerBIProvider.csv"))
        if os.path.exists(os.path.join(path, "AllSearchMetrixPowerBIProvider.csv")):
            os.remove(os.path.join(path, "AllSearchMetrixPowerBIProvider.csv"))
        print("Files deleted successfully")
    except Exception as e:
        print("An error occurred while deleting the files:", e)

def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")


cluster = "https://adxmapsanalytics.westeurope.kusto.windows.net"
# client_id = "3e6709c4-f552-4b2b-a16b-59eb99660fe7"
# client_secret = "OyX8Q~lLR2fkSGSROt-LurOdcdinyIy3uWA2ibTs"
# authority_id = "374f8026-7b54-4a3a-b87d-328fa26ec10d"

client_id = "693565ec-b8c0-4286-b02b-742dd95bc99a"
client_secret = "9Co8Q~k2e~pOk1p0-23gFUKQku45mJKXozULEcrB"
authority_id = "374f8026-7b54-4a3a-b87d-328fa26ec10d"

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret,
                                                                            authority_id)

client = KustoClient(kcsb)
db = "mapsanalyticsDB"

query = "results_addressing"
response = client.execute(db, query)

# Explore KQL data in pandas DataFrame
adxdf = dataframe_from_result_table(response.primary_results[0])

# def GetLatestProviderIDYearWeek(providerName):
#     # Get the Max Year
#     adxdf["Year"] = adxdf["matching_run_id"].str[:4]
#     providerMask = adxdf["provider_id"] == providerName
#     provideDF = adxdf[providerMask]
#     yearMax = max(provideDF["Year"].unique().tolist())
#     # Get the Max week
#     provideWeekMask = provideDF["Year"] == yearMax
#     provideWeekDF = provideDF[provideWeekMask]
#     weekMax = max(provideWeekDF["Week"].unique().tolist())
#     # selecting provideID and Year and Week
#     providerIDYearWeek = adxdf[(adxdf.provider_id == providerName) & (adxdf.Year == yearMax) & (adxdf.Week == weekMax)]
#     return providerIDYearWeek
#
#
# GenesisDF = GetLatestProviderIDYearWeek("Genesis")
# OrbisDF = GetLatestProviderIDYearWeek("Orbis")
# OSMDF = GetLatestProviderIDYearWeek("OSM")
# BingDF = GetLatestProviderIDYearWeek("Bing")
# GoogleDF = GetLatestProviderIDYearWeek("Google")
# HereDF = GetLatestProviderIDYearWeek("Here")
#
# # concatenate all providerID
# df =[]
# if not GenesisDF.empty:
#     df.append(GenesisDF)
# if not OrbisDF.empty:
#     df.append(OrbisDF)
# if not OSMDF.empty:
#     df.append(OSMDF)
# if not BingDF.empty:
#     df.append(BingDF)
# if not GoogleDF.empty:
#     df.append(GoogleDF)
# if not HereDF.empty:
#     df.append(HereDF)
# adxData = pd.concat(df)

#################################
# Year List
adxdf["Year"] = adxdf["matching_run_id"].str[:4]
providerMask = adxdf["provider_id"] == 'Orbis'
provideDF = adxdf[providerMask]

# Year List
yearList = provideDF["Year"].unique().tolist()

# unique Country List
countryISOList = provideDF["country"].unique().tolist()

# use the str.zfill() method to add a leading zero to single-digit numbers
adxdf["Week"] = adxdf["Week"].astype(str)
adxdf["Week"] = adxdf["Week"].str.zfill(2).astype(str)
# Concatenation
adxdf["Week Year"] = adxdf['Year'].astype(str)+' '+adxdf["Week"].astype(str)
adxdf = adxdf.sort_values(['Year', 'Week'], ascending=[True, True])
# Delete Existing Files
deleteFilesIfExist(path)

# Dump All ADX Data
adxdf.to_csv(allData)


# # A. Get Max Week in Year Per Country
# providerIDYearWeek = adxdf[(adxdf.Measurement == 'API')
#                            & (adxdf.Year == '2022')
#                            & (adxdf.provider_id == 'OSM')
#                            & (adxdf.metric == 'ASF')]
#
# grouped_df = providerIDYearWeek.groupby(['country', 'Measurement', 'metric', 'provider_id'])['country', 'Week'].max()
#
# grouped_df.reset_index(inplace=True, drop=True)
#
# # create a new dataframe with columns from df1 for common country and week
# groupedDFNew = pd.merge(providerIDYearWeek, grouped_df, on=['country', 'Week'], how='inner')
# # drop duplicate
# groupedDFNew.drop_duplicates(subset=['Week', 'provider_id', 'country'], inplace=True)


############## Working Code ###############
def getMaxWeekinYearPerCountry(adxdf: pd.DataFrame, Measurement: str, Year: str, providerId: str,
                               metric: str) -> pd.DataFrame:
    # Get Max Week in Year Per Country
    providerIDYearWeek = adxdf[(adxdf.Measurement == Measurement)
                               & (adxdf.Year == Year)
                               & (adxdf.provider_id == providerId)
                               & (adxdf.metric == metric)]

    grouped_df = providerIDYearWeek.groupby(['country', 'Measurement', 'metric', 'provider_id'])[
        'country', 'Week'].max()

    grouped_df.reset_index(inplace=True, drop=True)

    # create a new dataframe with columns from df1 for common country and week
    groupedDFNew = pd.merge(providerIDYearWeek, grouped_df, on=['country', 'Week'], how='inner')
    # drop duplicate
    groupedDFNew.drop_duplicates(subset=['Week', 'provider_id', 'country'], inplace=True)
    return groupedDFNew


def finalGetMaxWeekinYearPerCountry(adxdf: pd.DataFrame, Measurement: str, providerId: str,
                                    metric: str) -> pd.DataFrame:
    # create Genesis MAP
    df_2023 = getMaxWeekinYearPerCountry(adxdf, Measurement, '2023', providerId, metric)
    df_2022 = getMaxWeekinYearPerCountry(adxdf, Measurement, '2022', providerId, metric)
    # create a list to store the first appearance of each country
    first_appearances = []

    # create a list to store the dataframes containing each country
    country_dfs = []

    # search for each country in 2023, then 2022, and add to the lists if found
    for country in countryISOList:
        if country in df_2023['country'].values:
            first_appearances.append('2023')
            country_dfs.append(df_2023.loc[df_2023['country'] == country])
        elif country in df_2022['country'].values:
            first_appearances.append('2022')
            country_dfs.append(df_2022.loc[df_2022['country'] == country])
    # create a dataframe with the results
    if len(country_dfs) != 0:
        results_df = pd.concat(country_dfs)
        return results_df
    else:
        # create an empty DataFrame with the same column names as the original DataFrame
        empty_df = pd.DataFrame(columns=adxdf.columns)
        # set the original DataFrame to the empty DataFrame
        df = empty_df
        return df


finalDataFrame = []

# OSM MAP
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'ASF').empty:
    # OSMMapASF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'ASF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'SSF').empty:
    # OSMApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'SSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'LSF').empty:
    # OSMApiLSF =
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'LSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'PSF').empty:
    # OSMApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'APA').empty:
    # OSMApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'OSM', 'APA'))

# Genesis MAP

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'ASF').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'ASF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SSF').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'LSF').empty:
    # GenesisMapLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'LSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'PSF').empty:
    # GenesisMapPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'APA').empty:
    # GenesisMapAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'APA'))

# Genesis API
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'ASF').empty:
    # GenesisApiAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'ASF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'SSF').empty:
    # GenesisApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'SSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'LSF').empty:
    # GenesisApiLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'LSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'PSF').empty:
    # GenesisApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'APA').empty:
    # GenesisApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'APA'))

# Orbis API

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'ASF').empty:
    # OrbisApiAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'ASF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'SSF').empty:
    # OrbisApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'SSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'LSF').empty:
    # OrbisApiLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'PSF').empty:
    # OrbisApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'APA').empty:
    # OrbisApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'APA'))

# Orbis MAP

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'ASF').empty:
    # OrbisMAPASF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'ASF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SSF').empty:
    # OrbisMAPSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'SSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'LSF').empty:
    # OrbisMAPLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'PSF').empty:
    # OrbisMAPPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'APA').empty:
    # OrbisMAPAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'APA'))

# Here API
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'ASF').empty:
    # HereApiAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'ASF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'SSF').empty:
    # HereApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'SSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'LSF').empty:
    # HereApiLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'PSF').empty:
    # HereApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'PSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'APA').empty:
    # HereApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'APA'))

# Bing API
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'ASF').empty:
    # BingApiAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'ASF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'SSF').empty:
    # BingApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'SSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'LSF').empty:
    # BingApiLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'PSF').empty:
    # BingApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'PSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'APA').empty:
    # BingApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'APA'))

# Google API
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'ASF').empty:
    # GoogleApiAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'ASF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'SSF').empty:
    # GoogleApiSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'SSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'LSF').empty:
    # GoogleApiLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'PSF').empty:
    # GoogleApiPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'PSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'APA').empty:
    # GoogleApiAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'APA'))

powerBI = pd.concat(finalDataFrame)

powerBI.to_csv(searchPowerBI)
