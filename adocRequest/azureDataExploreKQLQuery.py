import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
import os


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


# # A. Get Max Week in Year Per Country
# providerIDYearWeek = adxdf[(adxdf.Measurement == 'MAP')
#                            & (adxdf.Year == '2023')
#                            & (adxdf.provider_id == 'Genesis')
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


# def getMaxWeekinYearPerCountry(adxdf, Measurement, Year, providerId, metric):
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
    results_df = pd.concat(country_dfs)

    return results_df


# # Genesis MAP
# GenesisMapAF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'ASF')
# GenesisMapSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SSF')
# GenesisMapLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'LSF')
# GenesisMapPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'PSF')
# GenesisMapAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'APA')
#
# # Genesis API
#
# GenesisApiAF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'ASF')
# GenesisApiSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'SSF')
# GenesisApiLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'LSF')
# GenesisApiPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'PSF')
# GenesisApiAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Genesis', 'APA')


# # Orbis MAP
# OrbisMapAF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'ASF')
# OrbisMapSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SSF')
# OrbisMapLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'LSF')
# OrbisMapPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'PSF')
# OrbisMapAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'APA')
#
# # Genesis API
# OrbisApiAF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'ASF')
# OrbisApiSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'SSF')
# OrbisApiLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'LSF')
# OrbisApiPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'PSF')
# OrbisApiAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Orbis', 'APA')
#
#
# # Here API
# HereApiAF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'ASF')
# HereApiSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'SSF')
# HereApiLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'LSF')
# HereApiPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'PSF')
# HereApiAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Here', 'APA')
#
# # Bing API
# BingApiAF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'ASF')
# BingApiSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'SSF')
# BingApiLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'LSF')
# BingApiPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'PSF')
# BingApiAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Bing', 'APA')
#
# # Google API
# OSMApiAF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'ASF')
# OSMApiSSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'SSF')
# OSMApiLSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'LSF')
# OSMApiPSF = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'PSF')
# OSMApiAPA = finalGetMaxWeekinYearPerCountry(adxdf, 'API', 'Google', 'APA')