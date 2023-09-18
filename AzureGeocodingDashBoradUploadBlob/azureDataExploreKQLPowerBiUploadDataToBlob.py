import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import datetime
import numpy as np

path = "/Users/parande/Documents/2_KQL/csv"
AllSearchMetrixPowerBIProvider = "AllSearchMetrixPowerBIProvider.csv"
searchMetrixPowerBIProvider = "SearchMetrixPowerBIProvider.csv"
SearchMetrixPowerBIProviderPivot = "SearchMetrixPowerBIProviderPivot.csv"
searchMetrixPowerBIProviderPiovtOrbisTrends = "searchMetrixPowerBIProviderPiovtOrbisTrends.csv"


def piovtTableFunctionSearchMetrixALL(powerBI):
    """
    :param powerBI: DatFrame
    :return: Pivote Table for Product Management
    """
    pivot_table = powerBI[['country', 'release_version', 'metric', 'Measurement', 'provider_id', 'mean']]
    # concatenate two columns and create a new column
    pivot_table['provider_id_Measurement'] = pivot_table['provider_id'] + '-' + pivot_table['Measurement']
    # Pivot the data frame
    pivotTableData = pd.pivot_table(pivot_table, values='mean', index=['country', 'metric', 'release_version'],
                                    columns='provider_id_Measurement').reset_index()
    # identify columns with "_matchPer" suffix
    match_cols = [col for col in pivotTableData.columns if col not in ('country', 'metric', 'release_version')]
    # create new column "High Match" or winner each Score and "Value_From Column"
    pivotTableData['Winner'] = pivotTableData[match_cols].max(axis=1)
    pivotTableData['Winner Score'] = pivotTableData[match_cols].idxmax(axis=1)
    # identify the column(s) containing None values
    null_cols = pivotTableData.columns[pivotTableData.isnull().any()]
    # replace the None values with a valid string value
    pivotTableData[null_cols] = pivotTableData[null_cols].fillna(0)
    # Calculate Deviation from Genesis MAP Orbis MAP
    pivotTableData['Deviation Genesis'] = pivotTableData['Genesis-MAP'] - pivotTableData['Winner']
    # Calculate Deviation from Orbis MAP
    pivotTableData['Deviation Orbis'] = pivotTableData['Orbis-MAP'] - pivotTableData['Winner']
    # Round the float columns to 2 decimal places
    # Remove Duplicate Base on Multiple columns
    powerBIDuplicates = powerBI[['country', 'country_rank']]
    rankCountry = powerBIDuplicates.drop_duplicates(subset=['country', 'country_rank'])
    # sort the dataframe by ascending 'country'
    rankCountry = rankCountry.sort_values(by=['country'])
    merged_df = rankCountry.merge(pivotTableData, on='country')
    # merged_df = merged_df.rename(columns={"country_rank": "rank"})
    pivotTableDataMergedDf = merged_df[
        ['country', 'metric', 'release_version', 'country_rank', 'Genesis-MAP', 'Orbis-MAP', 'OSM-MAP', 'Genesis-API',
         'Orbis-API',
         'Google-API', 'Here-API',
         'Bing-API', 'Winner', 'Winner Score', 'Deviation Genesis', 'Deviation Orbis']]

    pivotTableDataMergedDf.sort_values(by=['release_version'], ascending=True)
    # Dumping SearchMetrixPowerBIProviderPivotTable  to Local code
    return pivotTableDataMergedDf


def piovtTableFunctionSearchMetrix(powerBI):
    """
    :param powerBI: DatFrame
    :return: Pivote Table for Product Management
    """
    pivot_table = powerBI[
        ['country', 'ISO3_Country', 'release_version', 'metric', 'Measurement', 'provider_id', 'mean']]
    # concatenate two columns and create a new column
    pivot_table['provider_id_Measurement'] = pivot_table['provider_id'] + '-' + pivot_table['Measurement']
    # # Create Copy
    # pivotTableProvider = pivot_table.copy()
    #
    # pivotTableProviderDuplicates = pivotTableProvider[['country', 'provider_id_Measurement','release_version']]
    # rankCountry = pivotTableProviderDuplicates.drop_duplicates(subset=['country', 'provider_id_Measurement','release_version'])

    # Pivot the DataFrame
    pivotTableData = pd.pivot_table(pivot_table, values='mean', index=['country', 'ISO3_Country', 'metric'],
                                    columns='provider_id_Measurement').reset_index()
    # identify columns with "_matchPer" suffix
    match_cols = [col for col in pivotTableData.columns if col not in ('country', 'ISO3_Country', 'metric')]
    # create new column "HightMatch" or winner each Score and "Value_From Column"
    pivotTableData['Winner Score'] = pivotTableData[match_cols].max(axis=1)
    pivotTableData['Winner'] = pivotTableData[match_cols].idxmax(axis=1)
    # identify the column(s) containing None values
    null_cols = pivotTableData.columns[pivotTableData.isnull().any()]
    # replace the None values with a valid string value
    pivotTableData[null_cols] = pivotTableData[null_cols].fillna(0)
    # Calculate Deviation from Genesis MAP Orbis MAP
    pivotTableData['Deviation Genesis'] = pivotTableData['Genesis-MAP'] - pivotTableData['Winner Score']
    # Calculate Deviation from Orbis MAP
    pivotTableData['Deviation Orbis'] = pivotTableData['Orbis-MAP'] - pivotTableData['Winner Score']
    # Round the float columns to 2 decimal places
    # Remove Duplicate Base on Multiple columns
    powerBIDuplicates = powerBI[['country', 'country_rank']]
    rankCountry = powerBIDuplicates.drop_duplicates(subset=['country', 'country_rank'])
    # sort the dataframe by ascending 'country'
    rankCountry = rankCountry.sort_values(by='country')
    merged_df1 = rankCountry.merge(pivotTableData, on='country')

    # Remove Duplicate Base on Multiple columns
    powerBIDuplicates = powerBI[['country', 'country_group']]
    rankCountry2 = powerBIDuplicates.drop_duplicates(subset=['country', 'country_group'])
    # sort the dataframe by ascending 'country'
    rankCountry2 = rankCountry2.sort_values(by='country')
    merged_df = rankCountry2.merge(merged_df1, on='country')

    pivotTableDataMergedDf = merged_df[
        ['country', 'ISO3_Country', 'metric', 'country_rank', 'country_group', 'Genesis-MAP', 'Orbis-MAP', 'OSM-MAP',
         'Genesis-API', 'Orbis-API',
         'Google-API', 'Here-API', 'Bing-API', 'Winner', 'Winner Score', 'Deviation Genesis', 'Deviation Orbis']]
    # Dumping SearchMetrixPowerBIProviderPivotTable  to Local code

    return pivotTableDataMergedDf


def csvFileWriter(pandasDataFrame, filename, outputpath):
    if not os.path.exists(outputpath + filename):
        pandasDataFrame.to_csv(outputpath + filename, mode='w', index=False, encoding="utf-8")

    else:
        pandasDataFrame.to_csv(outputpath + filename, mode='a', header=False, index=False, encoding="utf-8")


def uploadFileToAzureBlobAndRenameExistngFile(csv_name, container_name, connect_str, pdDataFrame):
    # Set up the Azure Blob Storage connection
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    # Rename the old Blob if it exists
    blob_client = container_client.get_blob_client(csv_name)
    if blob_client.exists():
        try:
            # Create a new name with the current date and time
            now = datetime.datetime.now()
            new_blob_name = f'{csv_name}_{now.strftime("%Y-%m-%d_%H-%M-%S")}.csv'
            new_blob_client = container_client.get_blob_client(new_blob_name)
            new_blob_client.start_copy_from_url(blob_client.url)
            blob_client.delete_blob()
            print(f'The old Blob "{csv_name}" was renamed to "{new_blob_name}".')
        except Exception as e:
            print(f'Error renaming the Blob: {str(e)}')
    # Upload the CSV string to Azure Blob Storage
    blob_client = container_client.get_blob_client(csv_name)
    blob_client.upload_blob(pdDataFrame)
    print(
        f'The DataFrame from the ADX query was uploaded as a CSV file to the Blob Storage container "{container_name}" as "{csv_name}".')


# ADX credential
cluster = "https://adxmapsanalytics.westeurope.kusto.windows.net"
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

########## Set up the Azure Blob Storage connection #########

container_name = 'searchmetrixanalyticspowerbi'
connect_str = 'DefaultEndpointsProtocol=https;AccountName=metixreport;AccountKey=tad+pHj2VVTYsxeAqaskZoAHlXl9Oi9Zzg9K88itIL7LjUYzou0yc7CkvRh87HzC/+MAixEhh7Dc+ASt9oi1pQ==;EndpointSuffix=core.windows.net'

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
# filter Pandas series Orbis provider only
providerMask = adxdf["provider_id"] == 'Orbis'
# select Orbis provider only
provideDF = adxdf[providerMask]

# Year List
yearList = provideDF["Year"].unique().tolist()

# unique Country List
countryISOList = provideDF["country"].unique().tolist()

# use the str.zfill() method to add a leading zero to single-digit numbers
adxdf["Week"] = adxdf["Week"].astype(str)
adxdf["Week"] = adxdf["Week"].str.zfill(2).astype(str)
# Concatenation
adxdf["Week Year"] = adxdf['Year'].astype(str) + ' ' + adxdf["Week"].astype(str)
adxdf = adxdf.sort_values(['Year', 'Week'], ascending=[True, True])

# Export to Local
# adxdf.to_csv("/Users/parande/Documents/2_KQL/AllSearchMetrixPowerBIProvider.csv")

# AllSearchMetrixPowerBIProviderPiovt
AllSearchMetrixPowerBIProviderPiovt = piovtTableFunctionSearchMetrixALL(adxdf)


#####################################################################
########## Orbis Metrics - Trends Dashboards Preparation ############
#####################################################################
def OrbisMetricsTrendsLatestSixRelease(adxdf: pd.DataFrame):
    """
    :param adxdf: AllSearchMetrixPowerBIProviderPiovt
    :return: Orbis Metrics Trends The Latest Six Release columns in DataFrame
    """
    global topSixRelease
    # Filter the DataFrame based on the column "provider_id" equal to "Orbis" and column 'Measurement' equal to 'MAP'
    filteredOrbis = adxdf[(adxdf['provider_id'] == 'Orbis') & (adxdf['Measurement'] == 'MAP')]
    # Filter the values ending with "OV"
    filtereOrbisVersion = filteredOrbis[filteredOrbis['release_version'].str.endswith('OV')]['release_version']
    # Remove "OV" from the filtered values and create a list
    result_list = filtereOrbisVersion.str.replace('OV', '').tolist()
    # Remove duplicates
    result_list = list(set(result_list))
    # Sort the list in ascending order
    result_list.sort()
    topSixRelease = result_list[-6:]
    # Filter DataFrame based on the last five records
    # Initialize an empty DataFrame to store the merged results
    mergedDF = pd.DataFrame()
    for ReleaseVersion in topSixRelease:
        newReleaseVersion = ReleaseVersion + 'OV'
        # Filter relative records only
        orbisVersionFilter = filteredOrbis[filteredOrbis['release_version'] == newReleaseVersion]
        # Select only required columns
        selectedDF = orbisVersionFilter[['Alpha3_code', 'metric', 'mean']]
        # Rename Columns
        selectedDF.rename(columns={'mean': newReleaseVersion, 'Alpha3_code': 'country'}, inplace=True)
        # Sort DataFrame by columns 'A' and 'B'
        sortedDf = selectedDF.sort_values(by=['country', 'metric'])
        # Perform the left join
        # if mergedDF id empty update sortedDf first
        if mergedDF.empty:
            mergedDF = sortedDf
        # add columns for heights by columns
        else:
            mergedDF = mergedDF.merge(sortedDf, on=['country', 'metric'], how='left')
    # mergedDF will contain the merged DataFrame with all the renamed columns
    return mergedDF


def fillNullValuesWithZero(dataframe: pd.DataFrame) -> pd.DataFrame:
    # Check if the DataFrame has any null values
    if dataframe.isnull().any().any():
        # Fill null values with 0
        dataframe.fillna(0, inplace=True)

    return dataframe


# AllSearchMetrixPowerBIProviderPiovt.to_csv("/Users/parande/Documents/2_KQL/PivotTable/ALlSearchMetrixPowerBIPivotTableRank.csv")

# Convert the DataFrame to a CSV string
pdDataFrameAdxdf = adxdf.to_csv(index=False)


##############################################################################
# Write data inti Azure Data Storage
# Layer Name :AllSearchMetrixPowerBIProvider
##############################################################################

# Writing "AllSearchMetrixPowerBIProvider"" file in to Azure Blob Storage
# uploadFileToAzureBlobAndRenameExistngFile(AllSearchMetrixPowerBIProvider, container_name, connect_str, pdDataFrameAdxdf)

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


def addReleaseVersionColumn(ProviderID, AllDataFrame, ProviderPiovt):
    """
    :param ProviderID: ProviderID
    :param AllDataFrame: AllDataFrame Metix
    :param ProviderPiovt:
    :return:
    """
    pivot_table1 = AllDataFrame[['country', 'Measurement', 'provider_id', 'release_version']]
    # concatenate two columns and create a new column
    columnName = ProviderID + "_" + 'RV'
    pivot_table1["ProviderName"] = pivot_table1['provider_id'] + '-' + pivot_table1['Measurement']
    # Filter data
    newDF = pivot_table1[pivot_table1["ProviderName"] == ProviderID]
    # pivot_table1 = newDF[['country', 'release_version']]

    rankCountry = newDF[['country', 'release_version']]
    # rankCountry = pivot_table1.drop_duplicates(subset=['country', ])
    rankCountry.sort_values(by='country', inplace=True)
    # Create Dense Rank Number
    rankCountry["rank"] = rankCountry.groupby("country")["release_version"].rank(method="dense", ascending=False)
    rankCountry.sort_values(by=['country','rank'], ascending=True, inplace=True)
    # Filter out rows where "Rank" is equal to 1
    rankCountry = rankCountry[rankCountry['rank'] == 1]
    rankCountry = rankCountry.drop_duplicates(subset=['country', ])

    rankCountry.rename(columns={'release_version': columnName}, inplace=True)

    releaseVersionRanMerged_df = ProviderPiovt.merge(rankCountry, on='country', how='left')
    # Remove the "Rank" column
    releaseVersionRanMerged_df = releaseVersionRanMerged_df.drop('rank', axis=1)
    return releaseVersionRanMerged_df


finalDataFrame = []

# SHI MAP
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI'))

# SHI_order_1 MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_1').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_1'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_1').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_1'))

# SHI_order_2 MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_2').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_2'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_2').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_2'))

# SHI_order_3 MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_3').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_3'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_3').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_3'))

# SHI_order_4 MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_4').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_4'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_4').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_4'))

# SHI_order_5 MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_5').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_5'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_5').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_5'))

# SHI_order_country MAP and Orbis
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_oSHI_order_countryrder_5').empty:
    # GenesisMapAF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'SHI_order_country'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_country').empty:
    # GenesisMapSSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SHI_order_country'))

# Genesis MAP
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'LSF').empty:
    # GenesisMapLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'LSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'PSF').empty:
    # GenesisMapPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'PSF'))

if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'APA').empty:
    # GenesisMapAPA
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Genesis', 'APA'))

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
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'SSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'LSF').empty:
    # OrbisMAPLSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'LSF'))
if not finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'PSF').empty:
    # OrbisMAPPSF
    finalDataFrame.append(finalGetMaxWeekinYearPerCountry(adxdf, 'MAP', 'Orbis', 'PSF'))

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

##################################################################
# Dumping SearchMetrixPowerBIProvider to Local code
# powerBI.to_csv("/Users/parande/Documents/2_KQL/PivotTable/SearchMetrixPowerBIRank.csv")
# piovt DataFrame

searchMetrixPowerBIProviderPiovt = piovtTableFunctionSearchMetrix(powerBI)

# Remove Duplicate Base on Multiple columns 'release_version'

# 'Genesis-MAP'

searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Genesis-MAP', powerBI, searchMetrixPowerBIProviderPiovt)
# 'Genesis-API'

searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Genesis-API', powerBI, searchMetrixPowerBIProviderPiovt)

# 'Orbis-MAP'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Orbis-MAP', powerBI, searchMetrixPowerBIProviderPiovt)

# 'Orbis-API'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Orbis-API', powerBI, searchMetrixPowerBIProviderPiovt)

# 'OSM-MAP'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('OSM-MAP', powerBI, searchMetrixPowerBIProviderPiovt)

# 'Here-API'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Here-API', powerBI, searchMetrixPowerBIProviderPiovt)

# 'Bing-API'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Bing-API', powerBI, searchMetrixPowerBIProviderPiovt)

# 'Google-API'
searchMetrixPowerBIProviderPiovt = addReleaseVersionColumn('Google-API', powerBI, searchMetrixPowerBIProviderPiovt)

pdDFpiovtSearchMetrixPowerBIProviderPowerBi = searchMetrixPowerBIProviderPiovt.to_csv(index=False)

##############################################################################
# 1. Write data into Azure Data Storage
# Layer Name :SearchMetrixPowerBIProviderPivot
##############################################################################

uploadFileToAzureBlobAndRenameExistngFile(SearchMetrixPowerBIProviderPivot, container_name, connect_str,
                                          pdDFpiovtSearchMetrixPowerBIProviderPowerBi)

# searchMetrixPowerBIProviderPiovt.to_csv("/Users/parande/Documents/2_KQL/PivotTable/SearchMetrixPowerBIPivotTableRank.csv")
# Azure Blob Storage Data Processing

# Convert the DataFrame to a CSV string
#####################################################################
########## Orbis Metrics - Trends Dashboards Preparation ############
#####################################################################
OrbisMetricxLatestSixRelease = OrbisMetricsTrendsLatestSixRelease(adxdf)
searchMetrixPowerBIProviderPiovtCopy = searchMetrixPowerBIProviderPiovt.copy()

# Create a list of column names in the desired order
new_column_order = ['country', 'ISO3_Country', 'metric', 'country_rank', 'country_group',
                    'Genesis-MAP', 'Orbis-MAP', 'Genesis-API', 'Orbis-API',
                    'Google-API', 'Here-API', 'Bing-API', 'OSM-MAP', 'Winner', 'Winner Score',
                    'Deviation Genesis', 'Deviation Orbis', 'Genesis-MAP_RV',
                    'Genesis-API_RV', 'Orbis-MAP_RV', 'Orbis-API_RV', 'OSM-MAP_RV',
                    'Here-API_RV', 'Bing-API_RV', 'Google-API_RV']

# Rearrange the columns using the reindex() method
searchMetrixPowerBIProviderPiovtCopy = searchMetrixPowerBIProviderPiovtCopy.reindex(columns=new_column_order)

# Replace 'GBL' with 'WORLDWIDE' in the 'country' column
OrbisMetricxLatestSixRelease.loc[OrbisMetricxLatestSixRelease['country'] == 'GBL', 'country'] = 'WORLDWIDE'

# merge with orignal DataFrame
mergedDF = searchMetrixPowerBIProviderPiovtCopy.merge(OrbisMetricxLatestSixRelease, on=['country', 'metric'],
                                                      how='left')
columns_to_remove = ['country_group', 'Orbis-MAP', 'Genesis-API', 'Orbis-API', 'Winner', 'Winner Score',
                     'Deviation Genesis', 'Deviation Orbis', 'Genesis-MAP_RV',
                     'Genesis-API_RV', 'Orbis-MAP_RV', 'Orbis-API_RV', 'OSM-MAP_RV',
                     'Here-API_RV', 'Bing-API_RV', 'Google-API_RV']

# Remove the unwanted columns using the drop() method
mergedDFNew = mergedDF.drop(columns=columns_to_remove)

fillNullValuesWithZeroNew = fillNullValuesWithZero(mergedDFNew)

##############################################################################
# Write data inti Azure Data Storage
# Layer Name :SearchMetrixPowerBIProviderPivotOrbisMetricsTrends
##############################################################################

dfOrbisTrends = fillNullValuesWithZeroNew.to_csv(index=False)

uploadFileToAzureBlobAndRenameExistngFile(searchMetrixPowerBIProviderPiovtOrbisTrends, container_name, connect_str,
                                          dfOrbisTrends)

# fillNullValuesWithZeroNew.to_csv("/Users/parande/Documents/2_KQL/PivotTable/searchMetrixPowerBIProviderPiovtOrbisTrends.csv")


# pdDataFrameAdxPowerBi = powerBI.to_csv(index=False)
##############################################################################
# Write data inti Azure Data Storage
# Layer Name :searchMetrixPowerBIProvider
##############################################################################

# Writing "searchMetrixPowerBIProvider"" file in to Azure Blob Storage
# uploadFileToAzureBlobAndRenameExistngFile(searchMetrixPowerBIProvider, container_name, connect_str, pdDataFrameAdxPowerBi)
