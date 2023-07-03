import io
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Set up the connection to the ADX cluster

cluster = '<adx-cluster-name>'
client_id = '<aad-application-id>'
client_secret = '<aad-client-secret>'
tenant_id = '<aad-tenant-id>'
authority_uri = f'https://login.microsoftonline.com/{tenant_id}'
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(f'https://{cluster}.kusto.windows.net', client_id, client_secret, authority_uri)

# Define the ADX query
query = '<adx-query>'

# Execute the query and create a Pandas DataFrame from the result
response = KustoClient(kcsb).execute_query('<adx-database-name>', query)
df = dataframe_from_result_table(response.primary_results[0])

# Convert the DataFrame to a CSV string
csv_string = df.to_csv(index=False)

# Set up the Azure Blob Storage connection
connect_str = '<your-connection-string>'
container_name = '<your-container-name>'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Upload the CSV string to Azure Blob Storage
blob_name = '<blob-name>'
blob_client = container_client.get_blob_client(blob_name)
blob_client.upload_blob(csv_string)

print(f'The DataFrame from the ADX query was uploaded as a CSV file to the Blob Storage container "{container_name}" as "{blob_name}".')
