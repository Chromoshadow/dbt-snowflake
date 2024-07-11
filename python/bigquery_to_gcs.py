from google.cloud import bigquery
from google.oauth2 import service_account

key={
  "type": "service_account",
  "project_id": "lapiscine-426709",
  "private_key_id": "9d369fe01ad58d1c2c1558ca528c5b29812f2bb1",
  "private_key": "",
  "client_email": "svc-sap2snow-snow-bq@lapiscine-426709.iam.gserviceaccount.com",
  "client_id": "104584551326981726250",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/svc-sap2snow-snow-bq%40lapiscine-426709.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


table_names_query="select table_name from `lapiscine-426709.CORTEX_SAP_RAW.INFORMATION_SCHEMA.TABLES`"

credentials = service_account.Credentials.from_service_account_info(key)
client = bigquery.Client(credentials=credentials)
bucket_name = 'saptestdatasetspub'
project = "lapiscine-426709"
dataset_id = "CORTEX_SAP_RAW"

query_job = client.query(table_names_query)
table_names = query_job.result()

for row in table_names:

    table_name = row.table_name

    destination_uri_csv = "gs://{}/csv/{}/{}-*.csv".format(bucket_name, table_name, table_name)
    destination_uri_parquet = "gs://{}/parquet/{}/{}-*.parquet".format(bucket_name, table_name, table_name)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)

    extract_csv_job = client.extract_table(
        table_ref,
        destination_uri_csv,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_csv_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_name, destination_uri_csv)
    )

    extract_parquet_job = client.extract_table(
        table_ref,
        destination_uri_parquet,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_parquet_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_name, destination_uri_csv)
    )