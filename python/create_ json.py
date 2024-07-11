import json
from google.cloud import bigquery
from google.oauth2 import service_account

bq_key={
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

bq_dataset_json={
        "name": "",
        "properties": {
            "linkedServiceName": {
                "referenceName": "CON_BQ_SAP2SNOW_SRC",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "SAP_BIGQUERY"
            },
            "annotations": [],
            "type": "GoogleBigQueryV2Object",
            "schema": [],
            "typeProperties": {
                "dataset": "CORTEX_SAP_RAW",
                "table": "tvfst"
            }
        },
        "type": "Microsoft.DataFactory/factories/datasets"
    }

snow_dataset_json={
        "name": "",
            "properties": {
            "linkedServiceName": {
                "referenceName": "CON_SNOW_SAP2SNOW_DST",
                "type": "LinkedServiceReference"
            },
            "folder": {
                "name": "SAP_SNOWFLAKE"
            },
            "annotations": [],
            "type": "SnowflakeV2Table",
            "schema": [],
            "typeProperties": {
                "schema": "RAW",
                "table": "TVFST"
            }
        },
        "type": "Microsoft.DataFactory/factories/datasets"  
    }

pipeline_json={
    "name": "",
    "properties": {
        "activities": [
            {
                "name": "Copy data1",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": False,
                    "secureInput": False
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "GoogleBigQueryV2Source"
                    },
                    "sink": {
                        "type": "SnowflakeV2Sink",
                        "preCopyScript": "TRUNCATE TABLE SSI_SAP_TO_SNOW.RAW.TVFST;",
                        "importSettings": {
                            "type": "SnowflakeImportCopyCommand"
                        }
                    },
                    "enableStaging": True,
                    "stagingSettings": {
                        "linkedServiceName": {
                            "referenceName": "CON_BLOB_SAP2SNOW_STG",
                            "type": "LinkedServiceReference"
                        }
                    }
                },
                "inputs": [
                    {
                        "referenceName": "DS_BIGQUERY_SAP_TVFST",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "DS_SNOW_SAP_TVFST",
                        "type": "DatasetReference"
                    }
                ]
            }
        ],
        "annotations": [],
        "lastPublishTime": "2024-07-04T08:03:43Z"
    },
    "type": "Microsoft.DataFactory/factories/pipelines"
}

bq_query="select table_name from `lapiscine-426709.CORTEX_SAP_RAW.INFORMATION_SCHEMA.TABLES`"

credentials = service_account.Credentials.from_service_account_info(bq_key)
client = bigquery.Client(credentials=credentials)
query_job = client.query(bq_query)
table_names = query_job.result()
for row in table_names:
    table_name = row.table_name
    bq_dataset_name="DS_BIGQUERY_SAP_"+table_name.upper()
    snow_dataset_name="DS_SNOW_SAP_"+table_name.upper()
    pipeline_name="PP_CP_BQ_SNOW_SAP_"+table_name.upper()

    table_bq_dataset_json=bq_dataset_json.copy()
    table_bq_dataset_json["name"]=bq_dataset_name
    table_bq_dataset_json["properties"]["typeProperties"]["table"]=table_name
    with open(f'./bq_datasets/{bq_dataset_name}.json', 'w') as f:
        json.dump(table_bq_dataset_json, f)

    table_snow_dataset_json=snow_dataset_json.copy()
    table_snow_dataset_json["properties"]["typeProperties"]["table"]=table_name.upper()
    table_snow_dataset_json["name"]=snow_dataset_name
    with open(f'./snow_datasets/{snow_dataset_name}.json', 'w') as f:
        json.dump(table_snow_dataset_json, f)

    table_pipeline_json=pipeline_json.copy()
    table_pipeline_json["name"]=pipeline_name
    table_pipeline_json["properties"]["activities"][0]["typeProperties"]["sink"]["preCopyScript"]=f"TRUNCATE TABLE SSI_SAP_TO_SNOW.RAW.{table_name.upper()};"
    table_pipeline_json["properties"]["activities"][0]["inputs"][0]["referenceName"]=bq_dataset_name
    table_pipeline_json["properties"]["activities"][0]["outputs"][0]["referenceName"]=snow_dataset_name
    with open(f'./pipelines/{pipeline_name}.json', 'w') as f:
        json.dump(table_pipeline_json, f)