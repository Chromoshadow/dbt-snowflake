import snowflake.connector
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

def get_execute_ddls():
    # Créez des identifiants pour le service account BigQuery
    credentials = service_account.Credentials.from_service_account_info(key)

    # Définissez le schéma BigQuery
    schema = "lapiscine-426709.CORTEX_SAP_RAW"
    print("Gathering DDL for tables in schema {}".format(schema))

    # Requête SQL pour récupérer les DDL des tables
    query = """
    SELECT table_name,
           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ddl, '`', ''),
                                                                                                                             'INT64', 'INT'),
                                                                                                                'FLOAT64', 'FLOAT'),
                                                                                                       'BOOL', 'BOOLEAN'),
                                                                                              'STRUCT', 'VARIANT'),
                                                                                     'PARTITION BY', 'CLUSTER BY ('),
                                                                            ';', ');'),
                                                                   'CREATE TABLE ', 'CREATE TABLE if not exists '),
                                                          'table INT,', '"table" INT,'),
                                                 '_"table" INT,', '_table INT,'),
                                        'ARRAY<STRING>', 'ARRAY'),
    'DATE(_PARTITIONTIME)', 'date(loaded_at)'),
    ' OPTIONS(', ', //'),
    '));', ');'),
    '_at);', '_at));'),
    'start ', '"start" '),
    '_"start"', '_start'),
    'order ', '"order" '),
    '<', ', //'),
    'lapiscine-426709.CORTEX_SAP_RAW','RAW'),
    '_"order"', '_order') as ddl
    FROM `lapiscine-426709.CORTEX_SAP_RAW.INFORMATION_SCHEMA.TABLES`
    """

    ddl_query = query.format(schema)
    
    # Créez un client BigQuery
    client = bigquery.Client(credentials=credentials)
    query_job = client.query(ddl_query)
    ddl_set = query_job.result()
    
    # Connexion à Snowflake
    conn = snowflake.connector.connect(
        user='',
        password='',
        account='xm94285.eu-west-1',
        warehouse='MIGRATION_BIGQUERY',
        database='ssi_sap_to_snow',
        role='role_data_engineer',
        schema='raw'
    )

    try:
        for row in ddl_set:
            table_name = row.table_name
            ddl = row.ddl
            print("Running DDL for table {} in Snowflake".format(table_name))

            use_schema = "USE SCHEMA raw"
            
            with conn.cursor() as cursor:
                cursor.execute(use_schema)
                cursor.execute(ddl)
            
            print("Table {} created in bq_db.{} schema".format(table_name, schema))
    finally:
        conn.close()

# Point d'entrée du script
if __name__ == "__main__":
    get_execute_ddls()
