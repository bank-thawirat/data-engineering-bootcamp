import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "key-gcr.json"
service_account_info = json.load(open(keyfile))
credentials_gcr = service_account.Credentials.from_service_account_info(service_account_info)

keyfile = "key-bigquery.json"
service_account_info = json.load(open(keyfile))
credentials_bq = service_account.Credentials.from_service_account_info(service_account_info)

project_id = "data-engineer-384509"

# Load data from Local to GCS
bucket_name = "deb-bootcamp-100015"
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcr,
)
bucket = storage_client.bucket(bucket_name)

def upload_data(data):
    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bq,
        location=location,
    )
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

upload_data('addresses')
upload_data('events')
upload_data('order_items')
upload_data('orders')
upload_data('products')
upload_data('promos')
upload_data('users')