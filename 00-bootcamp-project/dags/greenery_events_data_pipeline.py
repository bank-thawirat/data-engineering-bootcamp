from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
import requests
import csv
# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here
API_URL = "http://34.87.139.82:8000/"

def _extract_data():
    ds = "2021-02-11"
    response = requests.get(f"{API_URL}events/?created_at={ds}")
    data = response.json()
    for item in data:
        print(item)    
    with open(f"/events.csv", "w") as f:
        writer = csv.writer(f)
        header = data[0].keys()
        writer.writerow(header)
        
        for each in data:
            result = [
                each[''],
                each[''],
                each[''],
                each[''],
                each[''],
                each[''],
                each[''],
                each['']
            ]

def _load_data_to_gcs():
    # Your code below
    pass


def _load_data_from_gcs_to_bigquery():
    # Your code below
    pass


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2023, 5, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_events_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule=None,  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery