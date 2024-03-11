import requests
import json
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException

# GCS
import os
from google.cloud import storage

URL = "https://api.api-ninjas.com/v1/airports"
# assuming there is a secret in GCP in JSON format
AUTH = json.loads(Variable.get("ninja"))

BUCKET_NAME = "ninja"
GCP_PROJECT = "gcp_project"


def fetch_data(URL, params):
    """Get all data from the Ninja API

    Args:
        URL (str): The url to make the get request.
        params (dict): The params to make the get request.

    Returns:
        dict: The fetched data
    """
    logging.info("Fetching data for german airports from the Ninja API ...")
    all_data = []
    while True:
        try:
            response = requests.get(
                URL, headers={"X-Api-Key": AUTH["api_key"]}, params=params
            )
            data = response.json()
            all_data.extend(data)
            if len(data) < 30:
                break
            params["offset"] = params.get("offset", 0) + len(data)
            return all_data
        except Exception as e:
            raise AirflowFailException(
                f"An error ocurred during the extraction: {str(e)}"
            )


def push_to_gcs(data, gcp_project, interval_start_date):
    """Write data to Google Cloud Storage.

    Args:
        data (dict): The data to be written.
        gcp_project (str): The GCP project identifier.
        logical_date (str): The logical date of the Airflow run.

    Returns:
        str: The filename of the written data.
    """
    bucket_name = BUCKET_NAME
    logging.info(f"Writing data to bucket: {bucket_name} ...")
    client = storage.Client(project=gcp_project)
    bucket = client.bucket(bucket_name)
    filename = f"german_airports_{interval_start_date}.json"
    logging.info(f"Writing data to GCS: {filename} ...")
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(data))
    logging.info(f"Successfully wrote data to GCS: {filename}")
    return filename


@dag(
    schedule="0 8 * * *",
    start_date=datetime(2024, 3, 6),
    dag_id="ingestion_airports_germany",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Vera Zang",
        "retries": 0,
    },
    tags=["ninja", "german airports"],
)
def taskflow():

    @task(task_id="fetch_data_and_save")
    def fetch_data_and_save() -> None:
        """
        Download the data from the API and save to local path and GCP bucket
        """
        context = get_current_context()
        params = {"country": "DE"}
        all_data = fetch_data(URL, params)

        logging.info("Succesfully fetched data")
        # save to local
        with open(f"german_airports_{context['ds']}.json", "w") as json_file:
            for item in all_data:
                json.dump(item, json_file)
                json_file.write("\n")

        # push to GCS
        push_to_gcs(all_data, GCP_PROJECT, context["ds"])

        return {"fetched_data": all_data}

    @task(task_id="add_keys")
    def add_keys() -> None:
        """
        Get the fetched data and add the keys
        """
        context = get_current_context()
        # get fetched data from previous task using context
        data = context["ti"].xcom_pull(task_id=fetch_data_and_save)["fetched_data"]
        for row in data:
            row["transformation_timestamp"] = datetime.now().isoformat()
            row["data_interval_start"] = context["ds"]
        return {"transformed_data": data}

    @task(task_id="load_data")
    def load_data() -> None:
        """
        Load the Data to BigQuery
        """

    fetch_data_and_save() >> add_keys() >> load_data()


DAG = taskflow()
