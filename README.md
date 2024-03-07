# German Airports Data Ingestion DAG

This Directed Acyclic Graph (DAG) is designed for ingesting data about German airports from the Ninja API and storing the results in Google Cloud Storage (GCS) or locally, followed by further processing and loading into BigQuery.

## Overview

The DAG, named `ingestion_airports_germany`, performs several key functions:

- Fetches data about German airports from the Ninja API.
- Saves the fetched data locally and in a specified Google Cloud Storage bucket.
- Transforms the data by adding timestamp and data interval start information.
- (Placeholder for loading data to BigQuery, as the implementation is not included).

## Setup

1. **API Key Configuration**: Store your Ninja API key in GCP Secrets Manager and reference it in your Airflow Variable named `ninja`.
2. **GCP Configuration**: Ensure your GCP project ID and bucket name are correctly set in the DAG file variables `GCP_PROJECT` and `BUCKET_NAME`.
3. **Dependencies**: Make sure the `requests`, `json`, and `google-cloud-storage` libraries are installed in your Airflow environment.

## Usage

Schedule the DAG to run with the desired frequency to ingest data about German airports. The default schedule is set to run daily at 08:00.

The DAG includes the following tasks:

- `fetch_data_and_save`: Fetches data from the Ninja API and saves it locally and to GCS.
- `add_keys`: Enhances the data with additional timestamp information.
- `load_data`: (To be implemented) Loads the data into BigQuery for further analysis.

## Customization

You can customize the DAG to fit your needs by:

- Adjusting the schedule according to your data ingestion frequency requirements.
- Modifying the `BUCKET_NAME` and `GCP_PROJECT` variables to point to your GCS bucket and GCP project.
- Changing the API endpoint or parameters by modifying the `URL` and `params` in the `fetch_data` function.
