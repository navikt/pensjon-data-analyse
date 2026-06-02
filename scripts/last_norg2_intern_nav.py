# API https://norg2.intern.nav.no/norg2/api/v1/enhet

from pathlib import Path
import sys

import requests
sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import gcp_utils

from google.cloud import bigquery


def fetch_raw_json() -> str:
    url = "https://norg2.intern.nav.no/norg2/api/v1/enhet"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def insert_raw_json(client: bigquery.Client, raw_json: str, gcp_project: str, dataset: str, table: str):
    table_id = f"{gcp_project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, [{"raw_json_payload": raw_json}])
    if errors:
        raise RuntimeError(f"Errors inserting rows: {errors}")
    print(f"Inserted raw JSON into {table_id}.")


if __name__ == "__main__":
    raw_json = fetch_raw_json()

    dataset_name = "raw_api_data"
    table_name = "norg2_intern_nav"
    project = "pensjon-saksbehandli-prod-1f83"
    target_principal = "bigquery-airflow-dvh@pensjon-saksbehandli-prod-1f83.iam.gserviceaccount.com"
    client = gcp_utils.get_bigquery_client(project=project, target_principal=target_principal)
    insert_raw_json(client, raw_json, project, dataset_name, table_name)