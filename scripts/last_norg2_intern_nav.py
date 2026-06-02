# API https://norg2.intern.nav.no/norg2/api/v1/enhet

# Hent alle enheter og legg dem i en dictionary med orgnr som nøkkel
# og enhetens navn som verdi. Skriv denne dictionaryen til en bigquery tabell.

import pandas as pd
import requests

def load_org_enhet():
    """
    kolonner vi bryr os om: enhetNr, navn, enhetId, status
    """
    url = "https://norg2.intern.nav.no/norg2/api/v1/enhet"
    response = requests.get(url)
    data: list[dict] = response.json()

    df = pd.DataFrame(data)

    df = df[["enhetNr", "navn", "enhetId", "status"]]
    # kun aktive enheter
    df = df[df["status"] == "Aktiv"]

    return df

def load_single_org_enhet(enhetNr: str):
    url = f"https://norg2.intern.nav.no/norg2/api/v1/enhet/{enhetNr}"
    response = requests.get(url)
    data: dict = response.json()
    return data

def insert_dim_org_enhet(df: pd.DataFrame, gcp_project: str, dataset: str, table: str):
    from google.cloud import bigquery

    client = bigquery.Client()
    table_id = f"{gcp_project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete.
    print(f"Loaded {job.output_rows} rows into {table_id}.")


if __name__ == "__main__":
    # show stats about the data
    df = load_org_enhet()
    dataset_name = "raw_api_data"
    table_name = "norg2_intern_nav"
    insert_dim_org_enhet(df, "pensjon-saksbehandli-dev-cb76", dataset_name, table_name) # dev
    insert_dim_org_enhet(df, "pensjon-saksbehandli-prod-1f83", dataset_name, table_name) # prod