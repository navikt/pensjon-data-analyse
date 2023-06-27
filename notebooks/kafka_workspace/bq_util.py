from google.cloud import bigquery


def create_bq_table_if_not_exists(bq_client: bigquery.Client, project_id: str, dataset_id: str, table: str) -> bigquery.Table:
    table_uri = f"{project_id}.{dataset_id}.{table}"

    schema = [
        bigquery.SchemaField("hendelsestype", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("tidspunkt", "TIMESTAMP"),
        bigquery.SchemaField("tema", "STRING"),
        bigquery.SchemaField("oppgavetype", "STRING"),
        bigquery.SchemaField("aktiv_fra", "DATE"),
        bigquery.SchemaField("aktiv_til", "DATE")
    ]

    table = bigquery.Table(table_uri, schema=schema)
    return bq_client.create_table(table, exists_ok=True)
