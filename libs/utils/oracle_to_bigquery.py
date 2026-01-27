import logging
import time
from pydantic import BaseModel

import pandas as pd
from google.cloud.bigquery import Client, Dataset, SchemaField, enums, LoadJobConfig
from google.cloud.exceptions import NotFound
from oracledb import Connection

from utils import pesys_utils
from utils.schemas import DefaultSchemas


class JobConfig(BaseModel):
    oracle_table: str
    delta_column_name_oracle: str = "teknisk_tid"
    gcp_project: str
    bigquery_dataset: str
    bigquery_table: str
    delta_column_name_bigquery: str = "teknisk_tid"
    schema: list[SchemaField] = DefaultSchemas.BEHANDLINGSSTATISTIKK_MELDINGER
    write_disposition: str = enums.WriteDisposition.WRITE_APPEND
    create_disposition: str = enums.CreateDisposition.CREATE_IF_NEEDED

    @property
    def bigquery_dataset(self) -> Dataset:
        dataset = Dataset(f"{self.gcp_project}.{self.bigquery_dataset}")
        dataset.location = "europe-north1"
        return dataset

    @property
    def bigquery_table_id(self) -> str:
        return f"{self.gcp_project}.{self.bigquery_dataset}.{self.bigquery_table}"
    
    @property
    def bigquery_view_id(self) -> str:
        return f"{self.gcp_project}.{self.bigquery_dataset}.{self.bigquery_table}_view"


def delta_load_oracle_table_to_bigquery(oracle_client: Connection, bigquery_client: Client, job_config: JobConfig):
    """Laster data fra en Oracle-tabell til en BigQuery-tabell basert på endringer i en delta-kolonne."""

    dataset = job_config.bigquery_dataset
    try:
        bigquery_client.get_dataset(dataset)
    except NotFound:
        dataset = bigquery_client.create_dataset(dataset)
        logging.info(f"Datasettet {job_config.bigquery_dataset} er opprettet.")

    # Sjekk om tabellen finnes og hent maks teknisk_tid
    maks_kjoretidspunkt_bq = "1900-01-01 00:00:00"
    try:  # hvis tabellen finnes, hent alle nye rader
        query = f"select max({job_config.delta_column_name_bigquery}) as maks_kjoretidspunkt_bq from `{job_config.bigquery_table_id}`"
        results = bigquery_client.query(query).result()
        df = results.to_dataframe()
    except NotFound:
        logging.info(
            f"Tabellen {job_config.bigquery_table_id} finnes ikke i BQ, så oppretter ny tabell med alle rader fra oracle."
        )
        df = pd.DataFrame()

    if not df.empty and pd.notnull(df.iloc[0]["maks_kjoretidspunkt_bq"]):
        maks_kjoretidspunkt_bq = df.iloc[0]["maks_kjoretidspunkt_bq"]
        logging.info(f"Maks kjoretidspunkt i BQ: {maks_kjoretidspunkt_bq}")
    sql_pen_dev = f"""select * from {job_config.oracle_table} 
                    where {job_config.delta_column_name_oracle} > to_timestamp_tz('{maks_kjoretidspunkt_bq}', 'YYYY-MM-DD HH24:MI:SS.FF6TZH:TZM')"""

    df_bq = pesys_utils.df_from_sql(sql_pen_dev, oracle_client)
    bigquery_job_config = LoadJobConfig(
        write_disposition=job_config.write_disposition,
        create_disposition=job_config.create_disposition,
        schema=job_config.schema,
    )


    start = time.time()
    job = bigquery_client.load_table_from_dataframe(df_bq, job_config.bigquery_table_id, job_config=bigquery_job_config)
    job.result()
    end = time.time()

    print(f"{len(df_bq)} rader ble skrevet til bigquery etter {end - start} sekunder.")

    view_query = f"""
    CREATE OR REPLACE VIEW `{job_config.bigquery_view_id}` as
    select * from `{job_config.bigquery_table_id}`
    """
    try:
        bigquery_client.query(view_query).result()
        logging.info(f"View {job_config.bigquery_view_id} opprettet/oppdatert.")
    except Exception as e:
        logging.error(f"Feil ved oppretting/oppdatering av view {job_config.bigquery_view_id}: {e}")