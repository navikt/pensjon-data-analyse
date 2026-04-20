import os
import sys
import logging
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils
from utils.oracle_to_bigquery import delta_load_oracle_table_to_bigquery, JobConfig

logging.basicConfig(level=logging.INFO)


ENVIRONMENT = "prod" if os.getenv("ENVIRONMENT") == "prod" else "dev"
if ENVIRONMENT == "dev":
    GCP_PROJECT_ID = "pensjon-saksbehandli-dev-cb76"
    GCP_SECRET_NAME = "pen-q2-pen_dataprodukt"
    TARGET_PRINCIPAL = "bq-airflow-dvh@pensjon-saksbehandli-dev-cb76.iam.gserviceaccount.com"
elif ENVIRONMENT == "prod":
    GCP_PROJECT_ID = "pensjon-saksbehandli-prod-1f83"
    GCP_SECRET_NAME = "pen-prod-lesekopien-pen_dataprodukt"
    TARGET_PRINCIPAL = "bq-airflow-dvh@pensjon-saksbehandli-prod-1f83.iam.gserviceaccount.com"
else:
    raise ValueError(f"Ukjent environment: {ENVIRONMENT}")
DATASET_NAME = "stonadsstatistikk"
TABLES = {
    "stonadsstatistikk_alder_belop": "pen_dataprodukt.stonadsstatistikk_alder_belop",
    "stonadsstatistikk_alder_beregning": "pen_dataprodukt.stonadsstatistikk_alder_beregning",
    "stonadsstatistikk_alder_vedtak": "pen_dataprodukt.stonadsstatistikk_alder_vedtak",
    "dataprodukt_ufore_diagnosekoder": "pen_dataprodukt.dataprodukt_ufore_diagnosekoder",
}

if __name__ == "__main__":
    client = gcp_utils.get_bigquery_client(project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL)
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME)
    oracle_client = pesys_utils.connect_to_oracle()
    for BQ_TABLE, ORACLE_TABLE in TABLES.items():
        job_config = JobConfig(
            oracle_table=ORACLE_TABLE,
            bigquery_table=BQ_TABLE,
            gcp_project=GCP_PROJECT_ID,
            bigquery_dataset_name=DATASET_NAME,
            delta_column_name_oracle="kjoretidspunkt",
            delta_column_name_bigquery="kjoretidspunkt",
            bigquery_schema=None,
        )
        delta_load_oracle_table_to_bigquery(
            oracle_client=oracle_client,
            bigquery_client=client,
            job_config=job_config,
        )
        logging.info(f"Ferdig med tabellen {BQ_TABLE}.")
    logging.info("Fullført overføring av data fra Oracle til BigQuery.")
