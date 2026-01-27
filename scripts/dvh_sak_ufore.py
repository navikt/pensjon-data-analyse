import os
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils
from utils.oracle_to_bigquery import delta_load_oracle_table_to_bigquery, JobConfig

logging.basicConfig(level=logging.INFO)


ENVIRONMENT = "prod" if os.getenv("ENVIRONMENT") == "prod" else "dev"

if ENVIRONMENT == "dev":
    GCP_PROJECT_ID = "pensjon-saksbehandli-dev-cb76"
    GCP_SECRET_NAME = "pen-q2-pen_dataprodukt"
    TARGET_PRINCIPAL = "bq-airflow-dev@pensjon-saksbehandli-dev-cb76.iam.gserviceaccount.com"
elif ENVIRONMENT == "prod":
    GCP_PROJECT_ID = "pensjon-saksbehandli-prod-1f83"
    GCP_SECRET_NAME = "pen-prod-lesekopien-pen_dataprodukt"
    TARGET_PRINCIPAL = "bq-airflow-prod@pensjon-saksbehandli-prod-1f83.iam.gserviceaccount.com"
else:
    raise ValueError(f"Ukjent environment: {ENVIRONMENT}")

DATASET_NAME = "saksbehandlingsstatistikk"
TABLE_NAME = "saksbehandlingsstatistikk_ufore"
ORACLE_TABLE = "pen_dataprodukt.behandlingsstatistikk_meldinger"


# grants for dev-data, se: https://console.cloud.google.com/bigquery?sq=230094999443:8ec4c0a3a32a4bd7b8862be1274fb077
# grants for prod-data går via Datamarkedplassen

if __name__ == "__main__":
    
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME)
    # bigquery
    # client = gcp_utils.get_bigquery_client(project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com")
    client = gcp_utils.get_bigquery_client(
        project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL
    )

    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME)
    oracle_client = pesys_utils.connect_to_oracle()

    job_config = JobConfig(
        oracle_table="pen_dataprodukt.behandlingsstatistikk_meldinger",
        delta_column_name_oracle="teknisk_tid",
        gcp_project=GCP_PROJECT_ID,
        bigquery_dataset_name=DATASET_NAME,
        bigquery_table=TABLE_NAME,
        delta_column_name_bigquery="teknisk_tid",
    )
    delta_load_oracle_table_to_bigquery(
        oracle_client=oracle_client,
        bigquery_client=client,
        job_config=job_config,
    )
    logging.info("Fullført overføring av data fra Oracle til BigQuery.")