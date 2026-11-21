import logging
import os
import sys
from pathlib import Path

from google.cloud.bigquery import LoadJobConfig

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import gcp_utils, pesys_utils
from utils.schemas import DefaultSchemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
if ENVIRONMENT == "prod":
    GCP_PROJECT_ID = "spu-data-innsikt-prod-d663"
    GCP_PROJECT_ID_NUMERIC = "927737712588"
    GCP_SECRET_NAME = "dvh-prod-sky-spu-srv"
    TARGET_PRINCIPAL = "bigquery-airflow-dvh@spu-data-innsikt-prod-d663.iam.gserviceaccount.com"
else:
    raise ValueError(f"Ukjent environment: {ENVIRONMENT}")


DATASET_NAME = "dvh_sky_spu"
bigquery_table = "sbx_sak__spu_analyse"
oracle_sql_path = Path(__file__).parent.parent / "sql" / "dvh_uforetrygd_analyse.sql"
schema = DefaultSchemas.SAKSBEHANDLING_ANALYSE.value
table_id = f"{GCP_PROJECT_ID}.{DATASET_NAME}.{bigquery_table}"

if __name__ == "__main__":
    client = gcp_utils.get_bigquery_client(project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL)
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME, project_id=GCP_PROJECT_ID_NUMERIC)
    with pesys_utils.connect_to_oracle() as oracle_client:
        df_sbh = pesys_utils.pandas_from_sql(
            sqlfile=oracle_sql_path,
            con=oracle_client,
            tuning=200_000,
            lowercase=True,
        )
    job_config = LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(df_sbh, table_id, job_config=job_config)
    job.result()
    logger.info(f"{len(df_sbh)} rader ble skrevet til {table_id}.")
    logger.info(f"Ferdig med tabellen {bigquery_table}.")
    logger.info("Fullført overføring av data fra Oracle til BigQuery.")
