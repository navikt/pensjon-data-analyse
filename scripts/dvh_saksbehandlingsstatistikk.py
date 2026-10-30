import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import gcp_utils, pesys_utils
from utils.oracle_to_bigquery import JobConfig, delta_load_oracle_table_to_bigquery

logging.basicConfig(level=logging.INFO)


ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
if ENVIRONMENT == "prod":
    GCP_PROJECT_ID = "spu-data-innsikt-prod-d663"
    GCP_PROJECT_ID_NUMERIC = "927737712588"
    GCP_SECRET_NAME = "dvh-prod-sky-spu-srv"
    TARGET_PRINCIPAL = "bigquery-airflow-dvh@spu-data-innsikt-prod-d663.iam.gserviceaccount.com"
else:
    raise ValueError(f"Ukjent environment: {ENVIRONMENT}")

# grants for dev-data, se: https://console.cloud.google.com/bigquery?sq=230094999443:8ec4c0a3a32a4bd7b8862be1274fb077
# grants for prod-data går via https://github.com/navikt/wendelboe-bigquery-terraform/blob/main/prod/pen-dataprodukt-dataset.tf
DATASET_NAME = "dvh_sky_spu"
TABLES = {
    "SBX_SAK.FAK_RAPPORT_TORG_MND_SPU": "sky_spu.FAK_RAPPORT_TORG_MND_SPU",
}

if __name__ == "__main__":
    client = gcp_utils.get_bigquery_client(project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL)
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME, project_id=GCP_PROJECT_ID_NUMERIC)
    oracle_client = pesys_utils.connect_to_oracle()
    for BQ_TABLE, ORACLE_TABLE in TABLES.items():
        job_config = JobConfig(
            oracle_table=ORACLE_TABLE,
            bigquery_table=BQ_TABLE,
            gcp_project=GCP_PROJECT_ID,
            bigquery_dataset_name=DATASET_NAME,
            delta_column_name_oracle="innreg_tid", # brukes ikke når det er write_truncate, men må ha en verdi
            delta_column_name_bigquery="innreg_tid", # brukes ikke når det er write_truncate, men må ha en verdi
            write_disposition="WRITE_TRUNCATE", # endre til WRITE_TRUNCATE_DATA med eksplisitt skjema
            bigquery_schema=None,
        )
        delta_load_oracle_table_to_bigquery(
            oracle_client=oracle_client,
            bigquery_client=client,
            job_config=job_config,
        )
        logging.info(f"Ferdig med tabellen {BQ_TABLE}.")
    logging.info("Fullført overføring av data fra Oracle til BigQuery.")
