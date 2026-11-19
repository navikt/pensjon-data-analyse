import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import gcp_utils, pesys_utils
from utils.oracle_to_bigquery import JobConfig, load_oracle_table_to_bigquery
from utils.schemas import DefaultSchemas

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
TABLES = [
    {
        "bigquery_table": "dvh_sbs__behandling_aarsak_dim_mv",
        "oracle_table": "DVH_SBS.BEHANDLING_AARSAK_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dvh_sbs__behandling_metode_dim_mv",
        "oracle_table": "DVH_SBS.BEHANDLING_METODE_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dvh_sbs__behandling_resultat_dim_mv",
        "oracle_table": "DVH_SBS.BEHANDLING_RESULTAT_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dvh_sbs__behandling_status_dim_mv",
        "oracle_table": "DVH_SBS.BEHANDLING_STATUS_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dvh_sbs__behandling_type_dim_mv",
        "oracle_table": "DVH_SBS.BEHANDLING_TYPE_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dvh_sbs__sak_ytelse_dim_mv",
        "oracle_table": "DVH_SBS.SAK_YTELSE_DIM_MV",
        "schema": None,
    },
    {
        "bigquery_table": "dt_kodeverk__dim_org",
        "oracle_table": "DT_KODEVERK.DIM_ORG",
        "schema": None,
    },
    {
        "bigquery_table": "sbx_sak__spu_analyse",
        "oracle_table": "sbx_sak.spu_analyse",  # dvh_sbs.uforetrygd_analyse
        "schema": DefaultSchemas.SAKSBEHANDLING_ANALYSE.value,
    },
]

if __name__ == "__main__":
    client = gcp_utils.get_bigquery_client(project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL)
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME, project_id=GCP_PROJECT_ID_NUMERIC)
    oracle_client = pesys_utils.connect_to_oracle()
    for table_config in TABLES:
        job_config = JobConfig(
            oracle_table=table_config["oracle_table"],
            bigquery_table=table_config["bigquery_table"],
            gcp_project=GCP_PROJECT_ID,
            bigquery_dataset_name=DATASET_NAME,
            write_disposition="WRITE_TRUNCATE", # endre til WRITE_TRUNCATE_DATA med eksplisitt skjema
            bigquery_schema=table_config["schema"],
        )
        load_oracle_table_to_bigquery(
            oracle_client=oracle_client,
            bigquery_client=client,
            job_config=job_config,
        )
        logging.info(f"Ferdig med tabellen {table_config['bigquery_table']}.")
    logging.info("Fullført overføring av data fra Oracle til BigQuery.")
