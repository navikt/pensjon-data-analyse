import os
import sys
import logging
from pathlib import Path
from google.cloud.bigquery import LoadJobConfig

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

# laster opp de tre stonad-alder-tabellene fra Oracle til BQ
# kjører en write_truncate istedenfor inkrementell last. Det er enklere


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
# Tre tabeller fra pen_dataprodukt schema
TABLES = {
    "stonad_alder_vedtak": "pen_dataprodukt.dataprodukt_alderspensjon_vedtak",
    "stonad_alder_beregning": "pen_dataprodukt.dataprodukt_alderspensjon_beregning",
    "stonad_alder_belop": "pen_dataprodukt.dataprodukt_alderspensjon_belop",
}


if __name__ == "__main__":
    # Sett opp tilkoblinger
    pesys_utils.set_db_secrets(secret_name=GCP_SECRET_NAME)
    client = gcp_utils.get_bigquery_client(project=GCP_PROJECT_ID, target_principal=TARGET_PRINCIPAL)
    oracle_client = pesys_utils.connect_to_oracle()

    # Konfigurer BigQuery job
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    # Overfør hver tabell
    for bq_table_name, oracle_table in TABLES.items():
        logging.info(f"Starter overføring av {oracle_table} til {bq_table_name}...")

        # Hent data fra Oracle
        sql = f"select * from {oracle_table}"
        df = pesys_utils.df_from_sql(sql, oracle_client)

        # Last opp til BigQuery
        bq_table_id = f"{GCP_PROJECT_ID}.{DATASET_NAME}.{bq_table_name}"
        job = client.load_table_from_dataframe(df, bq_table_id, job_config=job_config)
        job.result()
        logging.info(f"✓ {len(df)} rader overført til {bq_table_id}")

    oracle_client.close()
    logging.info("Fullført overføring av alle stonad-alder-tabeller fra Oracle til BigQuery.")
