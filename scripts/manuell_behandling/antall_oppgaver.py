import sys
from pathlib import Path
from google.cloud.bigquery import LoadJobConfig

from scripts.manuell_behandling.underkategori_mapper import map_underkategori_kode

sys.path.append(str(Path(__file__).parent.parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

# OBS! Dette kjøres som en append i BQ, så ved dobbeltkjøring vil det bli duplikater
PROJECT = "pensjon-saksbehandli-prod-1f83"
ANTALL_OPPG_BQ_TABLL = f"{PROJECT}.manuell_behandling.antall_oppgaver"

tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()

df_antall_oppgaver = pesys_utils.pandas_from_sql(
    "../../sql/manuell_behandling_antall_oppgaver.sql", con=con, tuning=tuning, lowercase=True
)
con.close()

df_antall_oppgaver_mapped = map_underkategori_kode(df_antall_oppgaver)

client = gcp_utils.get_bigquery_client(
    project=PROJECT,
    target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
)

client.load_table_from_dataframe(df_antall_oppgaver_mapped, ANTALL_OPPG_BQ_TABLL, job_config=job_config).result()
