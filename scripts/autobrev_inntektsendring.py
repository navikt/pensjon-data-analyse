import sys
from pathlib import Path
import logging
from google.cloud.bigquery import LoadJobConfig

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils, gcp_utils

logging.basicConfig(level=logging.INFO)

bq_inntektsendring = "pensjon-saksbehandli-prod-1f83.brev.autobrev_inntektsendring"
# Metabase, se https://metabase.ansatt.nav.no/dashboard/672

# oracle
pesys_utils.set_db_secrets(
    secret_name="pen-prod-pen_dataprodukt"
)  # TODO: bytt tilbake til lesekopien etter brannmuråpning
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_inntektsendring = pesys_utils.pandas_from_sql(
    sqlfile="../sql/autobrev_inntektsendring.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()

# datavask for å få bedre visualisering i Metabase

# BPEN090 navn
df_inntektsendring.loc[
    (df_inntektsendring["opprettet_av"] == "PPEN011") & (df_inntektsendring["maned"] != 1),
    "opprettet_av",
] = "BPEN090"

# BPEN091 navn
df_inntektsendring.loc[
    (df_inntektsendring["opprettet_av"] == "PPEN011") & (df_inntektsendring["maned"] == 1),
    "opprettet_av",
] = "BPEN091"

# BPEN091 brevtype der det ikke er sendt brev
df_inntektsendring.loc[
    (df_inntektsendring["opprettet_av"] == "BPEN091")
    & (df_inntektsendring["maned"] == 1)
    & (df_inntektsendring["brevtype"] == "Manuelt brev eller uten brev")
    & (df_inntektsendring["behandlingstype"] == "auto"),
    "brevtype",
] = "BPEN091 uten endret utbetaling, og da uten brev"

# bigquery
# bigquery
client = gcp_utils.get_bigquery_client(
    project="pensjon-saksbehandli-prod-1f83", target_principal="bq-airflow@wendelboe-prod-801c.iam.gserviceaccount.com"
)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)


run_job = client.load_table_from_dataframe(df_inntektsendring, bq_inntektsendring, job_config=job_config)
run_job.result()
