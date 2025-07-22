import logging
from google.cloud.bigquery import Client, LoadJobConfig

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils

# Metabase, se https://metabase.ansatt.nav.no/dashboard/737-etteroppgjoret?tab=468-tidslinje
bq_eo_oversikt = "pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_oversikt"
bq_eo_oversikt_per_dag = "pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_oversikt_per_dag"
bq_eo_varselbrev_sluttresultat = "pensjon-saksbehandli-prod-1f83.etteroppgjoret.eo_varselbrev_sluttresultat"

logging.basicConfig(level=logging.INFO)

# oracle
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
tuning = 10000
con = pesys_utils.connect_to_oracle()
df_eo_oversikt = pesys_utils.pandas_from_sql(
    sqlfile="../sql/eo_oversikt.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
df_eo_oversikt_per_dag = pesys_utils.pandas_from_sql(
    sqlfile="../sql/eo_oversikt_per_dag.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
df_eo_varselbrev_sluttresultat = pesys_utils.pandas_from_sql(
    sqlfile="../sql/eo_varselbrev_sluttresultat.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
con.close()


# bigquery
client = Client(project="pensjon-saksbehandli-prod-1f83")
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)

run_job = client.load_table_from_dataframe(df_eo_oversikt, bq_eo_oversikt, job_config=job_config)
run_job.result()

run_job2 = client.load_table_from_dataframe(df_eo_oversikt_per_dag, bq_eo_oversikt_per_dag, job_config=job_config)
run_job2.result()

run_job3 = client.load_table_from_dataframe(
    df_eo_varselbrev_sluttresultat, bq_eo_varselbrev_sluttresultat, job_config=job_config
)
run_job3.result()

logging.info("Data lastet opp til BigQuery.")
