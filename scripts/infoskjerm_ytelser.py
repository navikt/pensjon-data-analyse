import logging
from lib import pesys_utils
from google.cloud.bigquery import Client, LoadJobConfig

# ytelser_antall_kombinasjoner.sql
# ytelser_per_alderskull.sql

logging.basicConfig(level=logging.INFO)

# oracle PEN lesekopien
tuning = 10000
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()
df_ytelser_antall_kombinasjoner = pesys_utils.pandas_from_sql(
    "../sql/ytelser_antall_kombinasjoner.sql",
    con=con,
    tuning=tuning,
    lowercase=True,
)
df_ytelser_per_alderskull = pesys_utils.pandas_from_sql(
    "../sql/ytelser_per_alderskull.sql", con=con, tuning=tuning, lowercase=True
)
con.close()


# bigquery
client = Client(project="wendelboe-prod-801c")
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)

bq_datasett = "wendelboe-prod-801c.infoskjerm"
bq_ytelser_antall_kombinasjoner = f"{bq_datasett}.ytelser_antall_kombinasjoner"
bq_ytelser_per_alderskull = f"{bq_datasett}.ytelser_per_alderskull"


job1 = client.load_table_from_dataframe(
    df_ytelser_antall_kombinasjoner,
    bq_ytelser_antall_kombinasjoner,
    job_config=job_config,
)
job1.result()
logging.info(
    f"{len(df_ytelser_antall_kombinasjoner)} rader lastet opp til {bq_ytelser_antall_kombinasjoner}."
)

job2 = client.load_table_from_dataframe(
    df_ytelser_per_alderskull,
    bq_ytelser_per_alderskull,
    job_config=job_config,
)
job2.result()
logging.info(
    f"{len(df_ytelser_per_alderskull)} rader lastet opp til {bq_ytelser_per_alderskull}."
)

logging.info("Ferdig med datalasting til BigQuery")
