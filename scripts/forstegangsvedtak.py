import logging
from google.cloud.bigquery import Client, LoadJobConfig

# from lib import pesys_utils

logging.basicConfig(level=logging.INFO)

# oracle
# pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
# tuning = 10000
# con = pesys_utils.connect_to_oracle()
# df_inntektsendring = pesys_utils.pandas_from_sql(
#     sqlfile="../sql/forstegangsvedtak.sql",
#     con=con,
#     tuning=tuning,
#     lowercase=True,
# )
# con.close()
# %%

# leser fra csv midlertidig
import pandas as pd

df_forstegangsvedtak = pd.read_csv("../data/forstegangsvedtak.csv", sep=";")

df_forstegangsvedtak.loc[df_forstegangsvedtak["vedtakstype"] == "FORGANG", "vedtakstype"] = "Førstegang"
df_forstegangsvedtak.loc[df_forstegangsvedtak["vedtakstype"] == "AVSL", "vedtakstype"] = "Avslag"
df_forstegangsvedtak = df_forstegangsvedtak.sort_values(by=["armaned", "vedtakstype"], ascending=[False, False])
df_forstegangsvedtak["ar"] = df_forstegangsvedtak["ar"].astype(int)

df_alle_vedtak = pd.read_csv("../data/alle_vedtak.csv", sep=";")
df_alle_vedtak = df_alle_vedtak.sort_values(by=["armaned", "vedtakstype"], ascending=[False, False])
df_alle_vedtak["ar"] = df_alle_vedtak["ar"].astype(int)


# %%

# # plotly plot x as armaned, y as antall_vedtak, color as vedtakstype
# import plotly.express as px
# fig = px.bar(
#     df_forstegangsvedtak[df_forstegangsvedtak["ar"] > 2021],
#     x="armaned",
#     y="antall_vedtak",
#     color="vedtakstype",
#     labels={"armaned": "Armaned", "antall vedtak": "Antall vedtak"},
# )
# fig.show()

# fig2 = px.bar(
#     df_alle_vedtak[df_alle_vedtak["ar"] > 2021],
#     x="armaned",
#     y="antall_vedtak",
#     color="vedtakstype",
#     labels={"armaned": "Armaned", "antall vedtak": "Antall vedtak"},
# )
# fig2.show()

# bigquery
bq_prosjekt = "pensjon-saksbehandli-prod-1f83"
client = Client(project=bq_prosjekt)
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)

bq_datasett = f"{bq_prosjekt}.forstegangsvedtak"
bq_target_forstegangsvedtak = f"{bq_datasett}.vedtak"
bq_target_alle_vedtak = f"{bq_datasett}.alle_vedtak"


run_job = client.load_table_from_dataframe(df_forstegangsvedtak, bq_target_forstegangsvedtak, job_config=job_config)
run_job.result()

run_job2 = client.load_table_from_dataframe(df_alle_vedtak, bq_target_alle_vedtak, job_config=job_config)
run_job2.result()
