# %%
import pandas as pd
import plotly.express as px
from google.cloud.bigquery import Client

# ved dobbeltkjøring i airflow (eller fra Knast) vil det bli insert to ganger i BQ
# gjelder feks kravstatus og kravstatus_med_kravarsak, som er fra Airflow-DAGen "daglige_dataprodukter"
# tiltenkt manuell kjøring mot BQ

# fremgangsmåte:
# 1. hent alle rader i tabellen på BQ til en df
# 2. plotte sum antall krav per dag for datoer
# 3. finne timestamps med duplikater på samme dato
# 4. slette duplikater i df
# 5. plotte på nytt for å se at duplikater er borte
# 6. liste ut timestamps som skal slettes
# 7. slette duplikater-timestamps i BQ, etter brukerbekreftelse


tabell_kravstatus = "pensjon-saksbehandli-prod-1f83.saksstatistikk.kravstatus"
sql_kravstatus = f"""
select
    dato, -- timestamp
    extract(date from dato) as dag,
    sum(antall) as antall
from `{tabell_kravstatus}`
group by
    dato,
    extract(date from dato)
order by dato"""


bq_client = Client(project="pensjon-saksbehandli-prod-1f83")
df = bq_client.query(sql_kravstatus).to_dataframe()

# %%
# Plotting antall krav per dag for datoer > 2025
fig = px.bar(
    df,
    x="dag",
    y="antall",
    hover_data=["dato"],
)
fig.show()
# %%

# Finn datoer med duplikater
duplikater = df[df.duplicated(subset=["dag"], keep=False)]
print(f"Antall duplikater: {len(duplikater)}")

# %%
# Finn datoer (timestamp) som skal droppes (eldste for hver dag med duplikat)
dupe_dager = df[df.duplicated(subset=["dag"], keep=False)]["dag"].unique()
dato_to_drop = []

for dag in dupe_dager:
    dupe_rows = df[df["dag"] == dag].sort_values(by="dato")
    # behold siste, dropp alle andre
    dato_to_drop.extend(dupe_rows.iloc[:-1]["dato"].tolist())

print("Dato (timestamp) som skal droppes:")
for dato in dato_to_drop:
    print(dato)

# Fjern de eldste duplikatene fra df
df_cleaned = df[~df["dato"].isin(dato_to_drop)]

# %%
# Plotting antall krav per dag for datoer etter fjerning av duplikater
fig_cleaned = px.bar(
    df_cleaned,
    x="dag",
    y="antall",
    hover_data=["dato"],
)
fig_cleaned.show()
# %%

# Slett duplikater i BQ
bekreft = input("Er du sikker på at du vil slette duplikater i BQ? (j/n): ")
if bekreft.lower() == "j":
    if dato_to_drop:
        dato_list_str = ", ".join([f"'{dato}'" for dato in dato_to_drop])
        delete_sql = f"""
        DELETE FROM `{tabell_kravstatus}`
        WHERE dato IN ({dato_list_str})
        """
        bq_client.query(delete_sql)
    else:
        print("Ingen duplikater å slette.")
    print("Duplikater slettet i BQ.")
else:
    print("Ingen duplikater slettet i BQ.")
# %%
