# %%
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
# Plotting antall krav per dag for datoer før og etter fjerning av duplikater
fig = px.bar(df, x="dag", y="antall")
fig.update_traces(name="med evt duplikater", showlegend=True)
fig.add_bar(
    x=df_cleaned["dag"],
    y=df_cleaned["antall"],
    name="etter duplikatfjerning",
)
fig.update_layout(title="med evt duplikater og etter duplikatfjerning")
fig.show()
# %%

# Foreslår SQL for sletting av duplikater, som må kjøres manuelt i BQ
if dato_to_drop:
    dato_list_str = ", ".join([f"'{dato}'" for dato in dato_to_drop])
    print(
        f"""Foreslått SQL for sletting av duplikater:\n\n
    delete from `{tabell_kravstatus}`
    where dato in ({dato_list_str})
    \n\n"""
    )
else:
    print("Ingen SQL generert.")
