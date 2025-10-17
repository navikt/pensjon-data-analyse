import sys
from pathlib import Path
from google.cloud.bigquery import Client
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery.job.load import LoadJob

sys.path.append(str(Path(__file__).parent.parent.parent / "libs"))
from utils import pesys_utils


# dette skriptet speiler 4 syntetiske saker fra pen til bigquery i forbindelse med Magnvards styringsdashbord
target_dataset = "wendelboe-prod-801c.pen_testdata"

# -- t_sak
t_sak = 't_sak'
sql_t_sak = """select * from pen.t_sak
where sak_id in (
    22983576, -- løpende uføre
    22916985, -- alder som var løpende
    23505034, -- alder løpende førstegangsvedtak
    22983695 -- alder løpende regulert vedtak
)"""

# -- t_vedtak
t_vedtak = 't_vedtak'
sql_t_vedtak = """select * from pen.t_vedtak
where sak_id in (22983576, 22916985, 23505034, 22983695)"""

# -- t_kravhode
t_kravhode = 't_kravhode'
sql_t_kravhode = """select * from pen.t_kravhode
where sak_id in (22983576, 22916985, 23505034, 22983695)"""

# -- t_person
t_person = 't_person'
sql_t_person = """select * from pen.t_person
where person_id in (select person_id from pen.t_vedtak where sak_id in (22983576, 22916985, 23505034, 22983695))"""

# -- t_kravlinje
t_kravlinje = 't_kravlinje'
sql_t_kravlinje = """select * from pen.t_kravlinje
where kravhode_id in (select kravhode_id from pen.t_kravhode where sak_id in (22983576, 22916985, 23505034, 22983695))"""

# -- t_vilkar_vedtak, join både vedtak_id og kravlinje_id funker
t_vilkar_vedtak = 't_vilkar_vedtak'
sql_t_vilkar_vedtak = """select * from pen.t_vilkar_vedtak
where vedtak_id in (select vedtak_id from pen.t_vedtak where sak_id in (22983576, 22916985, 23505034, 22983695))"""

# -- t_hendelse
t_hendelse = 't_hendelse'
sql_t_hendelse = """select * from pen.t_hendelse
where sak_id in (22983576, 22916985, 23505034, 22983695)"""

# -- t_kontrollpunkt
t_kontrollpunkt = 't_kontrollpunkt'
sql_t_kontrollpunkt = """select * from pen.t_kontrollpunkt
where sak_id in (22983576, 22916985, 23505034, 22983695)"""

# -- t_k_hendelse_t
t_k_hendelse_t = 't_k_hendelse_t'
sql_t_k_hendelse_t = """select * from pen.t_k_hendelse_t"""
# -- t_k_krav_gjelder
t_k_krav_gjelder = 't_k_krav_gjelder'
sql_t_k_krav_gjelder = """select * from pen.t_k_krav_gjelder"""
# -- t_k_krav_s
t_k_krav_s = 't_k_krav_s'
sql_t_k_krav_s = """select * from pen.t_k_krav_s"""
# -- t_k_kravlinje_t
t_k_kravlinje_t = 't_k_kravlinje_t'
sql_t_k_kravlinje_t = """select * from pen.t_k_kravlinje_t"""
# -- t_k_sak_s
t_k_sak_s = 't_k_sak_s'
sql_t_k_sak_s = """select * from pen.t_k_sak_s"""
# -- t_k_sak_t
t_k_sak_t = 't_k_sak_t'
sql_t_k_sak_t = """select * from pen.t_k_sak_t"""
# -- t_k_vedtak_s
t_k_vedtak_s = 't_k_vedtak_s'
sql_t_k_vedtak_s = """select * from pen.t_k_vedtak_s"""
# -- t_k_vedtak_t
t_k_vedtak_t = 't_k_vedtak_t'
sql_t_k_vedtak_t = """select * from pen.t_k_vedtak_t"""
# -- t_k_vilk_vurd_t
t_k_vilk_vurd_t = 't_k_vilk_vurd_t'
sql_t_k_vilk_vurd_t = """select * from pen.t_k_vilk_vurd_t"""
# -- t_k_vilkar_resul_t
t_k_vilkar_resul_t = 't_k_vilkar_resul_t'
sql_t_k_vilkar_resul_t = """select * from pen.t_k_vilkar_resul_t"""


tabeller = [t_sak, t_vedtak, t_kravhode, t_person, t_kravlinje, t_vilkar_vedtak, t_hendelse, t_kontrollpunkt, t_k_hendelse_t,t_k_krav_gjelder,
    t_k_krav_s, t_k_kravlinje_t, t_k_sak_s, t_k_sak_t, t_k_vedtak_s, t_k_vedtak_t, t_k_vilk_vurd_t, t_k_vilkar_resul_t]
sqler = [ sql_t_sak, sql_t_vedtak, sql_t_kravhode, sql_t_person, sql_t_kravlinje, sql_t_vilkar_vedtak, sql_t_hendelse,
    sql_t_kontrollpunkt, sql_t_k_hendelse_t, sql_t_k_krav_gjelder, sql_t_k_krav_s, sql_t_k_kravlinje_t, sql_t_k_sak_s, sql_t_k_sak_t, sql_t_k_vedtak_s,
    sql_t_k_vedtak_t, sql_t_k_vilk_vurd_t, sql_t_k_vilkar_resul_t]

# oracle
df_liste = []
pesys_utils.set_db_secrets(secret_name="pen-q2-pen_dataprodukt")
tuning = 10000
con = pesys_utils.connect_to_oracle()
for tabell, sql in zip(tabeller, sqler):
    df = pesys_utils.df_from_sql(sql=sql, connection=con)
    df_liste.append(df)
    print(f"{tabell} har {len(df)} rader")
con.close()

# bigquery
bq_client = Client(project="wendelboe-prod-801c")
job_config = LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
)
for df, tabell in zip(df_liste, tabeller):
    if "merknad_liste_preg" in df.columns:
        df = df.drop(columns=["merknad_liste_preg"])
        print(f"fjernet merknad_liste_preg fra {tabell}")
    run_job: LoadJob = bq_client.load_table_from_dataframe(dataframe=df, destination=target_dataset + "." + tabell, job_config=job_config)
    print(f"Ferdig med opplasting av {tabell}, venter på at jobben skal fullføres...")
    run_job.result()
