import pandas as pd
import os
import psycopg2

from datetime import datetime
from sqlalchemy import create_engine
from google.cloud.bigquery import Client, LoadJobConfig, SchemaField, enums

from lib import utils


def update_dataproduct():
    utils.set_secrets_as_env(split_on=":", secret_name='projects/193123067890/secrets/pensjon-saksbehandling-nh4b/versions/latest')
    df = df_from_postgres()
    df = prepare_df(df)
    append_to_bq(df)


def df_from_postgres():
    method = "postgresql+psycopg2"
    host = "A01DBVL028.adeo.no:5432"
    db_name = "pensjon-psak"

    alchemyEngine = create_engine(f'{method}://{os.environ["PSAK_POSTGRES_USER"]}:{os.environ["PSAK_POSTGRES_PASSWORD"]}@{host}/{db_name}')
    dbConnection = alchemyEngine.connect()

    return pd.read_sql(
        """
        select bruk_nytt_design
        from t_saksbeh_preferanser;
        """,
        dbConnection)


def prepare_df(df):
    df["Andel_nytt"] = (df.Nytt / df.sum(axis=1)).apply(lambda x: round(x, 3))
    df["Uttrekk_tidspunkt"] = datetime.datetime.utcnow()

    df = (
        pd.DataFrame([
            df_pref[df_pref.bruk_nytt_design == True].count(),
            df_pref[df_pref.bruk_nytt_design == False].count(),
            df_pref[pd.isna(df_pref.bruk_nytt_design)].count()
        ])
        .T
        .rename(columns={0: "Nytt", 1: "Gammelt", 2: "Uspesifisert"})
    )
    return df
    

def append_to_bq(df):
    table_id = f'pensjon-saksbehandli-prod-1f83.saksbehandling_psak.nytt_gammelt_design'
    job_config = LoadJobConfig(
        schema = [
            SchemaField("Uspesifisert", enums.SqlTypeNames.INTEGER),
            SchemaField("Gammelt", enums.SqlTypeNames.INTEGER),
            SchemaField("Nytt", enums.SqlTypeNames.INTEGER),
            SchemaField("Uttrekk_tidspunkt", enums.SqlTypeNames.TIMESTAMP),
            SchemaField("Andel_nytt", enums.SqlTypeNames.FLOAT),
        ],
        write_disposition="WRITE_APPEND",
    )

    client = Client(project="pensjon-saksbehandli-prod-1f83", credentials=credentials)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print(f"Table {table_id} successfully updated")

update_dataproduct()