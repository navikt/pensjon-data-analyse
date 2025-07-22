import logging
import pandas as pd
from time import time
from datetime import datetime
from google.cloud.bigquery import Client, LoadJobConfig

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "libs"))
from utils import pesys_utils


logging.basicConfig(level=logging.INFO)
pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_airflow")


def date_to_tertial(date):
    return (date.month - 1) // 4 + 1


def overwrite_dataproduct():
    current_year = datetime.now().year
    N = (current_year + 1) - 2008
    years = [str(current_year - (i - 1)) for i in range(N)]

    overwrite_vedtak(N, years)
    overwrite_krav(N, years)


def overwrite_vedtak(N, years):
    con = pesys_utils.connect_to_oracle()
    with open("../sql/forslag_auto.sql") as sql:
        query = sql.read()

    client = Client(project="pensjon-saksbehandli-prod-1f83")

    table_id = "pensjon-saksbehandli-prod-1f83.vedtak.vedtak_automatisering_v2"
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    tuning = 100000

    with con.cursor() as cursor:
        cursor.prefetchrows = tuning
        cursor.arraysize = tuning

        for i in range(1, N):
            start = time()

            print(f"Henter vedtaksdata fra perioden {years[i]}-{years[i - 1]}.")

            cursor.execute(query.replace("x_year", years[i]).replace("y_year", years[i - 1]))

            df_one_year = pd.DataFrame(cursor.fetchall())

            end = time()

            print(
                f"{len(df_one_year)} rad(er) ble returnert etter {end - start} sekunder for perioden {years[i]}-{years[i - 1]}."
            )

            start = time()

            if len(df_one_year) > 0:
                df_one_year.columns = [x[0].lower() for x in cursor.description]
            else:
                continue

            df_one_year["dato_virk_fom"] = df_one_year["dato_virk_fom"].dt.floor("D")
            df_one_year["tertial"] = df_one_year.dato_virk_fom.apply(lambda date: date_to_tertial(date))

            job = client.load_table_from_dataframe(df_one_year, table_id, job_config=job_config)
            job.result()

            end = time()

            print(
                f"{len(df_one_year)} rad(er) ble skrevet til bigquery etter {end - start} sekunder for perioden {years[i]}-{years[i - 1]}."
            )

            job_config = LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
    con.close()
    print(f"Table {table_id} successfully overwritten")


def overwrite_krav(N, years):
    con = pesys_utils.connect_to_oracle()

    with open("../sql/forslag_auto_krav.sql") as sql:
        query = sql.read()

    client = Client(project="pensjon-saksbehandli-prod-1f83")

    table_id = "pensjon-saksbehandli-prod-1f83.saksstatistikk.krav_automatisering"
    job_config = LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    tuning = 100000

    with con.cursor() as cursor:
        cursor.prefetchrows = tuning
        cursor.arraysize = tuning

        for i in range(1, N):
            start = time()

            print(f"Henter kravdata fra perioden {years[i]}-{years[i - 1]}.")

            cursor.execute(query.replace("x_year", years[i]).replace("y_year", years[i - 1]))

            df_one_year = pd.DataFrame(cursor.fetchall())

            end = time()

            print(
                f"{len(df_one_year)} rad(er) ble returnert etter {end - start} sekunder for perioden {years[i]}-{years[i - 1]}."
            )

            if len(df_one_year) > 0:
                df_one_year.columns = [x[0].lower() for x in cursor.description]
            else:
                continue

            df_one_year["dato_opprettet"] = df_one_year["dato_opprettet"].dt.floor("D")
            df_one_year["tertial"] = df_one_year.dato_opprettet.apply(lambda date: date_to_tertial(date))

            job = client.load_table_from_dataframe(df_one_year, table_id, job_config=job_config)
            job.result()

            job_config = LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )
    con.close()
    print(f"Table {table_id} successfully overwritten")


if __name__ == "__main__":
    overwrite_dataproduct()
