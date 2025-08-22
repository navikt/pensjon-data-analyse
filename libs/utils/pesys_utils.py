import os
import json
import logging
import oracledb
import pandas as pd
from time import time
from google.cloud import secretmanager


def set_db_secrets(secret_name: str):
    """Setter secrets fra GSM som miljøvariabler, for å kunne kjøre connect_to_oracle().
    Hvis DB_USER og DB_PASSWORD er satt allerede for lokal kjøring, så endres de ikke.
    Dette skal være i hemmelighetene, med eksempelverdier for Q2:
    -   "DB_USER": "pen_dataprodukt",
    -   "DB_PORT": "1521",
    -   "DB_PASSWORD": "...",
    -   "DB_SERVICE_NAME": "pen_q2",
    -   "DB_HOST": "d26dbvl012.test.local",
    """
    logging.info(f"Setter miljøvariabl-hemmeligheter fra: {secret_name}")
    full_secret_name = f"projects/230094999443/secrets/{secret_name}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": full_secret_name})
    secret = json.loads(response.payload.data.decode("UTF-8"))
    os.environ["DB_HOST"] = secret["DB_HOST"]
    os.environ["DB_PORT"] = secret["DB_PORT"]
    os.environ["DB_SERVICE_NAME"] = secret["DB_SERVICE_NAME"]
    # hvis DB_USER eksisterer som miljøvariabel, bruk den, ellers bruk den fra secret
    os.environ["DB_USER"] = os.environ.get("DB_USER", secret["DB_USER"])
    os.environ["DB_PASSWORD"] = os.environ.get("DB_PASSWORD", secret["DB_PASSWORD"])
    logging.info(
        f"Secrets for {os.environ['DB_USER']} mot {os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_SERVICE_NAME']}"
    )


def connect_to_oracle():
    """Bruker miljøvariabler for å koble til en Oracle-database.
    Kombiner med set_db_secrets(secret_name) for å sette hemmeligheter fra GSM."""
    con = oracledb.connect(
        port=os.environ["DB_PORT"],
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        service_name=os.environ["DB_SERVICE_NAME"],
    )
    logging.info(f"Brukeren {os.environ['DB_USER']} er koblet til {os.environ['DB_HOST']}")
    return con


def df_from_sql(sql: str, connection: oracledb.Connection):
    """Henter data med en sql-spørring og returnerer en pandas.DataFrame."""
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [col[0].lower() for col in cursor.description]
        data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    logging.info(f"Hentet {len(df)} rader")
    return df


def pandas_from_sql(sqlfile, con, tuning=None, lowercase=False):
    with con.cursor() as cursor:
        start = time()
        if tuning:
            cursor.prefetchrows = tuning
            cursor.arraysize = tuning
        with open(sqlfile) as sql:
            cursor.execute(sql.read())
        df = pd.DataFrame(cursor.fetchall())
        end = time()
        logging.info(f"{len(df)} rader ble returnert etter {end - start} sekunder.")
        if len(df) > 0:
            if lowercase:
                df.columns = [x[0].lower() for x in cursor.description]
            else:
                df.columns = [x[0] for x in cursor.description]
        return df
