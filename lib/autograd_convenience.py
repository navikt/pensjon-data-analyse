import pandas as pd
import operator
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots



# Filtrer df[col_name] med relate, field (f.eks: operator.eq, "Førstegangsbehandling")
def filter_df(df: pd.DataFrame, col_name: str, relate, field: str):
    return df[relate(df[col_name], field)]


# Gjør klar grunnlags-dataframe
def prepare_df(df: pd.DataFrame, ytelse):
    print(f"Fjerner rader opprettet av batch, og beholder kun førstegangsbehandling innenfor {ytelse}. \n")
    print(f"Antall rader totalt: {df.shape[0]}")
    # Fjern saker opprettet av batch
    df = filter_df(df, "Opprettet av", operator.ne, "Batch")
    print(f"Antall rader ikke opprettet av batch: {df.shape[0]}")
    # Velg kun førstegangsbehandling
    df = filter_df(df, "Sakstype Topp", operator.eq, "Førstegangsbehandling")
    print(f"Antall rader førstegangsbehandling: {df.shape[0]}")
    # Velg ytelse
    df = filter_df(df, "Stønadsområde", operator.eq, ytelse)
    print(f"Antall rader for {ytelse}: {df.shape[0]}")

    #Sett typen av "Antall innkomne saker" til numerisk
    df.loc[:,"Antall innkomne saker"] = df["Antall innkomne saker"].apply(pd.to_numeric)
    
    print("""\nSummerer antall innkomne saker gruppert på ["År-måned", "Selvbetjening", "Automatiseringsgrad", "Utenlandstilsnitt"]""")
    # Grupper på interessante variabler
    df_grouped = df.groupby(["År-måned", "Selvbetjening", "Automatiseringsgrad", "Utenlandstilsnitt"], as_index=False)
    df = df_grouped["Antall innkomne saker"].sum().sort_values(["År-måned", "Selvbetjening", "Automatiseringsgrad", "Utenlandstilsnitt"]).reset_index(drop=True)
    
    print("Regner ut totalt antall saker per måned og andel per gruppering")
    df = add_total_and_andel(df)
    
    print(f"Antall rader etter gruppering: {df.shape[0]}")
    
    return df


# Funksjon som regner ut totalt antall saker per dag og andel per gruppering og legger til i df
def add_total_and_andel(df: pd.DataFrame) -> pd.DataFrame: 
    df = pd.merge(left=df, 
                  right=df.groupby("År-måned", as_index=False)["Antall innkomne saker"].sum(),
                  how="inner", 
                  on="År-måned", 
                  suffixes=("", " totalt"))
    df["Andel"] = df["Antall innkomne saker"] / df["Antall innkomne saker totalt"]
    return df


# Filtrer og summer antall innkomne saker gruppert
def sharpen_df(df, filtrer, groups):
    df = df[filtrer].groupby(groups, as_index=False)["Antall innkomne saker"].sum()
    return add_total_and_andel(df)
