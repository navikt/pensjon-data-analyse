import pandas as pd

def pandas_from_sql(filename: str, con):
    with open(filename, 'r') as file:
        query = file.read()
    return pd.read_sql(query, con=con)