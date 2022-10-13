import pandas as pd


def pandas_from_sql(filename: str, con):
    with open(filename, 'r') as file:
        query = file.read()
    return pd.read_sql(query, con=con)


def add_zero_to_mnd(x: str):
    if len(x) == 2:
        return x
    elif len(x) == 1:
        return '0' + x
    else:
        raise Exception(f"Wrong format on 'MAANED': {x}")
        
        
def add_zero_to_aar_mnd(x: str):
    if len(x) == 7:
        return x
    elif len(x) == 6:
        return x[:5] + '0' + x[5:]
    else:
        raise Exception(f"Wrong format on 'AAR_MAANED': {x}")