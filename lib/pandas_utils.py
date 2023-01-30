import pandas as pd
from time import time


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
        
        print(f'{len(df)} rad(er) ble returnert etter {end-start} sekunder.')
        
        if len(df) > 0:
            if lowercase:
                df.columns = [x[0].lower() for x in cursor.description]
            else:    
                df.columns = [x[0] for x in cursor.description]
        
        return df


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