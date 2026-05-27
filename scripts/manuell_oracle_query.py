import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent / "libs"))
from utils import pesys_utils

logging.basicConfig(level=logging.INFO)

# INSERT QUERY HERE
sql = """
select
    sysdate                                                   as sysdate_utc,
    systimestamp                                              as systimestamp_utc,
    systimestamp at time zone 'Europe/Oslo'                   as systimestamp_oslo,
    cast(systimestamp at time zone 'Europe/Oslo' as date)     as oslo_as_date,
    from_tz(cast(sysdate as timestamp), 'UTC')
        at time zone 'Europe/Oslo'                            as from_tz_oslo
from dual
"""

pesys_utils.set_db_secrets(secret_name="pen-prod-lesekopien-pen_dataprodukt")
con = pesys_utils.connect_to_oracle()
df = pesys_utils.df_from_sql(sql=sql, connection=con)
con.close()

print(df.to_string())
