import requests
import json
from pathlib import Path
import pandas as pd
from io import StringIO


def get_df_befolkning(year: int = 2025):
    query_path = Path(__file__).parent / "ssb_queries.json"
    query = json.loads(query_path.read_text())["befolkning"]
    query["body"]["query"].append({"code": "Tid", "selection": {"filter": "item", "values": [str(year)]}})
    resp = requests.post(
        query["url"],
        headers={"Content-Type": "application/json"},
        data=json.dumps(query["body"]),
    )

    csv_data = StringIO(resp.text)
    df = pd.read_csv(csv_data, sep=",")
    df["alder"] = df["alder"].apply(lambda x: int(x.split(" ")[0]) if isinstance(x, str) else x)
    df = df.rename(columns={f"Personer {year}": "befolkningsantall"})
    return df
