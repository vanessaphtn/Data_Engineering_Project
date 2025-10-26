import requests, pathlib
CH = "http://clickhouse:8123"
AUTH = ("default", "clickhouse")

def ch_query(sql: str):
    r = requests.post(f"{CH}/", params={"query": sql}, auth=AUTH)
    r.raise_for_status()

def ch_insert_csv(db: str, table: str, csv_path: str, fmt="CSVWithNames"):
    p = pathlib.Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(csv_path)
    with p.open("rb") as fh:
        r = requests.post(
            f"{CH}/?query=INSERT%20INTO%20{db}.{table}%20FORMAT%20{fmt}",
            data=fh, auth=AUTH, headers={"Content-Type": "text/plain"}
        )
        r.raise_for_status()
