import requests
import pandas as pd
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"
acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'

params = {
    'get': 'B01003_001E,NAME',
    'for': 'tract:*',
    'in': 'state:26+county:161',
    'key': api_key
}

acs_response = requests.get(acs_base_url, params=params)
print(acs_response.url)

if acs_response.status_code == 200:
    print(acs_response.text)

    acs_data = acs_response.json()
    acs_df = pd.DataFrame(acs_data[1:], columns=acs_data[0])
    print(acs_df)

tiger_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/6/query"

tiger_params = {
    "where": 'STATE="26" AND COUNTY="161"',
    "outFields": "STATE,COUNTY,TRACT,CENTLAT,CENTLON",
    "f": "json"
}

tiger_response = requests.get(tiger_url, params=tiger_params)
print(tiger_response.url)
print(tiger_response.text)
tiger_data = tiger_response.json()
tiger_records = [f["attributes"] for f in tiger_data["features"]]
tiger_df = pd.DataFrame(tiger_records)


acs_df["state"] = acs_df["state"].astype(str)
acs_df["county"] = acs_df["county"].astype(str)
acs_df["tract"] = acs_df["tract"].astype(str)

tiger_df["STATE"] = tiger_df["STATE"].astype(str)
tiger_df["COUNTY"] = tiger_df["COUNTY"].astype(str)
tiger_df["TRACT"] = tiger_df["TRACT"].astype(str)

merged = pd.merge(
    acs_df,
    tiger_df,
    how="inner",
    left_on=["state", "county", "tract"],
    right_on=["STATE", "COUNTY", "TRACT"]
)

merged.rename(columns={
    "B01003_001E": "population",
    "CENTLAT": "latitude",
    "CENTLON": "longitude"
}, inplace=True)

final_df = merged[["NAME", "state", "county", "tract", "population", "latitude", "longitude"]]
final_df["population"] = pd.to_numeric(final_df["population"], errors="coerce")

conn = sqlite3.connect("ann_arbor_with_geo.db")
final_df.to_sql("tract_data", conn, if_exists="replace", index=False)

conn.close()