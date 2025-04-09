import requests
import pandas as pd
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"
acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'

# Parameters for the API request
params = {
    'get': 'B01003_001E,NAME',
    'for': 'tract:*',
    'in': 'state:26+county:161',
    'key': api_key
}

# Make the API request
response = requests.get(acs_base_url, params=params)
print(response.url)

if response.status_code == 200:
    print(response.text)

    try:
        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df['B01003_001E'] = pd.to_numeric(df['B01003_001E'], errors='coerce')
        df['ALAND'] = pd.to_numeric(df['ALAND'], errors='coerce')
        df['density_km2'] = df['B01003_001E'] / (df['ALAND'] / 1_000_000)

        top_25 = df.sort_values(by='density_km2', ascending=False).head(25)

        conn = sqlite3.connect("ann_arbor_density.db")
        top_25.to_sql("tract_density", conn, if_exists="replace", index=False)

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print("Tables in DB:", cursor.fetchall())

        df_check = pd.read_sql("SELECT * FROM tract_density", conn)
        print(df_check.head())

        conn.close()
    except:
        print("Error parsing JSON")


