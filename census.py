import requests
import pandas as pd
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"
acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'

# Parameters for the API request
params = {
    'get': 'B01003_001E,NAME,ALAND',
    'for': 'tract:*',
    'in': 'state:26+county:161',
    'key': api_key
}

# Make the API request
response = requests.get(acs_base_url, params=params)

# Check if response is valid
if response.status_code == 200:
    # Print the raw response for debugging
    print(response.text)

    # Attempt to parse the JSON response
    try:
        data = response.json()

        # Check if data is returned
        if len(data) > 1:
            # Convert the response data to DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])

            # Step 4: Clean up and calculate density
            df['B01003_001E'] = pd.to_numeric(df['B01003_001E'], errors='coerce')  # Population
            df['ALAND'] = pd.to_numeric(df['ALAND'], errors='coerce')              # Land area (m²)
            df['density_km2'] = df['B01003_001E'] / (df['ALAND'] / 1_000_000)

            # Optional: Trim to 25 entries
            top_25 = df.sort_values(by='density_km2', ascending=False).head(25)

            # Step 5: Save to SQLite database
            conn = sqlite3.connect("ann_arbor_density.db")  # Creates the DB file locally
            top_25.to_sql("tract_density", conn, if_exists="replace", index=False)

            # Optional: Confirm it worked
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            print("Tables in DB:", cursor.fetchall())

            df_check = pd.read_sql("SELECT * FROM tract_density", conn)
            print(df_check.head())

            conn.close()
        else:
            print("No data returned from the API.")
    except Exception as e:
        print("Error parsing JSON:", e)
else:
    print(f"Request failed with status code {response.status_code}")


