import requests
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"
acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'
tiger_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/6/query"


cities = {
    'Bozeman': ('08950', '30'),
    'Pullman': ('56625', '53'),
    'Gainesville': ('25175', '12'),
    'Boone': ('07080', '37'),
    'Clemson': ('14950', '45'),
    'East Lansing': ('24120', '26'),
    'Moscow': ('54550', '16'),
    'Provo': ('62470', '49'),
    'Ann Arbor': ('03000', '26'),
    'Stanford': ('73906', '06'),
    'Bloomington': ('05860', '18'),
    'Athens': ('02736', '39'),
    'Ellensburg': ('21240', '53'),
    'San Luis Obispo': ('68154', '06'),
    'College Station': ('15976', '48'),
    'Laramie': ('45050', '56'),
    'Amherst': ('01370', '25'),
    'Manhattan': ('44250', '20'),
    'West Lafayette': ('82862', '18'),
    'Platteville': ('63250', '55')
}

conn = sqlite3.connect("city_data.db")
cursor = conn.cursor()

# Create table for city statistics
cursor.execute('''
    CREATE TABLE IF NOT EXISTS city_stats (
        city TEXT,
        name_from_api TEXT,
        population INTEGER,
        latitude REAL,
        longitude REAL,
        place_code TEXT,
        state_code TEXT
    )
''')

for city_name, (place_code, state_code) in cities.items():
    params = {
        'get': 'B01003_001E,NAME',
        'for': f'place:{place_code}',
        'in': f'state:{state_code}',
        'key': api_key
    }
    response = requests.get(acs_base_url, params=params)
    print(response.url)
    if response.status_code == 200:
        acs_json = response.json()
        population = acs_json[1][0]
        name = acs_json[1][1]
    else:
        print(f"Status: {response.status_code}")
        print(response.text)

    tiger_params = {
        "where": f'STATE={state_code} AND PLACE={place_code}',
        "outFields": "STATE,COUNTY,TRACT,CENTLAT,CENTLON",
        "f": "json"
    }
    tiger_response = requests.get(tiger_url, params=tiger_params)
    print(tiger_response.url)
    if tiger_response.status_code == 200:
        tiger_json = tiger_response.json()
        feature = tiger_json["features"][0]["attributes"]
        lat = feature.get("CENTLAT")
        lon = feature.get("CENTLON")
    else:
        print(f"Status: {tiger_response.status_code}")
        print(tiger_response.text)
    cursor.execute('''
        INSERT INTO city_stats (city, name_from_api, population, latitude, longitude, place_code, state_code)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (city_name, name, population, lat, lon, place_code, state_code))


