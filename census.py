import requests
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"
acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'
# tiger_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer/0/query"
# geo_base_url = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress'
cities = {
    'Bozeman': ('08950', '30', 'MT'),
    'Pullman': ('56625', '53', 'WA'),
    'Gainesville': ('25175', '12', 'FL'),
    'Boone': ('07080', '37', 'NC'),
    'Clemson': ('14950', '45', 'SC'),
    'East Lansing': ('24120', '26', 'MI'),
    'Moscow': ('54550', '16', 'ID'),
    'Provo': ('62470', '49', 'UT'),
    'Ann Arbor': ('03000', '26', 'MI'),
    'Stanford': ('73906', '06', 'CA'),
    'Bloomington': ('05860', '18', 'IN'),
    'Athens': ('02736', '39', 'OH'),
    'Ellensburg': ('21240', '53', 'WA'),
    'San Luis Obispo': ('68154', '06', 'CA'),
    'College Station': ('15976', '48', 'TX'),
    'Laramie': ('45050', '56', 'WY'),
    'Amherst': ('01370', '25', 'MA'),
    'Manhattan': ('44250', '20', 'KS'),
    'West Lafayette': ('82862', '18', 'IN'),
    'Platteville': ('63250', '55', 'WI')
}
    

def setup_database(db_name="city_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS city_stats (
            city TEXT,
            population INTEGER,
            place_code TEXT,
            state_code TEXT
        )
    ''')
    return conn, cursor

def get_city_population(place_code, state_code):
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
        population = int(acs_json[1][0])
        return population
    else:
        print(f"ACS Error {response.status_code}: {response.text}")
        return None

def populate_database(cities, conn, cursor):
    for city_name, (place_code, state_code, state_abbrev) in cities.items():
        population = get_city_population(place_code, state_code)
        if population is not None:
            full_city_name = f"{city_name}, {state_abbrev}"
            cursor.execute('''
                INSERT INTO city_stats (city, population, place_code, state_code)
                VALUES (?, ?, ?, ?)
            ''', (full_city_name, population, place_code, state_code))
    conn.commit()

def main():
    conn, cursor = setup_database()
    populate_database(cities, conn, cursor)
    conn.close()

main()