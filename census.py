import requests
import sqlite3

api_key = "a422cf66857dbe08ca51d9e5874ab31ed39fce06"

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
    'Platteville': ('63250', '55', 'WI'),
    'Ames': ('01855', '19', 'IA'),
    'Iowa City': ('38595', '19', 'IA'),
    'Madison': ('48000', '55', 'WI'),
    'Chapel Hill': ('11800', '37', 'NC'),
    'Berkeley': ('06000', '06', 'CA'),
    'Boulder': ('07850', '08', 'CO'),
    'Eugene': ('23850', '41', 'OR'),
    'Athens': ('03440', '13', 'GA'),
    'Lawrence': ('38900', '20', 'KS'),
    'Oxford': ('54840', '28', 'MS'),
    'Corvallis': ('15800', '41', 'OR'),
    'Tempe': ('73000', '04', 'AZ'),
    'Charlottesville': ('14968', '51', 'VA'),
    'Columbia': ('15670', '29', 'MO'),
    'State College': ('73808', '42', 'PA'),
    'Lexington': ('46027', '21', 'KY'),
    'Tallahassee': ('70600', '12', 'FL'),
    'Providence': ('59000', '44', 'RI'),
    'Santa Cruz': ('69112', '06', 'CA'),
    'New Haven': ('52000', '09', 'CT'),
    'Durham': ('19000', '37', 'NC'),
    'Cambridge': ('11000', '25', 'MA'),
    'Ithaca': ('38077', '36', 'NY'),
    'Princeton': ('60900', '34', 'NJ'),
    'Hanover': ('33780', '33', 'NH'),
    'Middlebury': ('44275', '50', 'VT'),
    'Swarthmore': ('75648', '42', 'PA'),
    'Gambier': ('29246', '39', 'OH'),
    'Oberlin': ('57834', '39', 'OH'),
    'Grinnell': ('33105', '19', 'IA'),
    'Saratoga Springs': ('65255', '36', 'NY'),
    'Claremont': ('13756', '06', 'CA'),
    'Walla Walla': ('75775', '53', 'WA'),
    'Davidson': ('16400', '37', 'NC'),
    'Sewanee': ('67140', '47', 'TN'),
    'Meadville': ('48360', '42', 'PA'),
    'Wooster': ('86548', '39', 'OH'),
    'Northfield': ('46924', '27', 'MN'),
    'Decorah': ('19405', '19', 'IA'),
    'Poughkeepsie': ('59641', '36', 'NY'),
    'Schenectady': ('65508', '36', 'NY'),
    'Brunswick': ('08395', '23', 'ME'),
    'Lewiston': ('38740', '23', 'ME'),
    'Haverford': ('33154', '42', 'PA'),
    'Worcester': ('82000', '25', 'MA'),
    'Waltham': ('72600', '25', 'MA'),
    'Middletown': ('47290', '09', 'CT'),
    'Clinton': ('16419', '36', 'NY'),
    'Canton': ('12331', '36', 'NY'),
    'Waterville': ('80740', '23', 'ME'),
    'Northampton': ('46330', '25', 'MA'),
    'Greencastle': ('29358', '18', 'IN'),
    'Crawfordsville': ('15742', '18', 'IN'),
    'Delaware': ('21434', '39', 'OH'),
    'Granville': ('31402', '39', 'OH'),
    'Lexington': ('45512', '51', 'VA'),
    'Staunton': ('75216', '51', 'VA'),
    'Harrisonburg': ('35624', '51', 'VA'),
    'Fredericksburg': ('29744', '51', 'VA'),
    'Farmville': ('27440', '51', 'VA'),
    'Radford': ('65392', '51', 'VA'),
    'Fayetteville': ('23290', '05', 'AR'),
    'Norman': ('52500', '40', 'OK'),
    'Stillwater': ('70300', '40', 'OK'),
    'Starkville': ('70240', '28', 'MS'),
    'Missoula': ('23200', '30', 'MT'),
    'Brookings': ('07580', '46', 'SD'),
    'Vermillion': ('67020', '46', 'SD'),
    'Murray': ('54642', '21', 'KY'),
    'Hattiesburg': ('31020', '28', 'MS'),
    'Valdosta': ('78800', '13', 'GA'),
    'Hilo': ('14650', '15', 'HI'),
    'Grand Forks': ('32060', '38', 'ND'),
    'Fargo': ('25700', '38', 'ND'),
    'Carbondale': ('11163', '17', 'IL'),
    'Normal': ('53234', '17', 'IL'),
    'Cape Girardeau': ('11242', '29', 'MO'),
    'Terre Haute': ('75428', '18', 'IN'),
    'Huntington': ('39460', '54', 'WV'),
    'Bowling Green': ('08902', '21', 'KY'),
    'Johnson City': ('38320', '47', 'TN')
}
    

def setup_database(db_name="city_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS census_100 (
            city TEXT,
            population INTEGER,
            place_code TEXT,
            state_code TEXT
        )
    ''')
    return conn, cursor

def get_city_population(place_code, state_code):
    acs_base_url = 'https://api.census.gov/data/2021/acs/acs5'
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
    
# def get_tiger_location(place_code, state_code):    
#     tiger_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Places_CouSub_ConCity_SubMCD/MapServer/0/query"
#     params = {
#         "where": f"STATEFP='{state_code}' AND PLACEFP='{place_code}'",
#         "outFields": "NAME,STATEFP,PLACEFP",
#         "returnGeometry": "true",
#         "f": "json"
#     }
#     response = requests.get(tiger_url, params=params)
#     print(response.url)
#     if response.status_code == 200:
#         data = response.json()
#         try:
#             geometry = data['features'][0]['geometry']
#             lon = geometry['x']
#             lat = geometry['y']
#             return (lat, lon)
#         except:
#             return (None, None)
#     else:
#         return (None, None)

# def populate_database(cities, conn, cursor):
#     for city_name, (place_code, state_code, state_abbrev) in cities.items():
#         population = get_city_population(place_code, state_code)
#         lat, lon = get_tiger_location(place_code, state_code)

#         if population is not None and lat is not None and lon is not None:
#             city_name = f"{city_name}, {state_abbrev}"
#             cursor.execute('''
#                 INSERT OR REPLACE INTO city_stats (city, population, latitude, longitude, place_code, state_code)
#                 VALUES (?, ?, ?, ?, ?, ?)
#             ''', (city_name, population, lat, lon, place_code, state_code))
#     conn.commit()

def populate_database(cities, conn, cursor):
    for city_name, (place_code, state_code, state_abbrev) in cities.items():
        population = get_city_population(place_code, state_code)
        if population is not None:
            city_name = f"{city_name}, {state_abbrev}"
            cursor.execute('''
                INSERT OR REPLACE INTO census_100 (city, population, place_code, state_code)
                VALUES (?, ?, ?, ?)
            ''', (city_name, population, place_code, state_code))
    conn.commit()


def create_limited_table(cities, conn, cursor, limit=20):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS census_20 (
            city TEXT,
            population INTEGER,
            place_code TEXT,
            state_code TEXT
        )
    ''')

    limited_cities = dict(list(cities.items())[:limit])

    for city_name, (place_code, state_code, state_abbrev) in limited_cities.items():
        population = get_city_population(place_code, state_code)
        if population is not None:
            full_name = f"{city_name}, {state_abbrev}"
            cursor.execute('''
                INSERT OR REPLACE INTO census_20 (city, population, place_code, state_code)
                VALUES (?, ?, ?, ?)
            ''', (full_name, population, place_code, state_code))

    conn.commit()


def main():
    conn, cursor = setup_database()
    populate_database(cities, conn, cursor)
    create_limited_table(cities, conn, cursor)
    conn.close()


main()
