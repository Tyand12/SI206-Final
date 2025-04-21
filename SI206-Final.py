#Final 206
#Names: Jolie Oleshansky, Tess Willison, Ty Anderson

import requests
import sqlite3
import time

API_KEY = 'HdYK5LcdRVbHofrCvKfsXR4yErKnMBss'
DB_NAME = 'combined_data.db'

CITIES = [
    'Bozeman, MT',
    'Pullman, WA',
    'Gainesville, FL',
    'Boone, NC',
    'Clemson, SC',
    'East Lansing, MI',
    'Moscow, ID',
    'Provo, UT',
    'Ann Arbor, MI',
    'Stanford, CA',
    'Bloomington, IN',
    'Athens, OH',
    'Ellensburg, WA',
    'San Luis Obispo, CA',
    'College Station, TX',
    'Laramie, WY',
    'Amherst, MA',
    'Manhattan, KS',
    'West Lafayette, IN',
    'Platteville, WI',
    'Ames, IA',
    'Iowa City, IA',
    'Madison, WI',
    'Chapel Hill, NC',
    'Berkeley, CA',
    'Boulder, CO',
    'Eugene, OR',
    'Athens, GA',
    'Lawrence, KS',
    'Oxford, MS',
    'Corvallis, OR',
    'Tempe, AZ',
    'Charlottesville, VA',
    'Columbia, MO',
    'State College, PA',
    'Lexington, KY',
    'Tallahassee, FL',
    'Providence, RI',
    'Santa Cruz, CA',
    'New Haven, CT',
    'Durham, NC',
    'Cambridge, MA',
    'Ithaca, NY',
    'Princeton, NJ',
    'Hanover, NH',
    'Middlebury, VT',
    'Swarthmore, PA',
    'Gambier, OH',
    'Oberlin, OH',
    'Grinnell, IA',
    'Saratoga Springs, NY',
    'Claremont, CA',
    'Walla Walla, WA',
    'Davidson, NC',
    'Sewanee, TN',
    'Meadville, PA',
    'Wooster, OH',
    'Northfield, MN',
    'Decorah, IA',
    'Poughkeepsie NY',
    'Schenectady, NY',
    'Brunswick, ME',
    'Lewiston, ME',
    'Haverford, PA',
    'Worcester, MA',
    'Waltham, MA',
    'Middletown, CT',
    'Clinton, NY',
    'Canton, NY',
    'Waterville, ME',
    'Northampton, MA',
    'Greencastle, IN',
    'Crawfordsville, IN',
    'Delaware, OH',
    'Granville, OH',
    'Lexington, VA',
    'Staunton, VA',
    'Harrisonburg, VA',
    'Fredericksburg, VA',
    'Farmville, VA',
    'Radford, VA',
    'Fayetteville, AR',
    'Norman, OK',
    'Stillwater, OK',
    'Starkville, MS',
    'Missoula, MT',
    'Brookings, SD',
    'Vermillion, SD',
    'Murray, KY',
    'Hattiesburg, MS',
    'Valdosta, GA',
    'Hilo, HI',
    'Grand Forks, ND',
    'Fargo, ND',
    'Carbondale, IL',
    'Normal, IL',
    'Cape Girardeau, MO',
    'Terre Haute, IN',
    'Huntington, WV',
    'Bowling Green, KY',
    'Johnson City, TN'
]

def reset_weather_data():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('DELETE FROM Weather')
    conn.commit()
    conn.close()
    print("Database has been reset.")

# Create table if it doesn't already exist
def create_weather_table():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT, 
            location_key TEXT,
            observation_time TEXT,
            temperature REAL,
            weather_text TEXT,
            is_daytime INTEGER,
            UNIQUE(city, observation_time)
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Weather100 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT, 
            location_key TEXT,
            observation_time TEXT,
            temperature REAL,
            weather_text TEXT,
            is_daytime INTEGER,
            UNIQUE(city, observation_time)
        )
    ''')
    conn.commit()
    conn.close()

# Get location key for a city
def get_location_key(city):
    url = "http://dataservice.accuweather.com/locations/v1/cities/search"
    params = {'apikey': API_KEY, 'q': city}
    response = requests.get(url, params=params)
    if response.status_code == 200 and response.json():
        return response.json()[0]['Key']
    else:
        print(f"Failed to get locationKey for {city}")
        return None

# Get current weather for a location key
def fetch_weather_data(location_key):
    url = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
    params = {'apikey': API_KEY, 'details': 'true'}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()[0]
        return {
            'observation_time': data['LocalObservationDateTime'],
            'temperature': data['Temperature']['Metric']['Value'],
            'weather_text': data['WeatherText'],
            'is_daytime': int(data['IsDayTime'])
        }
    else:
        print("Error fetching weather data.")
        return None

# Convert Celsius to Fahrenheit
def celsius_to_fahrenheit(celsius):
    return (celsius * 9/5) + 32

def store_weather_data_100(max_new=100, total_limit=100):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check current total row count
    cur.execute('SELECT COUNT(*) FROM Weather100')
    current_count = cur.fetchone()[0]

    if current_count >= total_limit:
        print("Limit of 100 rows already reached.")
        conn.close()
        return

    rows_added = 0
    for city in CITIES:
        if rows_added >= max_new or current_count + rows_added >= total_limit:
            break

        location_key = get_location_key(city)
        if not location_key:
            continue

        # Check if data for this city and observation time already exists
        cur.execute('''
            SELECT 1 FROM Weather100 WHERE city = ? AND observation_time = ?
        ''', (city, location_key))
        if cur.fetchone():
            print(f"Data for {city} already exists.")
            continue

        weather = fetch_weather_data(location_key)
        if weather:
            try:
                temperature_fahrenheit = celsius_to_fahrenheit(weather['temperature'])

                cur.execute('''
                    INSERT OR IGNORE INTO Weather100 
                    (city, location_key, observation_time, temperature, weather_text, is_daytime)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    city, location_key, weather['observation_time'], temperature_fahrenheit,
                    weather['weather_text'], weather['is_daytime']
                ))
                rows_added += 1
                print(f"Added: {city} | Total: {current_count + rows_added}")
                time.sleep(1)  # Avoid rate limits
            except Exception as e:
                print(f"Insert failed for {city}: {e}")

# Store up to max_new new records, total limit total_limit
def store_weather_data(max_new=20, total_limit=100):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Check current total row count
    cur.execute('SELECT COUNT(*) FROM Weather')
    current_count = cur.fetchone()[0]

    if current_count >= total_limit:
        print("Limit of 100 rows already reached.")
        conn.close()
        return

    rows_added = 0
    for city in CITIES:
        if rows_added >= max_new or current_count + rows_added >= total_limit:
            break

        location_key = get_location_key(city)
        if not location_key:
            continue

        # Check if data for this city and observation time already exists
        cur.execute('''
            SELECT 1 FROM Weather WHERE city = ? AND observation_time = ?
        ''', (city, location_key))
        if cur.fetchone():
            print(f"Data for {city} already exists.")
            continue

        weather = fetch_weather_data(location_key)
        if weather:
            try:
                temperature_fahrenheit = celsius_to_fahrenheit(weather['temperature'])

                cur.execute('''
                    INSERT OR IGNORE INTO Weather 
                    (city, location_key, observation_time, temperature, weather_text, is_daytime)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    city, location_key, weather['observation_time'], temperature_fahrenheit,
                    weather['weather_text'], weather['is_daytime']
                ))
                rows_added += 1
                print(f"Added: {city} | Total: {current_count + rows_added}")
                time.sleep(1)  # Avoid rate limits
            except Exception as e:
                print(f"Insert failed for {city}: {e}")


    # Commit all changes at once for efficiency
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_weather_table()
    store_weather_data(20, 100)
