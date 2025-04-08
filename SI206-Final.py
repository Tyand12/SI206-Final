#Final 206
#Names: Jolie Oleshansky, Tess Willison, Ty Anderson

import requests
import sqlite3
import time

#Jolie

API_KEY = 'meIGG1cfYGouCKivoLbDcUcmvardMj6p'
CITY = 'ANn Arbor'
DB_NAME = 'weather_traffic.db'

#create table if it doesn't already exist
def create_weather_table():
    #connect to or create database
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    #create table for weather data
    cur.execute('''
            CREATE TABLE IF NOT EXISTS Weather (
               id INTEGER PRIMARY KEY AUTOINCREMENT.
                city TEXT, 
                location_key TEXT,
                observation_time TEXT,
                temperatire REAL,
                weather_text TEXT,
                is_daytime INTEGER,
                UNIQUE(city, observation_time)
                )
    ''')
    #save table changes
    conn.commit()
    #close connection to database
    conn.close()
#get AccuWeather location key for Ann Arbor
def get_location_key(city):
    url = "http://dataservice.accuweather.com/locations/v1/cities/search"
    params = {'apikey': API_KEY, 'q': city}
    response = requests.get(url, params=params)
    if response.status_code == 200 and response.json():
        #use the first match
        return response.json()[0]['Key']
    else:
        print(f"Failed to get locationKey for {city}")
        return None
    
#use the location key to fetch current weather data
def fetch_weather_data(location_key):
    url = f"http://dataservice.accuweather.com/currentconditions/v1/{location_key}"
    #add details for more info
    params = {'apikey': API_KEY, 'details': 'true'}

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()[0]
        return {
            'observation_time': data['LocalObservationDataTime'],
            'temperature': data['Temperature']['Metric']['Value'],
            'weather_text': data['WeatherText'],
            'is_daytime': int(data['IsDayTime'])
        }
    else:
        print("Error fetching weather data.")
        return None
    
#store up to 20 new weather records w/o exceeding total of 100
def store_weather_data(max_new=20, total_limit=100):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    #check how many AA rows we already have
    cur.execute('SELECT COUNT(*) FROM Weather WHERE city = ?', (CITY,))
    current_count = cur.fetchone()[0]

    if current_count >= total_limit:
        print("Limit of 100 rows already reached.")
        conn.close()
        return
    
    #get location key only once
    location_key = get_location_key(CITY)
    if not location_key:
        conn.close()
        return
    
    rows_added = 0

    while rows_added < max_new and current_count + rows_added < total_limit:
        weather = fetch_weather_data(location_key)
        if weather:
            try:
                #try to insert the new observation and ignore if dup
                cur.execute('''
                    INSERT OR IGNORE INTO Weather
                    (city, location_key, observation_time, temperature, weather_text, is_daytime)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    CITY, location_key, weather['observation_time'], weather['temperature'],
                    weather['weather_text'], weather['is_daytime']
                ))
                conn.commit()
                rows_added += 1
                print(f"Added row {current_count + rows_added}")
                #from chat: wait 1 second to avoid hitting API limits
                time.sleep(1) 
            except:
                print("Insert failed:")

        else:
            break

    conn.close()







if __name__ == '__main__':
    create_weather_table()
    store_weather_data(20, 100)