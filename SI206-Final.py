#Final 206
#Names: Jolie Oleshansky, Tess Willison, Ty Anderson

import requests
import sqlite3
import time

#Jolie

API_KEY = 'meIGG1cfYGouCKivoLbDcUcmvardMj6p'
CITY = 'ANn Arbor'
DB_NAME = 'weather_traffic.db'

def create_weather_table():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
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
    conn.commit()
    conn.close()

def get_location_key(city):
    url = "http://dataservice.accuweather.com/locations/v1/cities/search"
    params = {'apikey': API_KEY, 'q': city}
    response = requests.get(url, params=params)
    if response.status_code == 200 and response.json():
        return response.json()[0]['Key']
    else:
        print(f"Failed to get locationKey for {city}")
        return None
    
