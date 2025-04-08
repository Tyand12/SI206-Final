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
        data = response.json()[]
        return {
            'observation_time': data['LocalObservationDataTime'],
            'temperature': data['Temperature']['Metric']['Value'],
            'weather_text': data['WeatherText'],
            'is_daytime': int(data['IsDayTime'])
        }
    else:
        print("Error fetching weather data.")
        return None
    
    