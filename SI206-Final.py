#Final 206
#Names: Jolie Oleshansky, Tess Willison, Ty Anderson

import requests
import sqlite3
import time

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
