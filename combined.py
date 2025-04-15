import sqlite3
import os

combined_db = "combined_data.db"
traffic_db = "traffic_data.db"
census_db = "city_data.db"
weather_db = "weather_density.db"

def create_combined_db():
    conn = sqlite3.connect(combined_db)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS traffic (
            city TEXT PRIMARY KEY,
            frc TEXT,
            currentSpeed INTEGER,
            freeFlowSpeed INTEGER,
            confidence REAL,
            roadClosure BOOLEAN
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS census (
            city TEXT PRIMARY KEY,
            population INTEGER,
            place_code TEXT,
            state_code TEXT
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            location_key TEXT,
            observation_time TEXT,
            temperature REAL,
            weather_text TEXT,
            is_daytime INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def copy_data():
    conn_combined = sqlite3.connect(combined_db)
    cur_combined = conn_combined.cursor()

    if os.path.exists(traffic_db):
        conn = sqlite3.connect(traffic_db)
        cur = conn.cursor()
        for row in cur.execute("SELECT city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure FROM traffic"):
            cur_combined.execute('''
                INSERT OR REPLACE INTO traffic VALUES (?, ?, ?, ?, ?, ?)
            ''', row)
        conn.close()

    if os.path.exists(census_db):
        conn = sqlite3.connect(census_db)
        cur = conn.cursor()
        for row in cur.execute("SELECT city, population, place_code, state_code FROM city_stats"):
            cur_combined.execute('''
                INSERT OR REPLACE INTO census VALUES (?, ?, ?, ?)
            ''', row)
        conn.close()

    if os.path.exists(weather_db):
        conn = sqlite3.connect(weather_db)
        cur = conn.cursor()
        for row in cur.execute("SELECT city, location_key, observation_time, temperature, weather_text, is_daytime FROM Weather"):
            cur_combined.execute('''
                INSERT INTO weather (city, location_key, observation_time, temperature, weather_text, is_daytime)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', row)
        conn.close()

    conn_combined.commit()
    conn_combined.close()

if __name__ == "__main__":
    create_combined_db()
    copy_data()

