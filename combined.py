# import sqlite3
# import os

# # combined_db = "combined_data.db"
# # traffic_db = "traffic_data.db"
# # census_db = "city_data.db"
# # weather_db = "weather_density.db"

# def create_combined_db():
#     conn = sqlite3.connect('city_data.db')
#     cur = conn.cursor()

#     cur.execute('''
#         CREATE TABLE IF NOT EXISTS combined (
#             city TEXT PRIMARY KEY,
#             frc TEXT,
#             currentSpeed INTEGER,
#             freeFlowSpeed INTEGER,
#             confidence REAL,
#             roadClosure BOOLEAN,
#             population INTEGER,
#             place_code TEXT,
#             state_code TEXT,
#             location_key TEXT,
#             observation_time TEXT,
#             temperature REAL,
#             weather_text TEXT,
#             is_daytime INTEGER
#         )
#     ''')

#     conn.commit()
#     conn.close()

# def copy_and_merge_data():
#     data = {}

#     if os.path.exists(traffic_db):
#         conn = sqlite3.connect(traffic_db)
#         cur = conn.cursor()
#         for row in cur.execute("SELECT city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure FROM traffic_sample"):
#             city = row[0]
#             data[city] = {
#                 'frc': row[1], 'currentSpeed': row[2], 'freeFlowSpeed': row[3],
#                 'confidence': row[4], 'roadClosure': row[5]
#             }
#         conn.close()

#     if os.path.exists(census_db):
#         conn = sqlite3.connect(census_db)
#         cur = conn.cursor()
#         for row in cur.execute("SELECT city, population, place_code, state_code FROM city_stats"):
#             city = row[0]
#             if city not in data:
#                 data[city] = {}
#             data[city].update({
#                 'population': row[1], 'place_code': row[2], 'state_code': row[3]
#             })
#         conn.close()

#     if os.path.exists(weather_db):
#         conn = sqlite3.connect(weather_db)
#         cur = conn.cursor()
#         for row in cur.execute("""
#             SELECT city, location_key, observation_time, temperature, weather_text, is_daytime
#             FROM Weather
#             WHERE observation_time IN (
#                 SELECT MAX(observation_time)
#                 FROM Weather AS w2
#                 WHERE w2.city = Weather.city
#             )
#         """):
#             city = row[0]
#             if city not in data:
#                 data[city] = {}
#             data[city].update({
#                 'location_key': row[1], 'observation_time': row[2], 'temperature': row[3],
#                 'weather_text': row[4], 'is_daytime': row[5]
#             })
#         conn.close()

#     conn_combined = sqlite3.connect(combined_db)
#     cur_combined = conn_combined.cursor()

#     for city, info in data.items():
#         cur_combined.execute('''
#             INSERT OR REPLACE INTO combined (
#                 city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure,
#                 population, place_code, state_code,
#                 location_key, observation_time, temperature, weather_text, is_daytime
#             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#         ''', (
#             city,
#             info.get('frc'),
#             info.get('currentSpeed'),
#             info.get('freeFlowSpeed'),
#             info.get('confidence'),
#             info.get('roadClosure'),
#             info.get('population'),
#             info.get('place_code'),
#             info.get('state_code'),
#             info.get('location_key'),
#             info.get('observation_time'),
#             info.get('temperature'),
#             info.get('weather_text'),
#             info.get('is_daytime')
#         ))

#     conn_combined.commit()
#     conn_combined.close()

# if __name__ == "__main__":
#     create_combined_db()
#     copy_and_merge_data()



# import sqlite3
# import pandas as pd

# DB_PATH = "combined_data.db"

# CATEGORICAL_COLUMNS = ['frc', 'weather_text']


# def encode_strings(df, columns):
#     """
#     Replace strings in specified columns with integer codes.
#     Returns encoded DataFrame and mapping dictionaries.
#     """
#     mappings = {}
#     for col in columns:
#         df[col + '_id'], mapping = pd.factorize(df[col])
#         mappings[col] = dict(enumerate(mapping))
#     df = df.drop(columns=columns)
#     return df, mappings


# def load_encoded_data():
#     """
#     Load the combined table from the database and return an encoded version.
#     """
#     conn = sqlite3.connect(DB_PATH)
#     df = pd.read_sql_query("SELECT * FROM combined", conn)
#     df_encoded, mappings = encode_strings(df, CATEGORICAL_COLUMNS)

#     df_encoded.to_sql('combined_encoded', conn, if_exists='replace', index=False)

#     conn.close()
#     return df_encoded, mappings


# if __name__ == "__main__":
#     encoded_df, column_mappings = load_encoded_data()

#     print("Sample of Encoded Data:")
#     print(encoded_df.head())

#     print("\nMappings:")
#     for col, mapping in column_mappings.items():
#         print(f"{col}: {mapping}")


import sqlite3
import pandas as pd

DB_PATH = "combined_data.db"

def create_combined_table():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS combined (
            city TEXT PRIMARY KEY,
            frc TEXT,
            currentSpeed INTEGER,
            freeFlowSpeed INTEGER,
            confidence REAL,
            roadClosure BOOLEAN,
            population INTEGER,
            place_code TEXT,
            state_code TEXT,
            location_key TEXT,
            observation_time TEXT,
            temperature REAL,
            weather_text TEXT,
            is_daytime INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def copy_and_merge_data():
    data = {}
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for row in cur.execute("SELECT city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure FROM traffic_sample"):
        city = row[0]
        data[city] = {
            'frc': row[1], 'currentSpeed': row[2], 'freeFlowSpeed': row[3],
            'confidence': row[4], 'roadClosure': row[5]
        }

    for row in cur.execute("SELECT city, population, place_code, state_code FROM census_20"):
        city = row[0]
        if city not in data:
            data[city] = {}
        data[city].update({
            'population': row[1], 'place_code': row[2], 'state_code': row[3]
        })

    for row in cur.execute("""
        SELECT city, location_key, observation_time, temperature, weather_text, is_daytime
        FROM Weather
        WHERE observation_time IN (
            SELECT MAX(observation_time)
            FROM Weather AS w2
            WHERE w2.city = Weather.city
        )
    """):
        city = row[0]
        if city not in data:
            data[city] = {}
        data[city].update({
            'location_key': row[1], 'observation_time': row[2], 'temperature': row[3],
            'weather_text': row[4], 'is_daytime': row[5]
        })

    for city, info in data.items():
        cur.execute('''
            INSERT OR REPLACE INTO combined (
                city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure,
                population, place_code, state_code,
                location_key, observation_time, temperature, weather_text, is_daytime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            city,
            info.get('frc'),
            info.get('currentSpeed'),
            info.get('freeFlowSpeed'),
            info.get('confidence'),
            info.get('roadClosure'),
            info.get('population'),
            info.get('place_code'),
            info.get('state_code'),
            info.get('location_key'),
            info.get('observation_time'),
            info.get('temperature'),
            info.get('weather_text'),
            info.get('is_daytime')
        ))

    conn.commit()
    conn.close()


CATEGORICAL_COLUMNS = ['frc', 'weather_text']

def encode_strings(df, columns):
    mappings = {}
    for col in columns:
        df[col + '_id'], mapping = pd.factorize(df[col])
        mappings[col] = dict(enumerate(mapping))
    df = df.drop(columns=columns)
    return df, mappings

def load_encoded_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM combined", conn)
    df_encoded, mappings = encode_strings(df, CATEGORICAL_COLUMNS)
    df_encoded.to_sql('combined_encoded', conn, if_exists='replace', index=False)
    conn.close()
    return df_encoded, mappings


if __name__ == "__main__":
    create_combined_table()
    copy_and_merge_data()

    encoded_df, column_mappings = load_encoded_data()

    print("Sample of Encoded Data:")
    print(encoded_df.head())

    print("\nMappings:")
    for col, mapping in column_mappings.items():
        print(f"{col}: {mapping}")



# frc

# 0: FRC2
# 1: FRC3
# 2: FRC4
# 3: FRC1
# weather_text

# 0: Mostly sunny
# 1: Sunny
# 2: Cloudy
# 3: Light snow
# 4: Mostly cloudy
# 5: Partly sunny



