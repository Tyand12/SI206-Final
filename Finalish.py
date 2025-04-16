import sqlite3
import requests

API_KEY = "ubarmsPfwXhib4NBihcxaGB5EfuJR3M2"
BASE_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
UNIT = "MPH"
OPENLR = "false"

city_points = [
        ("Bozeman, MT", "45.6795", "-111.0440"),
    ("Pullman, WA", "46.7314", "-117.1787"),
    ("Gainesville, FL", "29.6516", "-82.3248"),
    ("Boone, NC", "36.2168", "-81.6746"),
    ("Clemson, SC", "34.6834", "-82.8374"),
    ("East Lansing, MI", "42.7360", "-84.4839"),
    ("Moscow, ID", "46.7310", "-117.0002"),
    ("Provo, UT", "40.2338", "-111.6585"),
    ("Ann Arbor, MI", "42.2808", "-83.7430"),
    ("Stanford, CA", "37.4275", "-122.1697"),
    ("Bloomington, IN", "39.1653", "-86.5264"),
    ("Athens, OH", "39.3292", "-82.1013"),
    ("Ellensburg, WA", "47.0003", "-120.5386"),
    ("San Luis Obispo, CA", "35.2828", "-120.6596"),
    ("College Station, TX", "30.6111", "-96.3410"),
    ("Laramie, WY", "41.3114", "-105.5911"),
    ("Amherst, MA", "42.3732", "-72.5199"),
    ("Manhattan, KS", "39.1836", "-96.5717"),
    ("West Lafayette, IN", "40.4259", "-86.9081"),
    ("Platteville, WI", "42.7367", "-90.4783")
]

headers = {
    "accept": "*/*"
}


def create_database():
    """Create SQLite database and traffic table if it doesn't exist."""
    conn = sqlite3.connect("traffic_data.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            frc TEXT,
            currentSpeed INTEGER,
            freeFlowSpeed INTEGER,
            confidence REAL,
            roadClosure BOOLEAN
        )
    """)
    conn.commit()
    conn.close()


def fetch_traffic_data(lat, lon):
    """Fetch traffic data from TomTom API for given latitude and longitude."""
    params = {
        "point": f"{lat},{lon}",
        "unit": UNIT,
        "openLr": OPENLR,
        "key": API_KEY
    }

    response = requests.get(BASE_URL, params=params, headers=headers)
    if response.status_code == 200:
        try:
            flow = response.json().get("flowSegmentData", {})
            return {
                "frc": flow.get("frc"),
                "currentSpeed": flow.get("currentSpeed"),
                "freeFlowSpeed": flow.get("freeFlowSpeed"),
                "confidence": flow.get("confidence"),
                "roadClosure": int(flow.get("roadClosure", False))
            }
        except:
            return None
    else:
        return None


def insert_traffic_data(city, data):
    """Insert traffic data for a city into the database."""
    if data is None:
        return

    conn = sqlite3.connect("traffic_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO traffic (
                city, frc, currentSpeed, freeFlowSpeed, confidence, roadClosure
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (city, data["frc"], data["currentSpeed"], data["freeFlowSpeed"],
              data["confidence"], data["roadClosure"]))
        conn.commit()
        conn.close()
    except:
        conn.close()


def fetch_and_store_all_data():
    """Fetch and store traffic data for all defined city points."""
    for city, lat, lon in city_points:
        data = fetch_traffic_data(lat, lon)
        insert_traffic_data(city, data)


if __name__ == "__main__":
    create_database()
    fetch_and_store_all_data()




