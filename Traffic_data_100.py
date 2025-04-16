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
("Platteville, WI", "42.7367", "-90.4783"),
("Ames, IA", "42.0347", "-93.6204"),
("Iowa City, IA", "41.6611", "-91.5302"),
("Madison, WI", "43.0731", "-89.4012"),
("Chapel Hill, NC", "35.9132", "-79.0558"),
("Berkeley, CA", "37.8715", "-122.2730"),
("Boulder, CO", "40.01499", "-105.2705"),
("Eugene, OR", "44.0521", "-123.0868"),
("Athens, GA", "33.9519", "-83.3576"),
("Lawrence, KS", "38.9717", "-95.2353"),
("Oxford, MS", "34.3665", "-89.5192"),
("Corvallis, OR", "44.5646", "-123.2620"),
("Tempe, AZ", "33.4255", "-111.9400"),
("Charlottesville, VA", "38.0293", "-78.4767"),
("Columbia, MO", "38.9517", "-92.3341"),
("State College, PA", "40.7934", "-77.8600"),
("Lexington, KY", "38.0406", "-84.5037"),
("Tallahassee, FL", "30.4383", "-84.2807"),
("Providence, RI", "41.8240", "-71.4128"),
("Santa Cruz, CA", "36.9741", "-122.0308"),
("New Haven, CT", "41.3083", "-72.9279"),
("Durham, NC", "35.9940", "-78.8986"),
("Cambridge, MA", "42.3736", "-71.1097"),
("Ithaca, NY", "42.4430", "-76.5019"),
("Princeton, NJ", "40.3573", "-74.6672"),
("Hanover, NH", "43.7022", "-72.2896"),
("Middlebury, VT", "44.0153", "-73.1673"),
("Swarthmore, PA", "39.9026", "-75.3496"),
("Gambier, OH", "40.3753", "-82.3979"),
("Oberlin, OH", "41.2939", "-82.2174"),
("Grinnell, IA", "41.7434", "-92.7232"),
("Saratoga Springs, NY", "43.0831", "-73.7846"),
("Claremont, CA", "34.0967", "-117.7198"),
("Walla Walla, WA", "46.0646", "-118.3430"),
("Davidson, NC", "35.4993", "-80.8487"),
("Sewanee, TN", "35.2034", "-85.9219"),
("Meadville, PA", "41.6414", "-80.1514"),
("Wooster, OH", "40.8051", "-81.9351"),
("Northfield, MN", "44.4583", "-93.1616"),
("Decorah, IA", "43.3033", "-91.7857"),
("Poughkeepsie, NY", "41.7004", "-73.9210"),
("Schenectady, NY", "42.8142", "-73.9396"),
("Brunswick, ME", "43.9145", "-69.9653"),
("Lewiston, ME", "44.1004", "-70.2148"),
("Haverford, PA", "40.0104", "-75.3080"),
("Worcester, MA", "42.2626", "-71.8023"),
("Waltham, MA", "42.3765", "-71.2356"),
("Middletown, CT", "41.5623", "-72.6506"),
("Clinton, NY", "43.0484", "-75.3788"),
("Canton, NY", "44.5951", "-75.1690"),
("Waterville, ME", "44.5520", "-69.6317"),
("Northampton, MA", "42.3251", "-72.6412"),
("Greencastle, IN", "39.6448", "-86.8647"),
("Crawfordsville, IN", "40.0412", "-86.8745"),
("Delaware, OH", "40.2987", "-83.0670"),
("Granville, OH", "40.0681", "-82.5190"),
("Lexington, VA", "37.7840", "-79.4428"),
("Staunton, VA", "38.1496", "-79.0717"),
("Harrisonburg, VA", "38.4496", "-78.8689"),
("Fredericksburg, VA", "38.3032", "-77.4605"),
("Farmville, VA", "37.3021", "-78.3919"),
("Radford, VA", "37.1318", "-80.576")
 

]

headers = {
    "accept": "*/*"
}


def create_database():
    """Create SQLite database and traffic table if it doesn't exist."""
    conn = sqlite3.connect("traffic_data_100.db")
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

    conn = sqlite3.connect("traffic_data_100.db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO traffic (
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




