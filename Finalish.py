# curl -X 'GET' \
#   'https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point=42.28083%2C-83.74304&unit=MPH&openLr=false&key=*****' \
#   -H 'accept: */*'

# https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json?point=42.28083%2C-83.74304&unit=MPH&openLr=false&key=*****


import requests

url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
params = {
    "point": "42.28083,-83.74304",
    "unit": "MPH",
    "openLr": "false",
    "key": "ubarmsPfwXhib4NBihcxaGB5EfuJR3M2"
}

response = requests.get(url, params=params)

print("STATUS CODE:", response.status_code)

if response.status_code == 200:
    try:
        data = response.json()
        print("Traffic Data:")
        print(data)
    except Exception as e:
        print("Could not parse JSON:", e)
else:
    print("Request failed with status:", response.status_code)
    print("Response text:", response.text)



