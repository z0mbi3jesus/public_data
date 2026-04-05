import requests
from datetime import datetime

def fetch_weather(config):
    api_url = config["api_url"]
    key = config["key"]
    city = config["city"]

    params = {"key": key, "q": city}

    import logging
    logging.basicConfig(
        filename='weather_debug.log',
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    try:
        resp = requests.get(api_url, params=params)
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as e:
        print(f"[✖] Weather fetch failed: {e}")
        return []

    current = json_data.get("current", {})
    location = json_data.get("location", {})

    data_points = [
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "temp_c", "value": current.get("temp_c"), "unit": "C"},
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "temp_f", "value": current.get("temp_f"), "unit": "F"},
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "humidity", "value": current.get("humidity"), "unit": "%"},
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "wind_kph", "value": current.get("wind_kph"), "unit": "kph"},
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "wind_dir", "value": current.get("wind_dir"), "unit": ""},
        {"timestamp": datetime.now().isoformat(), "location": location.get("name"), "metric": "condition_text", "value": current.get("condition", {}).get("text"), "unit": ""}
    ]
    return data_points

def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]

def get_units():
    return ["", "", "", "", ""]
