import requests
from datetime import datetime

import logging
logging.basicConfig(
    filename='air_quality_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
def fetch_air_quality(config):
    """
    Fetch air quality data from AirNow API.
    Returns a list of dicts, one per pollutant per location.
    """
    api_url = config["api_url"]
    logging.debug('fetch_air_quality called with config: %s', config)
    key = config["key"]
    zip_code = config["zip"]

    params = {
        "format": "application/json",
        "zipCode": zip_code,
        "distance": "25",
        "API_KEY": key
    }

    try:
        resp = requests.get(api_url, params=params)
        resp.raise_for_status()
        json_data = resp.json()
    except Exception as e:
        print(f"[✖] Air quality fetch failed: {e}")
        return []

    data_points = []
    for obs in json_data:
        data_points.append({
            "timestamp": datetime.now().isoformat(),
            "location": obs.get("ReportingArea"),
            "category": obs.get("Category", {}).get("Name"),
            "pollutant": obs.get("ParameterName"),
            "value": obs.get("AQI"),
            "unit": "AQI"
        })
    return data_points

def get_headers():
    return ["timestamp", "location", "category", "pollutant", "value", "unit"]

def get_units():
    return ["", "", "", "", "AQI", ""]
