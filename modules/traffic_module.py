import logging
logging.basicConfig(
    filename='traffic_debug.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s'
)
logging.debug('traffic_module loaded')
import requests
from datetime import datetime

def fetch_traffic(config):
    api_url = config["api_url"]
    key = config["key"]
    points = config.get("points", [])

    data_points = []

    for pt in points:
        params = {"key": key, "point": pt["coords"]}
        try:
            resp = requests.get(api_url, params=params)
            resp.raise_for_status()
            json_data = resp.json()
            fsd = json_data.get("flowSegmentData", {})
            logging.debug(f"Fetched data for {pt['label']}: {fsd}")
        except Exception as e:
            print(f"[✖] Traffic fetch failed for {pt['label']}: {e}")
            continue

        metrics = {
            "currentSpeed": "kph",
            "freeFlowSpeed": "kph",
            "currentTravelTime": "sec",
            "freeFlowTravelTime": "sec",
            "confidence": ""
        }

        for metric, unit in metrics.items():
            data_points.append({
                "timestamp": datetime.now().isoformat(),
                "location": pt["label"],
                "metric": metric,
                "value": fsd.get(metric),
                "unit": unit
            })
    return data_points

def get_headers():
    return ["timestamp", "location", "metric", "value", "unit"]

def get_units():
    return ["", "", "", "", ""]
