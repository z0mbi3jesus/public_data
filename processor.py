"""
processor.py

Processing layer — runs after each orchestrator collection cycle.
  1. Normalizes new stream rows into raw_events (JSON payloads).
  2. Computes three feature signals from the last 90 minutes of data.
  3. Updates stream_health with the freshness status of every stream.
"""

import json
import math
from datetime import datetime, timezone, timedelta

import mysql.connector
from settings_loader import load_json_config

SIGNAL_WINDOW_MINUTES = 90

# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_conn():
    cfg = load_json_config()
    m = cfg["storage"]["mysql"]
    return mysql.connector.connect(
        host=m["host"],
        port=int(m["port"]),
        user=m["user"],
        password=m["password"],
        database=m["database"],
        ssl_disabled=True,
    )


def _safe_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ── Stream table metadata ─────────────────────────────────────────────────────

STREAM_TABLES = {
    "air_quality": "stream_air_quality",
    "weather":     "stream_weather",
    "traffic":     "stream_traffic",
    "airport":     "stream_airport",
    "opensky":     "stream_opensky",
    "purpleair":   "stream_purpleair",
}

# Which field carries the location label for each stream
STREAM_LOCATION_FIELD = {
    "air_quality": "location",
    "weather":     "location",
    "traffic":     "location",
    "airport":     "airport_code",
    "opensky":     "location",
    "purpleair":   "location",
}


def _table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE %s", (table_name,))
    row = cur.fetchone()
    cur.close()
    return row is not None


# ── Cursor table (tracks last normalized stream row id) ───────────────────────

def _ensure_cursor_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS `processor_cursor` (
            `stream_name` VARCHAR(64) NOT NULL,
            `last_id`     BIGINT      NOT NULL DEFAULT 0,
            `updated_at`  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`stream_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cur.close()


def _ensure_processed_signal_columns(conn):
    cur = conn.cursor()
    cur.execute("SHOW COLUMNS FROM `processed_signals` LIKE 'details_json'")
    exists = cur.fetchone()
    if not exists:
        cur.execute("ALTER TABLE `processed_signals` ADD COLUMN `details_json` JSON NULL")
        conn.commit()
    cur.close()


def _get_cursor(conn, stream_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT `last_id` FROM `processor_cursor` WHERE `stream_name` = %s",
        (stream_name,)
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0


def _update_cursor(conn, stream_name, last_id):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO `processor_cursor` (`stream_name`, `last_id`)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE `last_id` = VALUES(`last_id`)
        """,
        (stream_name, last_id)
    )
    conn.commit()
    cur.close()


# ── Normalization ─────────────────────────────────────────────────────────────

def normalize_stream(conn, stream_name):
    """Copy new rows from a stream table into raw_events with JSON payloads."""
    table = STREAM_TABLES.get(stream_name)
    if not table:
        return 0
    if not _table_exists(conn, table):
        # Stream may be disabled and never ingested yet.
        return 0

    last_id = _get_cursor(conn, stream_name)
    location_field = STREAM_LOCATION_FIELD.get(stream_name, "location")

    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM `{table}` WHERE `id` > %s ORDER BY `id` ASC",
        (last_id,)
    )
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return 0

    insert_cur = conn.cursor()
    new_last_id = last_id

    for row in rows:
        row_dict = dict(zip(cols, row))
        row_id = row_dict.get("id", 0)
        # Prefer ingested_at — always UTC ISO set by orchestrator. Fall back to timestamp.
        timestamp_raw = row_dict.get("ingested_at") or row_dict.get("timestamp", "")
        location_label = str(row_dict.get(location_field, "") or "")

        try:
            event_ts = datetime.fromisoformat(
                str(timestamp_raw).replace("Z", "+00:00")
            )
            event_ts_str = event_ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            event_ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        payload_json = json.dumps(row_dict, default=str)

        insert_cur.execute(
            """
            INSERT INTO `raw_events`
                (`stream_name`, `event_ts`, `ingested_at`, `location_label`, `payload`, `processed`)
            VALUES (%s, %s, NOW(), %s, %s, 0)
            """,
            (stream_name, event_ts_str, location_label, payload_json)
        )
        new_last_id = max(new_last_id, int(row_id) if row_id else 0)

    conn.commit()
    insert_cur.close()

    if new_last_id > last_id:
        _update_cursor(conn, stream_name, new_last_id)

    return len(rows)


# ── Recent payload fetching ───────────────────────────────────────────────────

def _get_recent_payloads(conn, stream_name, window_minutes=SIGNAL_WINDOW_MINUTES):
    """Return a list of parsed payload dicts from raw_events within the window."""
    since = (
        datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    ).strftime("%Y-%m-%d %H:%M:%S")

    cur = conn.cursor()
    cur.execute(
        "SELECT `payload` FROM `raw_events` WHERE `stream_name` = %s AND `event_ts` >= %s",
        (stream_name, since)
    )
    rows = cur.fetchall()
    cur.close()

    result = []
    for (payload_str,) in rows:
        try:
            result.append(json.loads(payload_str))
        except Exception:
            pass
    return result


# ── Raw signal extractors ─────────────────────────────────────────────────────

def _congestion_fraction(payloads):
    """
    Average congestion fraction across traffic locations (0=free flow, 1=standstill).
    Returns None if no valid data.
    """
    by_location = {}
    for p in payloads:
        loc    = p.get("location", "")
        metric = p.get("metric", "")
        val    = _safe_float(p.get("value"))
        if metric in ("currentSpeed", "freeFlowSpeed") and val is not None:
            if loc not in by_location:
                by_location[loc] = {}
            by_location[loc][metric] = val  # last write(newest) wins

    ratios = []
    for data in by_location.values():
        cs  = data.get("currentSpeed")
        ffs = data.get("freeFlowSpeed")
        if cs is not None and ffs is not None and ffs > 0:
            ratios.append(1.0 - cs / ffs)

    return sum(ratios) / len(ratios) if ratios else None


def _latest_weather(payloads):
    """Return dict of most recent value for each weather metric."""
    result = {}
    for p in payloads:
        metric = p.get("metric", "")
        if metric in ("temp_f", "humidity", "wind_kph", "condition_text"):
            result[metric] = p.get("value")
    return result


def _aqi_values(payloads):
    return [v for v in (_safe_float(p.get("value")) for p in payloads) if v is not None]


def _active_flight_count(payloads):
    return sum(
        1 for p in payloads
        if str(p.get("status", "")).lower() in ("scheduled", "active", "en-route")
    )


# ── Feature signal: demand_pressure_score (restaurant) ───────────────────────

def compute_demand_pressure(conn):
    """
    demand_pressure_score 0-100 — higher means more customer demand expected.
    Points:
      Traffic activity (city busyness)   0-25
      Weather comfort (temp + condition) 0-35
      Air quality (outdoor friendliness) 0-20
      Airport flight activity            0-20
    """
    traffic_p  = _get_recent_payloads(conn, "traffic")
    weather_p  = _get_recent_payloads(conn, "weather")
    aq_p       = _get_recent_payloads(conn, "air_quality")
    airport_p  = _get_recent_payloads(conn, "airport")

    score = 0.0
    components = {}

    # Traffic: congestion → people are on the road
    congestion = _congestion_fraction(traffic_p)
    if congestion is not None:
        pts = round(congestion * 25, 1)
        score += pts
        components["traffic_congestion_pct"] = round(congestion * 100, 1)
        components["traffic_pts"] = pts

    # Weather: temperature comfort
    weather = _latest_weather(weather_p)
    temp_f    = _safe_float(weather.get("temp_f"))
    condition = str(weather.get("condition_text", "")).lower()

    temp_pts = 0.0
    if temp_f is not None:
        if 60 <= temp_f <= 72:
            temp_pts = 20.0
        elif temp_f < 60:
            temp_pts = max(0.0, 20.0 - (60 - temp_f) * 0.8)
        else:
            temp_pts = max(0.0, 20.0 - (temp_f - 72) * 0.8)

    cond_pts = 0.0
    if condition:
        if any(w in condition for w in ("sunny", "clear")):
            cond_pts = 15.0
        elif any(w in condition for w in ("partly", "overcast", "cloudy", "mist", "fog")):
            cond_pts = 10.0
        elif any(w in condition for w in ("drizzle", "light rain")):
            cond_pts = 5.0
        elif any(w in condition for w in ("rain", "snow", "sleet", "blizzard", "thunder", "storm")):
            cond_pts = 0.0
        else:
            cond_pts = 8.0

    weather_pts = round(temp_pts + cond_pts, 1)
    score += weather_pts
    components["temp_f"] = temp_f
    components["condition"] = weather.get("condition_text")
    components["weather_pts"] = weather_pts

    # Air quality
    aqi_vals = _aqi_values(aq_p)
    if aqi_vals:
        avg_aqi = sum(aqi_vals) / len(aqi_vals)
        aqi_pts = max(0.0, round(20.0 - max(0, avg_aqi - 50) * 0.4, 1))
        aqi_pts = min(aqi_pts, 20.0)
        score += aqi_pts
        components["avg_aqi"] = round(avg_aqi, 1)
        components["aqi_pts"] = aqi_pts

    # Airport: visitor volume proxy
    active_flights = _active_flight_count(airport_p)
    airport_pts = min(round(active_flights * 0.4, 1), 20.0)
    score += airport_pts
    components["active_flights"] = active_flights
    components["airport_pts"] = airport_pts

    final = min(round(score, 1), 100.0)
    label = "high" if final >= 65 else ("moderate" if final >= 35 else "low")
    return final, label, components


# ── Feature signal: delivery_risk_score (logistics) ──────────────────────────

def compute_delivery_risk(conn):
    """
    delivery_risk_score 0-100 — higher means more delivery risk.
    Points:
      Traffic congestion   0-40
      Weather hazard       0-35  (precipitation type + wind, capped)
      Air quality hazard   0-25
    """
    traffic_p = _get_recent_payloads(conn, "traffic")
    weather_p = _get_recent_payloads(conn, "weather")
    aq_p      = _get_recent_payloads(conn, "air_quality")

    score = 0.0
    components = {}

    # Traffic congestion → longer delivery times
    congestion = _congestion_fraction(traffic_p)
    if congestion is not None:
        pts = round(congestion * 40, 1)
        score += pts
        components["traffic_congestion_pct"] = round(congestion * 100, 1)
        components["traffic_pts"] = pts

    # Weather: precipitation severity
    weather  = _latest_weather(weather_p)
    condition = str(weather.get("condition_text", "")).lower()
    wind_kph  = _safe_float(weather.get("wind_kph"))
    temp_f    = _safe_float(weather.get("temp_f"))

    precip_pts = 0.0
    if condition:
        if any(w in condition for w in ("blizzard", "ice", "freezing", "sleet")):
            precip_pts = 35.0
        elif any(w in condition for w in ("snow", "heavy rain", "thunder", "storm")):
            precip_pts = 25.0
        elif any(w in condition for w in ("rain", "drizzle", "shower")):
            precip_pts = 15.0
        elif any(w in condition for w in ("fog", "mist")):
            precip_pts = 10.0
    if temp_f is not None and temp_f < 32:
        precip_pts = min(precip_pts + 10.0, 35.0)

    wind_pts = 0.0
    if wind_kph is not None:
        wind_pts = min(wind_kph / 40.0 * 15.0, 15.0)

    weather_pts = round(min(precip_pts + wind_pts, 35.0), 1)
    score += weather_pts
    components["condition"] = weather.get("condition_text")
    components["wind_kph"] = wind_kph
    components["temp_f"] = temp_f
    components["weather_pts"] = weather_pts

    # Air quality: smoke/smog visibility hazard
    aqi_vals = _aqi_values(aq_p)
    if aqi_vals:
        avg_aqi = sum(aqi_vals) / len(aqi_vals)
        if avg_aqi > 150:
            aqi_pts = 25.0
        elif avg_aqi > 100:
            aqi_pts = 15.0
        elif avg_aqi > 50:
            aqi_pts = 5.0
        else:
            aqi_pts = 0.0
        score += aqi_pts
        components["avg_aqi"] = round(avg_aqi, 1)
        components["aqi_pts"] = aqi_pts

    final = min(round(score, 1), 100.0)
    label = "high" if final >= 60 else ("moderate" if final >= 30 else "low")
    return final, label, components


# ── Feature signal: outdoor_safety_score (outdoor) ───────────────────────────

def compute_outdoor_safety(conn):
    """
    outdoor_safety_score 0-100 — higher means safer outdoor conditions.
    Starts at 100, deducts penalties:
      AQI penalty          up to -50
      Extreme temperature  up to -20
      Wind                 up to -15
      Precipitation        up to -15
    Labels: safe (>=75), moderate (>=45), poor (<45)
    """
    weather_p = _get_recent_payloads(conn, "weather")
    aq_p      = _get_recent_payloads(conn, "air_quality")

    score = 100.0
    components = {}

    # AQI
    aqi_vals = _aqi_values(aq_p)
    if aqi_vals:
        avg_aqi = sum(aqi_vals) / len(aqi_vals)
        if avg_aqi > 200:
            aqi_pen = 50.0
        elif avg_aqi > 150:
            aqi_pen = 35.0
        elif avg_aqi > 100:
            aqi_pen = 20.0
        elif avg_aqi > 50:
            aqi_pen = 10.0
        else:
            aqi_pen = 0.0
        score -= aqi_pen
        components["avg_aqi"] = round(avg_aqi, 1)
        components["aqi_penalty"] = aqi_pen

    # Temperature extremes
    weather   = _latest_weather(weather_p)
    temp_f    = _safe_float(weather.get("temp_f"))
    wind_kph  = _safe_float(weather.get("wind_kph"))
    condition = str(weather.get("condition_text", "")).lower()

    if temp_f is not None:
        if temp_f < 20 or temp_f > 100:
            temp_pen = 20.0
        elif temp_f < 32 or temp_f > 90:
            temp_pen = 12.0
        elif temp_f < 40 or temp_f > 85:
            temp_pen = 5.0
        else:
            temp_pen = 0.0
        score -= temp_pen
        components["temp_f"] = temp_f
        components["temp_penalty"] = temp_pen

    # Wind
    if wind_kph is not None:
        wind_pen = min(wind_kph / 60.0 * 15.0, 15.0)
        score -= wind_pen
        components["wind_kph"] = wind_kph
        components["wind_penalty"] = round(wind_pen, 1)

    # Precipitation
    if condition:
        if any(w in condition for w in ("blizzard", "thunder", "storm", "freezing")):
            precip_pen = 15.0
        elif any(w in condition for w in ("snow", "sleet", "heavy rain")):
            precip_pen = 12.0
        elif any(w in condition for w in ("rain", "drizzle", "shower")):
            precip_pen = 8.0
        else:
            precip_pen = 0.0
        score -= precip_pen
        components["condition"] = weather.get("condition_text")
        components["precip_penalty"] = precip_pen

    final = max(round(score, 1), 0.0)
    label = "safe" if final >= 75 else ("moderate" if final >= 45 else "poor")
    return final, label, components


# ── Signal writer ─────────────────────────────────────────────────────────────

def _write_signal(conn, vertical, signal_key, value, label, streams_used, details, window_start, window_end):
    computed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO `processed_signals`
            (`computed_at`, `window_start`, `window_end`, `vertical`, `signal_key`,
             `signal_value`, `signal_label`, `contributing_streams`, `details_json`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            computed_at.strftime("%Y-%m-%d %H:%M:%S"),
            window_start.strftime("%Y-%m-%d %H:%M:%S"),
            window_end.strftime("%Y-%m-%d %H:%M:%S"),
            vertical,
            signal_key,
            value,
            label,
            streams_used,
            json.dumps(details or {}, default=str),
        )
    )
    conn.commit()
    cur.close()


# ── Stream health ─────────────────────────────────────────────────────────────

def update_stream_health(conn):
    """Write one stream_health row per stream based on last ingestion freshness."""
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    for stream_name, table in STREAM_TABLES.items():
        try:
            if not _table_exists(conn, table):
                continue
            cur = conn.cursor()
            cur.execute(f"SELECT MAX(`ingested_at`) FROM `{table}`")
            row = cur.fetchone()
            cur.close()
            last_ingested = row[0] if row else None

            # Count rows from the most recent ingestion batch
            rows_last_run = 0
            if last_ingested:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT COUNT(*) FROM `{table}` WHERE `ingested_at` = %s",
                    (last_ingested,)
                )
                rows_last_run = cur.fetchone()[0] or 0
                cur.close()

            if last_ingested is None:
                status = "down"
                last_success_str = None
            else:
                try:
                    ts = datetime.fromisoformat(
                        str(last_ingested).replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                    age_min = (datetime.now(timezone.utc).replace(tzinfo=None) - ts).total_seconds() / 60
                    status = "ok" if age_min < 45 else ("degraded" if age_min < 90 else "down")
                    last_success_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    status = "degraded"
                    last_success_str = None

            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO `stream_health`
                    (`checked_at`, `stream_name`, `status`, `last_success_at`, `rows_last_run`)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (checked_at, stream_name, status, last_success_str, rows_last_run)
            )
            conn.commit()
            cur.close()

        except Exception as e:
            print(f"[FAIL] stream_health update for {stream_name}: {e}")


# ── Main entry point ──────────────────────────────────────────────────────────

def run_processor():
    conn = _get_conn()
    _ensure_cursor_table(conn)
    _ensure_processed_signal_columns(conn)

    # 1. Normalize new stream rows into raw_events
    print("--- Normalization ---")
    for stream_name in STREAM_TABLES:
        count = normalize_stream(conn, stream_name)
        print(f"[OK] {stream_name}: {count} new rows -> raw_events")

    # 2. Compute feature signals over the last SIGNAL_WINDOW_MINUTES
    now          = datetime.now(timezone.utc).replace(tzinfo=None)
    window_start = now - timedelta(minutes=SIGNAL_WINDOW_MINUTES)

    print("\n--- Feature Signals ---")

    score, label, comp = compute_demand_pressure(conn)
    _write_signal(conn, "restaurant", "demand_pressure_score",
                  score, label, "traffic,weather,air_quality,airport", comp,
                  window_start, now)
    print(f"[OK] demand_pressure_score  = {score:5.1f}  ({label})")
    print(f"     breakdown: {comp}")

    score, label, comp = compute_delivery_risk(conn)
    _write_signal(conn, "logistics", "delivery_risk_score",
                  score, label, "traffic,weather,air_quality", comp,
                  window_start, now)
    print(f"[OK] delivery_risk_score    = {score:5.1f}  ({label})")
    print(f"     breakdown: {comp}")

    score, label, comp = compute_outdoor_safety(conn)
    _write_signal(conn, "outdoor", "outdoor_safety_score",
                  score, label, "weather,air_quality", comp,
                  window_start, now)
    print(f"[OK] outdoor_safety_score   = {score:5.1f}  ({label})")
    print(f"     breakdown: {comp}")

    # 3. Update stream health
    print("\n--- Stream Health ---")
    update_stream_health(conn)
    print("[OK] stream_health updated")

    conn.close()
    print("\nProcessor run complete.")


if __name__ == "__main__":
    run_processor()
