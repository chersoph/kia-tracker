import os
import math
import time
import json
import sqlite3
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify, request, render_template

from hyundai_kia_connect_api import VehicleManager

app = Flask(__name__)

# Core runtime configuration.
# These values can be tuned from Docker environment variables without editing code.
DB_PATH = os.getenv("DB_PATH", "/data/kia_trip_logger.db")
PORT = int(os.getenv("PORT", "8000"))


# Slow/default poll interval, mainly used when the car appears parked.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
NIGHT_PARKED_POLL_INTERVAL_SECONDS = int(os.getenv("NIGHT_PARKED_POLL_INTERVAL_SECONDS", "3600"))
NIGHT_PARKED_START_HOUR = int(os.getenv("NIGHT_PARKED_START_HOUR", "23"))
NIGHT_PARKED_END_HOUR = int(os.getenv("NIGHT_PARKED_END_HOUR", "6"))

# Faster poll interval used while movement is detected so the map gets denser routes.
MOVING_POLL_INTERVAL_SECONDS = int(os.getenv("MOVING_POLL_INTERVAL_SECONDS", "20"))
MOVING_POLL_INTERVAL_LOCAL_SECONDS = int(os.getenv("MOVING_POLL_INTERVAL_LOCAL_SECONDS", "120"))
MOVING_POLL_INTERVAL_HIGHWAY_SECONDS = int(os.getenv("MOVING_POLL_INTERVAL_HIGHWAY_SECONDS", "600"))
LOCAL_TRIP_MAX_SPEED_KMH = float(os.getenv("LOCAL_TRIP_MAX_SPEED_KMH", "70"))

# Keep polling quickly for a few cycles after the last detected movement.
# This avoids dropping back to a slow interval too early during a trip.
RECENT_MOVEMENT_GRACE_POLLS = int(os.getenv("RECENT_MOVEMENT_GRACE_POLLS", "6"))

# Storage thresholds.
# A point is only stored when movement or odometer change is meaningful enough.
MIN_MOVEMENT_METERS = float(os.getenv("MIN_MOVEMENT_METERS", "50"))
MIN_ODOMETER_DELTA_KM = float(os.getenv("MIN_ODOMETER_DELTA_KM", "0.1"))
TRIP_GAP_MINUTES = int(os.getenv("TRIP_GAP_MINUTES", "15"))

# Reverse geocoding settings for converting coordinates into readable place names.
GEOCODE_ENABLED = os.getenv("GEOCODE_ENABLED", "true").lower() == "true"
GEOCODE_CACHE_DECIMALS = int(os.getenv("GEOCODE_CACHE_DECIMALS", "3"))
GEOCODE_EMAIL = os.getenv("GEOCODE_EMAIL", "")
GEOCODE_USER_AGENT = os.getenv("GEOCODE_USER_AGENT", "kia-trip-logger/1.0")

# Kia API login configuration.
KIA_REGION = int(os.getenv("KIA_REGION", "1"))
KIA_BRAND = int(os.getenv("KIA_BRAND", "1"))
KIA_USERNAME = os.environ["KIA_USERNAME"]
KIA_PASSWORD = os.environ["KIA_PASSWORD"]
KIA_PIN = os.getenv("KIA_PIN", "")

# Kia EV3 Battery capacity.
BATTERY_CAPACITY_KWH = float(os.getenv("BATTERY_CAPACITY_KWH", "81.4"))
USABLE_BATTERY_KWH = float(os.getenv("USABLE_BATTERY_KWH", "74"))

night_short_poll_override = False
lock = threading.Lock()
last_geocode_ts = 0.0
pending_pre_movement_point = None

def is_night_local():
    now_local = datetime.now().astimezone()
    hour = now_local.hour

    if NIGHT_PARKED_START_HOUR == NIGHT_PARKED_END_HOUR:
        return True

    if NIGHT_PARKED_START_HOUR < NIGHT_PARKED_END_HOUR:
        return NIGHT_PARKED_START_HOUR <= hour < NIGHT_PARKED_END_HOUR

    return hour >= NIGHT_PARKED_START_HOUR or hour < NIGHT_PARKED_END_HOUR

def estimate_speed_kmh(new_point, old_point):
    if not old_point:
        return None

    old_ts = old_point.get("ts")
    new_ts = new_point.get("ts")
    if not old_ts or not new_ts:
        return None

    seconds = (parse_ts(new_ts) - parse_ts(old_ts)).total_seconds()
    if seconds <= 0:
        return None

    distance_km = None

    old_odo = old_point.get("odometer_km")
    new_odo = new_point.get("odometer_km")
    if old_odo is not None and new_odo is not None and new_odo >= old_odo:
        distance_km = new_odo - old_odo
    else:
        moved_m = haversine_m(
            old_point.get("lat"), old_point.get("lon"),
            new_point.get("lat"), new_point.get("lon")
        )
        if moved_m is None:
            return None
        distance_km = moved_m / 1000.0

    return distance_km / (seconds / 3600.0)

def now_utc():
    """Return the current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def parse_ts(ts):
    """Parse timestamps that were previously stored in the database."""
    return datetime.fromisoformat(ts)

def estimate_trip_energy_kwh(start_battery_percent, end_battery_percent):
    if start_battery_percent is None or end_battery_percent is None:
        return None
    delta_percent = start_battery_percent - end_battery_percent
    if delta_percent < 0:
        return None
    return round(USABLE_BATTERY_KWH * delta_percent / 100.0, 2)

def db():
    """Open a SQLite connection with row access by column name."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Create the SQLite schema if it does not already exist.

    Tables:
    - trip_points: raw point history collected from the Kia API.
    - geocode_cache: small cache so we do not reverse-geocode the same area repeatedly.
    - trips: derived trip segments rebuilt from trip_points.
    """
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trip_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            vehicle_id TEXT,
            vehicle_name TEXT,
            lat REAL,
            lon REAL,
            odometer_km REAL,
            battery_percent REAL,
            range_km REAL,
            location_name TEXT,
            source TEXT,
            total_range_km REAL,
            charging INTEGER,
            plugged_in INTEGER,
            charging_power_kw REAL,
            charge_port_open INTEGER,
            remaining_charge_minutes REAL,
            climate_on INTEGER,
            engine_running INTEGER,
            locked INTEGER,
            car_battery_percent REAL,
            vehicle_status_ts TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trip_points_ts ON trip_points(ts)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS geocode_cache (
            key TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            location_name TEXT,
            raw_json TEXT,
            created_ts TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL,
            trip_index INTEGER NOT NULL,
            start_ts TEXT NOT NULL,
            end_ts TEXT NOT NULL,
            start_point_id INTEGER,
            end_point_id INTEGER,
            vehicle_id TEXT,
            vehicle_name TEXT,
            start_lat REAL,
            start_lon REAL,
            end_lat REAL,
            end_lon REAL,
            start_location_name TEXT,
            end_location_name TEXT,
            start_odometer_km REAL,
            end_odometer_km REAL,
            distance_km REAL,
            duration_minutes REAL,
            energy_kwh REAL,
            kwh_per_km REAL,
            point_count INTEGER
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trips_day ON trips(day)")

    cur.execute("PRAGMA table_info(trip_points)")
    point_columns = [row["name"] for row in cur.fetchall()]

    point_column_defs = {
        "total_range_km": "REAL",
        "charging": "INTEGER",
        "plugged_in": "INTEGER",
        "charging_power_kw": "REAL",
        "charge_port_open": "INTEGER",
        "remaining_charge_minutes": "REAL",
        "climate_on": "INTEGER",
        "engine_running": "INTEGER",
        "locked": "INTEGER",
        "car_battery_percent": "REAL",
        "vehicle_status_ts": "TEXT",
    }

    for col, col_type in point_column_defs.items():
        if col not in point_columns:
            cur.execute(f"ALTER TABLE trip_points ADD COLUMN {col} {col_type}")

    cur.execute("PRAGMA table_info(trips)")
    trip_columns = [row["name"] for row in cur.fetchall()]

    if "energy_kwh" not in trip_columns:
        cur.execute("ALTER TABLE trips ADD COLUMN energy_kwh REAL")

    if "kwh_per_km" not in trip_columns:
        cur.execute("ALTER TABLE trips ADD COLUMN kwh_per_km REAL")

    conn.commit()
    conn.close()

def haversine_m(lat1, lon1, lat2, lon2):
    """Calculate approximate distance in meters between two latitude/longitude points."""
    if None in (lat1, lon1, lat2, lon2):
        return None

    r = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def latest_point():
    """Return the newest stored point or None if the database is empty."""
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trip_points ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def geocode_key(lat, lon):
    """Round coordinates into a cache key so nearby points can reuse the same geocode."""
    return f"{round(lat, GEOCODE_CACHE_DECIMALS)},{round(lon, GEOCODE_CACHE_DECIMALS)}"


def get_cached_geocode(lat, lon):
    """Fetch a previously cached place name for these coordinates, if available."""
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT location_name FROM geocode_cache WHERE key = ?", (geocode_key(lat, lon),))
    row = cur.fetchone()
    conn.close()
    return row["location_name"] if row else None


def store_cached_geocode(lat, lon, location_name, raw_json):
    """Store reverse-geocode results so repeated lookups are avoided."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO geocode_cache
        (key, lat, lon, location_name, raw_json, created_ts)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            geocode_key(lat, lon),
            lat,
            lon,
            location_name,
            json.dumps(raw_json, ensure_ascii=False),
            now_utc(),
        ),
    )
    conn.commit()
    conn.close()


def format_location_name(data):
    """
    Build a short, human-readable location name from Nominatim reverse geocode data.
    The goal is readability in the UI rather than full postal precision.
    """
    address = data.get("address", {}) or {}
    parts = [
        address.get("road"),
        address.get("house_number"),
        address.get("suburb"),
        address.get("city") or address.get("town") or address.get("village"),
        address.get("municipality"),
    ]

    cleaned = []
    for p in parts:
        if p and p not in cleaned:
            cleaned.append(p)

    if cleaned:
        return ", ".join(cleaned)

    return data.get("display_name")


def reverse_geocode(lat, lon):
    """
    Convert coordinates into a readable place name.

    Nominatim should not be hit too aggressively, so this function:
    - respects a basic rate limit,
    - reuses cached lookups,
    - stores successful results locally.
    """
    global last_geocode_ts

    if lat is None or lon is None or not GEOCODE_ENABLED:
        return None

    cached = get_cached_geocode(lat, lon)
    if cached:
        return cached

    elapsed = time.time() - last_geocode_ts
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)

    params = {
        "format": "jsonv2",
        "lat": lat,
        "lon": lon,
        "zoom": 16,
        "addressdetails": 1,
    }
    if GEOCODE_EMAIL:
        params["email"] = GEOCODE_EMAIL

    resp = requests.get(
        "https://nominatim.openstreetmap.org/reverse",
        params=params,
        headers={"User-Agent": GEOCODE_USER_AGENT},
        timeout=20,
    )
    last_geocode_ts = time.time()
    resp.raise_for_status()

    data = resp.json()
    location_name = format_location_name(data)
    if location_name:
        store_cached_geocode(lat, lon, location_name, data)
    return location_name


def enrich_with_location_name(point):
    """
    Fill in location_name for a point if missing.
    Failures are logged but do not stop trip collection.
    """
    if point.get("location_name"):
        return point

    lat = point.get("lat")
    lon = point.get("lon")
    if lat is None or lon is None:
        return point

    try:
        point["location_name"] = reverse_geocode(lat, lon)
    except Exception as e:
        print(
            json.dumps(
                {
                    "ts": now_utc(),
                    "warning": "reverse_geocode_failed",
                    "error": str(e),
                    "lat": lat,
                    "lon": lon,
                }
            ),
            flush=True,
        )
    return point


def insert_point(point):
    """Persist one collected vehicle point into SQLite."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trip_points (
            ts, vehicle_id, vehicle_name, lat, lon, odometer_km,
            battery_percent, range_km, location_name, source,
            total_range_km, charging, plugged_in, charging_power_kw,
            charge_port_open, remaining_charge_minutes, climate_on,
            engine_running, locked, car_battery_percent, vehicle_status_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            point["ts"],
            point["vehicle_id"],
            point["vehicle_name"],
            point["lat"],
            point["lon"],
            point["odometer_km"],
            point["battery_percent"],
            point["range_km"],
            point["location_name"],
            point["source"],
            point.get("total_range_km"),
            point.get("charging"),
            point.get("plugged_in"),
            point.get("charging_power_kw"),
            point.get("charge_port_open"),
            point.get("remaining_charge_minutes"),
            point.get("climate_on"),
            point.get("engine_running"),
            point.get("locked"),
            point.get("car_battery_percent"),
            point.get("vehicle_status_ts"),
        ),
    )
    conn.commit()
    conn.close()

def should_store_point(new_point, old_point):
    """
    Decide whether a new point is important enough to store.

    This keeps the database from filling with nearly identical stationary points.
    Storage is more selective than movement detection, because we want denser
    polling while driving without necessarily storing every single poll.
    """
    if not old_point:
        return True

    moved = haversine_m(
        old_point.get("lat"), old_point.get("lon"),
        new_point.get("lat"), new_point.get("lon")
    )
    old_odo = old_point.get("odometer_km")
    new_odo = new_point.get("odometer_km")

    odo_changed = (
        old_odo is not None
        and new_odo is not None
        and abs(new_odo - old_odo) >= MIN_ODOMETER_DELTA_KM
    )
    gps_changed = moved is not None and moved >= MIN_MOVEMENT_METERS
    return gps_changed or odo_changed


def point_indicates_movement(new_point, old_point):
    """
    Decide whether the car seems to be moving enough to switch into fast polling.

    This threshold is intentionally a bit more sensitive than should_store_point().
    That way the app can begin denser polling early, while database inserts still
    remain selective enough to avoid too much noise.
    """
    if not old_point:
        return False

    moved = haversine_m(
        old_point.get("lat"), old_point.get("lon"),
        new_point.get("lat"), new_point.get("lon")
    )
    old_odo = old_point.get("odometer_km")
    new_odo = new_point.get("odometer_km")

    gps_motion = moved is not None and moved >= max(15.0, MIN_MOVEMENT_METERS / 2.0)
    odo_motion = (
        old_odo is not None
        and new_odo is not None
        and new_odo > old_odo
    )
    return gps_motion or odo_motion

def _as_int_bool(value):
    if value is None:
        return None
    return 1 if bool(value) else 0

def fetch_vehicle_data():
    """
    Pull the latest vehicle state from the Kia API.

    Each returned dictionary is shaped to match what the database and API routes
    expect, so the rest of the app can stay simple.
    """
    vm = VehicleManager(
        region=KIA_REGION,
        brand=KIA_BRAND,
        username=KIA_USERNAME,
        password=KIA_PASSWORD,
        pin=KIA_PIN,
    )

    vm.check_and_refresh_token()
    vm.update_all_vehicles_with_cached_state()

    results = []
    for vehicle_id, vehicle in vm.vehicles.items():
        point = {
            "ts": now_utc(),
            "vehicle_id": vehicle_id,
            "vehicle_name": getattr(vehicle, "name", "kia"),
            "lat": getattr(vehicle, "_location_latitude", None),
            "lon": getattr(vehicle, "_location_longitude", None),
            "odometer_km": getattr(vehicle, "_odometer", None),
            "battery_percent": getattr(vehicle, "ev_battery_percentage", None),
            "range_km": getattr(vehicle, "_ev_driving_range", None),
            "location_name": getattr(vehicle, "_last_geocoded_location", None),
            "source": "kia-api-direct",

            "total_range_km": getattr(vehicle, "total_driving_distance", None),
            "charging": _as_int_bool(getattr(vehicle, "ev_battery_is_charging", None)),
            "plugged_in": _as_int_bool(getattr(vehicle, "ev_battery_is_plugged_in", None)),
            "charging_power_kw": getattr(vehicle, "ev_charging_power", None),
            "charge_port_open": _as_int_bool(getattr(vehicle, "ev_charge_port_door_is_open", None)),
            "remaining_charge_minutes": getattr(vehicle, "ev_estimated_current_charge_duration", None),
            "climate_on": _as_int_bool(getattr(vehicle, "air_control_is_on", None)),
            "engine_running": _as_int_bool(getattr(vehicle, "engine_is_running", None)),
            "locked": _as_int_bool(getattr(vehicle, "is_locked", None)),
            "car_battery_percent": getattr(vehicle, "car_battery_percentage", None),
            "vehicle_status_ts": getattr(vehicle, "last_updated_at", None),
        }
        print(
            json.dumps(
                {
                    "ts": now_utc(),
                    "event": "vehicle_attr_probe",
                    "vehicle_id": vehicle_id,
                    "charging_attr": getattr(vehicle, "ev_battery_is_charging", "missing"),
                    "plugged_in_attr": getattr(vehicle, "ev_battery_is_plugged_in", "missing"),
                    "charge_power_attr": getattr(vehicle, "ev_charging_power", "missing"),
                    "status_ts_attr": getattr(vehicle, "last_updated_at", "missing"),
                }
            ),
            flush=True,
        )
        results.append(enrich_with_location_name(point))
    return results

def get_points_for_day(day):
    """Return all raw points for one day, sorted oldest to newest."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM trip_points
        WHERE substr(ts, 1, 10) = ?
        ORDER BY ts ASC
        """,
        (day,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def summarize_day(points):
    if not points:
        return {
            "day": None,
            "point_count": 0,
            "start_ts": None,
            "end_ts": None,
            "duration_minutes": 0,
            "start_location": None,
            "end_location": None,
            "start_odometer_km": None,
            "end_odometer_km": None,
            "distance_km": 0,
            "energy_kwh": None,
            "kwh_per_km": None,
        }

    start = points[0]
    end = points[-1]

    duration_minutes = round((parse_ts(end["ts"]) - parse_ts(start["ts"])).total_seconds() / 60, 1)

    distance_km = 0
    if start.get("odometer_km") is not None and end.get("odometer_km") is not None:
        distance_km = round(end["odometer_km"] - start["odometer_km"], 2)

    energy_kwh = None
    kwh_per_km = None
    start_batt = start.get("battery_percent")
    end_batt = end.get("battery_percent")

    if (
        start_batt is not None and
        end_batt is not None and
        distance_km is not None and
        distance_km > 0
    ):
        energy_kwh = round(((start_batt - end_batt) / 100.0) * BATTERY_CAPACITY_KWH, 2)
        if energy_kwh >= 0:
            kwh_per_km = round(energy_kwh / distance_km, 3)
        else:
            energy_kwh = None

    return {
        "day": start["ts"][:10],
        "point_count": len(points),
        "start_ts": start["ts"],
        "end_ts": end["ts"],
        "duration_minutes": duration_minutes,
        "start_location": {
            "lat": start.get("lat"),
            "lon": start.get("lon"),
            "name": start.get("location_name"),
        },
        "end_location": {
            "lat": end.get("lat"),
            "lon": end.get("lon"),
            "name": end.get("location_name"),
        },
        "start_odometer_km": start.get("odometer_km"),
        "end_odometer_km": end.get("odometer_km"),
        "distance_km": distance_km,
        "energy_kwh": energy_kwh,
        "kwh_per_km": kwh_per_km,
    }

def segment_points_into_trips(points):
    """
    Split one day's points into separate trips.

    The current rule is simple: if the gap between two consecutive points is large
    enough, that starts a new trip. This is easy to reason about and tune later.
    """
    if not points:
        return []

    trips = []
    current = [points[0]]

    for prev, cur in zip(points, points[1:]):
        prev_ts = parse_ts(prev["ts"])
        cur_ts = parse_ts(cur["ts"])
        gap_min = (cur_ts - prev_ts).total_seconds() / 60.0

        if gap_min >= TRIP_GAP_MINUTES:
            trips.append(current)
            current = [cur]
        else:
            current.append(cur)

    if current:
        trips.append(current)

    return trips

def rebuild_trips_for_day(day):
    points = get_points_for_day(day)
    segments = segment_points_into_trips(points)

    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM trips WHERE day = ?", (day,))

    created = []

    for idx, seg in enumerate(segments, start=1):
        start = seg[0]
        end = seg[-1]

        duration_minutes = round(
            (parse_ts(end["ts"]) - parse_ts(start["ts"])).total_seconds() / 60, 1
        )

        distance_km = 0
        if start.get("odometer_km") is not None and end.get("odometer_km") is not None:
            distance_km = round(end["odometer_km"] - start["odometer_km"], 2)

        energy_kwh = estimate_trip_energy_kwh(
            start.get("battery_percent"),
            end.get("battery_percent")
        )

        kwh_per_km = None
        if energy_kwh is not None and distance_km > 0:
            kwh_per_km = round(energy_kwh / distance_km, 3)

        cur.execute(
            """
            INSERT INTO trips (
                day, trip_index, start_ts, end_ts,
                start_point_id, end_point_id,
                vehicle_id, vehicle_name,
                start_lat, start_lon, end_lat, end_lon,
                start_location_name, end_location_name,
                start_odometer_km, end_odometer_km,
                distance_km, duration_minutes, energy_kwh, kwh_per_km, point_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                day,
                idx,
                start["ts"],
                end["ts"],
                start["id"],
                end["id"],
                start.get("vehicle_id"),
                start.get("vehicle_name"),
                start.get("lat"),
                start.get("lon"),
                end.get("lat"),
                end.get("lon"),
                start.get("location_name"),
                end.get("location_name"),
                start.get("odometer_km"),
                end.get("odometer_km"),
                distance_km,
                duration_minutes,
                energy_kwh,
                kwh_per_km,
                len(seg),
            )
        )

        created.append({
            "trip_index": idx,
            "start_ts": start["ts"],
            "end_ts": end["ts"],
            "distance_km": distance_km,
            "duration_minutes": duration_minutes,
            "energy_kwh": energy_kwh,
            "kwh_per_km": kwh_per_km,
            "point_count": len(seg),
            "start_location_name": start.get("location_name"),
            "end_location_name": end.get("location_name"),
        })

    conn.commit()
    conn.close()
    return created

def get_trips_for_day(day):
    """Return all derived trips for one day."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM trips
        WHERE day = ?
        ORDER BY trip_index ASC
        """,
        (day,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_points_for_trip(day, trip_index):
    """Return only the raw points belonging to one derived trip."""
    trips = get_trips_for_day(day)
    match = None
    for trip in trips:
        if trip["trip_index"] == trip_index:
            match = trip
            break

    if not match:
        return []

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM trip_points
        WHERE id BETWEEN ? AND ?
        ORDER BY id ASC
        """,
        (match["start_point_id"], match["end_point_id"]),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def choose_sleep_seconds(last_point, movement_detected, fast_poll_countdown, sample_point=None):
    if movement_detected or fast_poll_countdown > 0:
        speed_kmh = estimate_speed_kmh(sample_point, last_point) if sample_point and last_point else None

        if speed_kmh is not None and speed_kmh >= LOCAL_TRIP_MAX_SPEED_KMH:
            return MOVING_POLL_INTERVAL_HIGHWAY_SECONDS

        return MOVING_POLL_INTERVAL_LOCAL_SECONDS

    if is_night_local():
        if night_short_poll_override:
            return POLL_INTERVAL_SECONDS
        return NIGHT_PARKED_POLL_INTERVAL_SECONDS

    return POLL_INTERVAL_SECONDS

def poll_loop():
    global pending_pre_movement_point

    fast_poll_countdown = 0

    while True:
        sleep_seconds = POLL_INTERVAL_SECONDS

        try:
            with lock:
                vehicles = fetch_vehicle_data()
                last = latest_point()
                inserted_days = set()
                movement_detected = False
                moving_reference_point = None

                for vehicle_point in vehicles:
                    moving_now = point_indicates_movement(vehicle_point, last)
                    should_store_now = should_store_point(vehicle_point, last)

                    if moving_now:
                        movement_detected = True
                        moving_reference_point = vehicle_point

                        if pending_pre_movement_point:
                            same_as_last = (
                                last is not None and
                                last.get("lat") == pending_pre_movement_point.get("lat") and
                                last.get("lon") == pending_pre_movement_point.get("lon") and
                                last.get("odometer_km") == pending_pre_movement_point.get("odometer_km") and
                                last.get("battery_percent") == pending_pre_movement_point.get("battery_percent") and
                                last.get("range_km") == pending_pre_movement_point.get("range_km")
                            )

                            if not same_as_last:
                                insert_point(pending_pre_movement_point)
                                inserted_days.add(pending_pre_movement_point["ts"][:10])
                                last = pending_pre_movement_point
                                should_store_now = should_store_point(vehicle_point, last)

                        pending_pre_movement_point = None

                    if should_store_now:
                        insert_point(vehicle_point)
                        inserted_days.add(vehicle_point["ts"][:10])
                        last = vehicle_point
                    else:
                        if not moving_now:
                            pending_pre_movement_point = dict(vehicle_point)

                for day in inserted_days:
                    rebuild_trips_for_day(day)

                if movement_detected:
                    fast_poll_countdown = RECENT_MOVEMENT_GRACE_POLLS
                elif fast_poll_countdown > 0:
                    fast_poll_countdown -= 1

                sleep_seconds = choose_sleep_seconds(
                    last_point=last,
                    movement_detected=movement_detected,
                    fast_poll_countdown=fast_poll_countdown,
                    sample_point=moving_reference_point,
                )

        except Exception as e:
            error_text = str(e)

            if "Exceeds number of requests" in error_text:
                sleep_seconds = max(sleep_seconds, 3600)

            print(
                json.dumps(
                    {
                        "ts": now_utc(),
                        "error": error_text,
                        "backoff_sleep_seconds": sleep_seconds,
                    }
                ),
                flush=True,
            )

        time.sleep(sleep_seconds)


@app.route("/")
def index():
    """Serve the main map UI."""
    return render_template("index.html")


@app.route("/health")
def health():
    """Simple health endpoint for Docker, curl, or Home Assistant checks."""
    return jsonify({"status": "ok", "ts": now_utc()})


@app.route("/status")
def status():
    """Return the latest known vehicle status in a compact JSON format."""
    point = latest_point()
    return jsonify(
        {
            "tracker_status": "running",
            "timestamp": point["ts"] if point else now_utc(),
            "source": point["source"] if point else "none",
            "vehicle": point["vehicle_name"] if point else None,
            "last_location": {
                "lat": point["lat"] if point else None,
                "lon": point["lon"] if point else None,
                "name": point["location_name"] if point else None,
            },
            "odometer_km": point["odometer_km"] if point else None,
            "battery_percent": point["battery_percent"] if point else None,
            "range_km": point["range_km"] if point else None,
        }
    )


@app.route("/debug/recent")
def debug_recent():
    """Debug endpoint showing the 50 most recent stored raw points."""
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trip_points ORDER BY id DESC LIMIT 50")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/days")
def api_days():
    """List days present in the database together with point counts."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT substr(ts, 1, 10) AS day, COUNT(*) AS points
        FROM trip_points
        GROUP BY substr(ts, 1, 10)
        ORDER BY day DESC
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/points")
def api_points():
    """
    Return raw point data for one day.
    If trip_index is supplied, return only the points for that trip.
    """
    day = request.args.get("day")
    trip_index = request.args.get("trip_index", type=int)

    if not day:
        return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400

    if trip_index:
        return jsonify(get_points_for_trip(day, trip_index))

    return jsonify(get_points_for_day(day))


@app.route("/api/summary")
def api_summary():
    """Return a whole-day summary derived from raw points."""
    day = request.args.get("day")
    if not day:
        return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400
    points = get_points_for_day(day)
    return jsonify(summarize_day(points))


@app.route("/api/trips")
def api_trips():
    """Return all derived trips for a specific day."""
    day = request.args.get("day")
    if not day:
        return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400
    return jsonify(get_trips_for_day(day))


@app.route("/admin/backfill_geocodes")
def admin_backfill_geocodes():
    """
    Maintenance endpoint that fills missing location_name values for older points.
    Limited to a small batch to keep each request predictable.
    """
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, lat, lon
            FROM trip_points
            WHERE location_name IS NULL
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            ORDER BY id ASC
            LIMIT 20
            """
        )
        rows = cur.fetchall()

        updated = []
        for row in rows:
            row_id = row["id"]
            lat = row["lat"]
            lon = row["lon"]
            try:
                name = reverse_geocode(lat, lon)
                if name:
                    cur.execute(
                        "UPDATE trip_points SET location_name = ? WHERE id = ?",
                        (name, row_id),
                    )
                    updated.append({"id": row_id, "location_name": name})
            except Exception as e:
                updated.append({"id": row_id, "error": str(e)})

        conn.commit()
        conn.close()
        return jsonify({"updated": updated, "count": len(updated)})
    except Exception as e:
        print(
            json.dumps(
                {
                    "ts": now_utc(),
                    "route": "admin_backfill_geocodes",
                    "error": str(e),
                }
            ),
            flush=True,
        )
        raise


@app.route("/admin/rebuild_trips")
def admin_rebuild_trips():
    """Maintenance endpoint that rebuilds one day's derived trip table."""
    try:
        day = request.args.get("day")
        if not day:
            return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400
        created = rebuild_trips_for_day(day)
        return jsonify({"day": day, "trips_created": created, "count": len(created)})
    except Exception as e:
        print(
            json.dumps(
                {
                    "ts": now_utc(),
                    "route": "admin_rebuild_trips",
                    "error": str(e),
                }
            ),
            flush=True,
        )
        raise

@app.route("/api/night_poll_override", methods=["GET"])
def api_night_poll_override():
    return jsonify(
        {
            "enabled": night_short_poll_override,
            "night_active": is_night_local(),
            "parked_poll_seconds": (
                POLL_INTERVAL_SECONDS
                if night_short_poll_override
                else NIGHT_PARKED_POLL_INTERVAL_SECONDS
            ),
        }
    )


@app.route("/api/night_poll_override", methods=["POST"])
def api_set_night_poll_override():
    global night_short_poll_override

    data = request.get_json(silent=True) or {}
    night_short_poll_override = bool(data.get("enabled", False))

    return jsonify(
        {
            "ok": True,
            "enabled": night_short_poll_override,
            "night_active": is_night_local(),
            "parked_poll_seconds": (
                POLL_INTERVAL_SECONDS
                if night_short_poll_override
                else NIGHT_PARKED_POLL_INTERVAL_SECONDS
            ),
        }
    )

if __name__ == "__main__":
    # Startup sequence:
    # 1. initialize the SQLite schema,
    # 2. start the background poller thread,
    # 3. start Flask so the UI and API routes are reachable.
    print("tracker.py starting", flush=True)
    print(
        f"DB_PATH={DB_PATH} PORT={PORT} GEOCODE_ENABLED={GEOCODE_ENABLED} "
        f"POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS} "
        f"MOVING_POLL_INTERVAL_SECONDS={MOVING_POLL_INTERVAL_SECONDS}",
        flush=True,
    )
    init_db()
    t = threading.Thread(target=poll_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT)
