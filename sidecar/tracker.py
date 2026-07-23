"""
Kia EV3 trip logger — Flask backend.

This single-file app has four jobs, all wired together through a small
number of shared helper functions:

1. POLL THE CAR: a background thread (poll_loop) periodically calls the Kia
   Connect API via fetch_vehicle_data(), decides whether the new reading is
   worth keeping (should_store_point / point_indicates_movement), and saves
   accepted readings ("points") with insert_point() into the trip_points
   table.
2. TURN POINTS INTO TRIPS: whenever new points are stored, poll_loop calls
   rebuild_trips_for_day(), which re-derives that day's "trips" (drives and
   charging stops) from scratch using segment_points_into_trips() and
   build_charging_segment_before_drive(), and rewrites the `trips` table for
   that day.
3. SERVE THE MAP UI: the "/" route renders index.html (a Leaflet map), and
   the "/api/*" routes below feed it JSON (days available, points, trip
   summaries) so the page's JavaScript can draw routes and stats.
4. INTEGRATE WITH HOME ASSISTANT: the "/ha/*" routes shape the same
   underlying data (latest point, per-day summary, trip list) into a form
   convenient for Home Assistant sensors/dashboards.

Call graph at a glance (arrows = "calls"):

    main (bottom of file)
      -> init_db()                         creates/migrates the SQLite schema
      -> poll_loop()  [background thread]
           -> fetch_vehicle_data() -> enrich_with_location_name() -> reverse_geocode()
           -> latest_point(), point_indicates_movement(), should_store_point(), same_point()
           -> insert_point()
           -> rebuild_trips_for_day()
                -> get_points_for_day()
                -> segment_points_into_trips()
                -> build_charging_segment_before_drive() -> find_previous_parked_point()
                -> estimate_trip_energy_kwh()
      -> app.run()                          serves the Flask routes defined near
                                             the bottom of this file (UI, /api/*,
                                             /ha/*, /admin/*)

Every database access goes through db(), which opens a fresh short-lived
SQLite connection (SQLite handles many small connections fine and this keeps
the background thread and Flask's request threads from fighting over one
connection object).
"""

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

# =============================================================================
# Configuration (environment variables)
# =============================================================================
# Every setting below can be tuned via Docker environment variables without
# touching code. Comments note which function(s) actually read each value.

# Core runtime configuration.
# These values can be tuned from Docker environment variables without editing code.
DB_PATH = os.getenv("DB_PATH", "/data/kia_trip_logger.db")  # used by db()
PORT = int(os.getenv("PORT", "8000"))  # used by app.run() at startup

# Slow/default poll interval, mainly used when the car appears parked.
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # used by poll_loop()

# Faster poll interval used while movement is detected so the map gets denser routes.
MOVING_POLL_INTERVAL_SECONDS = int(os.getenv("MOVING_POLL_INTERVAL_SECONDS", "20"))  # used by poll_loop()

# Keep polling quickly for a few cycles after the last detected movement.
# This avoids dropping back to a slow interval too early during a trip.
RECENT_MOVEMENT_GRACE_POLLS = int(os.getenv("RECENT_MOVEMENT_GRACE_POLLS", "6"))  # used by poll_loop()

# Storage thresholds.
# A point is only stored when movement or odometer change is meaningful enough.
# Read by should_store_point(), point_indicates_movement(), point_has_meaningful_travel()
# and segment_points_into_trips().
MIN_MOVEMENT_METERS = float(os.getenv("MIN_MOVEMENT_METERS", "50"))
MIN_ODOMETER_DELTA_KM = float(os.getenv("MIN_ODOMETER_DELTA_KM", "0.1"))
TRIP_GAP_MINUTES = int(os.getenv("TRIP_GAP_MINUTES", "15"))  # used by segment_points_into_trips()

# Reverse geocoding settings for converting coordinates into readable place names.
# Read by reverse_geocode() and geocode_key().
GEOCODE_ENABLED = os.getenv("GEOCODE_ENABLED", "true").lower() == "true"
GEOCODE_CACHE_DECIMALS = int(os.getenv("GEOCODE_CACHE_DECIMALS", "3"))
GEOCODE_EMAIL = os.getenv("GEOCODE_EMAIL", "")
GEOCODE_USER_AGENT = os.getenv("GEOCODE_USER_AGENT", "kia-trip-logger/1.0")

# Kia API login configuration. Read by fetch_vehicle_data() when constructing
# the VehicleManager client from hyundai_kia_connect_api.
KIA_REGION = int(os.getenv("KIA_REGION", "1"))
KIA_BRAND = int(os.getenv("KIA_BRAND", "1"))
KIA_USERNAME = os.environ["KIA_USERNAME"]
KIA_PASSWORD = os.environ["KIA_PASSWORD"]
KIA_PIN = os.getenv("KIA_PIN", "")

# Kia EV3 Battery capacity.
# BATTERY_CAPACITY_KWH is used by summarize_day() (whole-day energy estimate).
# USABLE_BATTERY_KWH is used by estimate_trip_energy_kwh() and
# build_charging_segment_before_drive() (per-trip / per-charge energy estimate).
BATTERY_CAPACITY_KWH = float(os.getenv("BATTERY_CAPACITY_KWH", "81.4"))
USABLE_BATTERY_KWH = float(os.getenv("USABLE_BATTERY_KWH", "74"))

# Shared mutable state used across poll cycles in poll_loop():
# - lock: guards a full poll cycle so overlapping timer fires cannot interleave.
# - last_geocode_ts: last time reverse_geocode() called Nominatim, for rate limiting.
# - pending_pre_movement_point: a point that looked "parked" but is held back one
#   cycle, so that if the car starts moving next cycle we can still record the
#   moment right before departure (see poll_loop() for the full logic).
lock = threading.Lock()
last_geocode_ts = 0.0
pending_pre_movement_point = None


# =============================================================================
# Time helpers
# =============================================================================

def now_utc():
    """
    Return the current UTC timestamp in ISO-8601 format.

    Used all over the file (poll_loop, error logging, Home Assistant builders,
    /health, /status routes, etc.) any time "now" needs to be recorded or
    returned to a caller.
    """
    return datetime.now(timezone.utc).isoformat()


def parse_ts(ts):
    """
    Parse an ISO-8601 timestamp string (as stored in trip_points/trips) back
    into a datetime object so durations can be computed.

    Called by: summarize_day(), segment_points_into_trips(),
    build_charging_segment_before_drive(), rebuild_trips_for_day().
    """
    return datetime.fromisoformat(ts)


def estimate_trip_energy_kwh(start_battery_percent, end_battery_percent):
    """
    Estimate energy used during a drive from the battery percentage drop.

    Returns None if either reading is missing, or if the battery percentage
    went UP (that would mean the car charged during the drive, e.g. via
    regenerative braking beyond expected noise, which this simple model does
    not try to account for).

    Called by: rebuild_trips_for_day() when assembling each "drive" segment.
    """
    if start_battery_percent is None or end_battery_percent is None:
        return None
    delta_percent = start_battery_percent - end_battery_percent
    if delta_percent < 0:
        return None
    return round(USABLE_BATTERY_KWH * delta_percent / 100.0, 2)


# =============================================================================
# Database setup
# =============================================================================

def db():
    """
    Open a short-lived SQLite connection with row access by column name
    (so rows can be read like dicts, e.g. row["lat"]).

    Nearly every function that touches the database calls this to get its
    own connection rather than sharing one — SQLite handles this cheaply,
    and it avoids issues between the poll_loop background thread and
    Flask's request-handling threads.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Create the SQLite schema if it does not already exist, and migrate it
    forward if the code has grown new columns since the database was created.

    Tables:
    - trip_points: raw point history collected from the Kia API.
    - geocode_cache: small cache so we do not reverse-geocode the same area repeatedly.
    - trips: derived trip segments rebuilt from trip_points.

    Called once at startup, from the `if __name__ == "__main__":` block,
    before the poller thread or Flask start. The ALTER TABLE loops below let
    older databases pick up newly added columns (e.g. energy_kwh, kind)
    without losing existing data.
    """
    conn = db()
    cur = conn.cursor()

    cur.execute("""
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
        """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trip_points_ts ON trip_points(ts)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            key TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            location_name TEXT,
            raw_json TEXT,
            created_ts TEXT
        )
        """)

    cur.execute("""
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
        """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trips_day ON trips(day)")

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

    # Schema migration: add any trip_points columns that are defined above
    # but missing from an existing (older) database file.
    cur.execute("PRAGMA table_info(trip_points)")
    point_columns = [row["name"] for row in cur.fetchall()]

    for col, col_type in point_column_defs.items():
        if col not in point_columns:
            cur.execute(f"ALTER TABLE trip_points ADD COLUMN {col} {col_type}")

    # Same migration pattern as above, but for the derived `trips` table.
    cur.execute("PRAGMA table_info(trips)")
    trip_columns = [row["name"] for row in cur.fetchall()]

    trip_column_defs = {
        "energy_kwh": "REAL",
        "kwh_per_km": "REAL",
        "arrival_battery_percent": "REAL",
        "next_departure_battery_percent": "REAL",
        "charge_gain_percent": "REAL",
        "next_departure_ts": "TEXT",
        "kind": "TEXT",
    }

    for col, col_type in trip_column_defs.items():
        if col not in trip_columns:
            cur.execute(f"ALTER TABLE trips ADD COLUMN {col} {col_type}")


# =============================================================================
# Geometry helper
# =============================================================================

def haversine_m(lat1, lon1, lat2, lon2):
    """
    Calculate approximate great-circle distance in meters between two
    latitude/longitude points using the haversine formula.

    Called by: should_store_point(), point_indicates_movement(),
    point_has_meaningful_travel(), segment_points_into_trips() (indirectly,
    via point_has_meaningful_travel), and find_previous_parked_point().
    Returns None if any coordinate is missing.
    """
    if None in (lat1, lon1, lat2, lon2):
        return None

    r = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# =============================================================================
# Point queries
# =============================================================================

def latest_point():
    """
    Return the newest stored point (as a dict) or None if the database is
    empty.

    Called by: poll_loop() (to compare the latest poll against what's already
    stored), build_home_assistant_state(), and the /status route.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trip_points ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


# =============================================================================
# Geocoding (coordinates -> readable place name), with a small local cache
# =============================================================================

def geocode_key(lat, lon):
    """
    Round coordinates into a cache key so nearby points can reuse the same
    geocode lookup instead of hitting Nominatim again.

    Called by: get_cached_geocode(), store_cached_geocode().
    """
    return f"{round(lat, GEOCODE_CACHE_DECIMALS)},{round(lon, GEOCODE_CACHE_DECIMALS)}"


def get_cached_geocode(lat, lon):
    """
    Fetch a previously cached place name for these coordinates, if available.

    Called by: reverse_geocode(), as the first thing it tries before hitting
    the network.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT location_name FROM geocode_cache WHERE key = ?",
        (geocode_key(lat, lon),),
    )
    row = cur.fetchone()
    conn.close()
    return row["location_name"] if row else None


def store_cached_geocode(lat, lon, location_name, raw_json):
    """
    Store reverse-geocode results so repeated lookups are avoided.

    Called by: reverse_geocode(), after a successful Nominatim lookup.
    """
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

    Called by: reverse_geocode(), to turn the raw Nominatim JSON response
    into the string that ends up in point["location_name"].
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

    Calls: get_cached_geocode() (cache check), format_location_name()
    (shape the response), store_cached_geocode() (persist the result).
    Called by: enrich_with_location_name() (during normal polling) and the
    /admin/backfill_geocodes route (batch-filling older points).
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

    Calls: reverse_geocode(). Called by: fetch_vehicle_data(), once per
    vehicle reading pulled from the Kia API.
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


# =============================================================================
# Point storage and movement/storage decisions
# =============================================================================

def insert_point(point):
    """
    Persist one collected vehicle point into SQLite (trip_points table).

    Called by: poll_loop(), for each point it decides is worth keeping
    (see should_store_point() / point_indicates_movement() below).
    """
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

    Calls: haversine_m(). Called by: poll_loop(), for every poll cycle.
    Compare with point_indicates_movement() below, which uses a lower
    (more sensitive) threshold to decide polling speed rather than storage.
    """
    if not old_point:
        return True

    moved = haversine_m(
        old_point.get("lat"),
        old_point.get("lon"),
        new_point.get("lat"),
        new_point.get("lon"),
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

    Calls: haversine_m(). Called by: poll_loop(), to decide whether to speed
    up the polling interval (see MOVING_POLL_INTERVAL_SECONDS /
    RECENT_MOVEMENT_GRACE_POLLS) and how to handle the pending pre-movement
    point.
    """
    if not old_point:
        return False

    moved = haversine_m(
        old_point.get("lat"),
        old_point.get("lon"),
        new_point.get("lat"),
        new_point.get("lon"),
    )
    old_odo = old_point.get("odometer_km")
    new_odo = new_point.get("odometer_km")

    gps_motion = moved is not None and moved >= max(15.0, MIN_MOVEMENT_METERS / 2.0)
    odo_motion = old_odo is not None and new_odo is not None and new_odo > old_odo
    return gps_motion or odo_motion


def _as_int_bool(value):
    """
    Normalize a Python boolean/None into SQLite-friendly 1/0/None.

    Called by: fetch_vehicle_data(), for each boolean-ish vehicle attribute
    (charging, plugged_in, climate_on, engine_running, locked, etc.) before
    it gets stored.
    """
    if value is None:
        return None
    return 1 if bool(value) else 0


# =============================================================================
# Kia API access
# =============================================================================

def fetch_vehicle_data():
    """
    Pull the latest vehicle state from the Kia API for every vehicle on the
    account.

    Each returned dictionary is shaped to match what the database and API routes
    expect, so the rest of the app can stay simple.

    Calls: _as_int_bool() (normalize booleans), enrich_with_location_name()
    (fill in a place name for each point). Called by: poll_loop(), once per
    poll cycle.
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
            "plugged_in": _as_int_bool(
                getattr(vehicle, "ev_battery_is_plugged_in", None)
            ),
            "charging_power_kw": getattr(vehicle, "ev_charging_power", None),
            "charge_port_open": _as_int_bool(
                getattr(vehicle, "ev_charge_port_door_is_open", None)
            ),
            "remaining_charge_minutes": getattr(
                vehicle, "ev_estimated_current_charge_duration", None
            ),
            "climate_on": _as_int_bool(getattr(vehicle, "air_control_is_on", None)),
            "engine_running": _as_int_bool(getattr(vehicle, "engine_is_running", None)),
            "locked": _as_int_bool(getattr(vehicle, "is_locked", None)),
            "car_battery_percent": getattr(vehicle, "car_battery_percentage", None),
            "vehicle_status_ts": getattr(vehicle, "last_updated_at", None),
        }
        results.append(enrich_with_location_name(point))
    return results


# =============================================================================
# Day-level queries and summaries
# =============================================================================

def get_points_for_day(day):
    """
    Return all raw points for one day (YYYY-MM-DD), sorted oldest to newest.

    Called by: api_points() ("/api/points"), api_summary() ("/api/summary"),
    build_home_assistant_day(), and rebuild_trips_for_day() (which then
    segments these points into trips).
    """
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


def summarize_day(points, trips=None):
    """
    Build a whole-day summary (start/end time, distance, energy estimate)
    from a list of raw points for that day.

    Energy figures: if `trips` (the day's rows from get_trips_for_day(), each
    already tagged with kind="drive"/"charge") is supplied, energy_kwh and
    kwh_per_km are computed by summing only the DRIVE trips' individually
    estimated energy_kwh and distance_km. This is important because a naive
    whole-day battery_percent(start) - battery_percent(end) calculation would
    net out any charging that happened between drives on the same day against
    the energy consumed while driving -- e.g. drive down 20%, charge up 25%,
    drive down 15% nets to only a 10% drop for the whole day, drastically
    understating (or even hiding) real consumption. Per-trip energy_kwh values
    (from estimate_trip_energy_kwh() in rebuild_trips_for_day()) don't have
    this problem because each drive trip's battery delta is measured only
    across that trip's own start/end points, never spanning a charge.

    If `trips` is omitted, falls back to the old whole-day battery-delta
    estimate (kept for any caller that only has points, not trips, handy).

    Calls: parse_ts(). Called by: api_summary() ("/api/summary") and
    build_home_assistant_day(). Note the non-energy fields (distance,
    duration, locations) are always a coarse whole-day estimate from raw
    points; only the energy fields use trips when available.
    """
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

    duration_minutes = round(
        (parse_ts(end["ts"]) - parse_ts(start["ts"])).total_seconds() / 60, 1
    )

    distance_km = 0
    if start.get("odometer_km") is not None and end.get("odometer_km") is not None:
        distance_km = round(end["odometer_km"] - start["odometer_km"], 2)

    energy_kwh = None
    kwh_per_km = None

    if trips is not None:
        # Preferred path: sum only the drive trips' own energy_kwh/distance_km,
        # so charging stops on the same day can never offset driving energy.
        drive_trips = [t for t in trips if t.get("kind") != "charge"]
        drive_distance_km = sum(
            float(t.get("distance_km") or 0) for t in drive_trips
        )
        drive_energy_kwh = sum(
            float(t.get("energy_kwh") or 0)
            for t in drive_trips
            if t.get("energy_kwh") is not None
        )
        if drive_distance_km > 0 and drive_energy_kwh > 0:
            energy_kwh = round(drive_energy_kwh, 2)
            kwh_per_km = round(drive_energy_kwh / drive_distance_km, 3)
    else:
        # Fallback used only when no trips list is available: estimate from
        # the whole day's raw start/end battery_percent. Can be skewed by any
        # charging that happened between drives on the same day (see
        # docstring above), so prefer passing `trips` whenever possible.
        start_batt = start.get("battery_percent")
        end_batt = end.get("battery_percent")

        if (
            start_batt is not None
            and end_batt is not None
            and distance_km is not None
            and distance_km > 0
        ):
            energy_kwh = round(
                ((start_batt - end_batt) / 100.0) * BATTERY_CAPACITY_KWH, 2
            )
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


# =============================================================================
# Trip segmentation
# =============================================================================
# The functions below turn a day's raw points into discrete "trips"
# (drives) and, separately, charging stops inserted just before a drive.
# rebuild_trips_for_day() is the orchestrator; the rest are its helpers.

def is_charging_point(point):
    """True if this reading looks like the car is charging or plugged in.
    Called by: segment_points_into_trips() (to detect charging->driving boundaries)."""
    return bool(point.get("charging")) or bool(point.get("plugged_in"))


def is_active_point(point):
    """True if this reading looks like the car is "in use" (engine or climate on).
    Called by: segment_points_into_trips() and find_previous_parked_point()."""
    return bool(point.get("engine_running")) or bool(point.get("climate_on"))


def point_has_meaningful_travel(prev, cur):
    """
    True if the odometer or GPS position changed enough between two
    consecutive points to count as "actual travel" rather than GPS/sensor
    noise while parked.

    Calls: haversine_m(). Called by: segment_points_into_trips().
    """
    moved_m = haversine_m(
        prev.get("lat"), prev.get("lon"), cur.get("lat"), cur.get("lon")
    )

    prev_odo = prev.get("odometer_km")
    cur_odo = cur.get("odometer_km")
    odo_delta = None
    if prev_odo is not None and cur_odo is not None:
        odo_delta = cur_odo - prev_odo

    return (moved_m is not None and moved_m >= MIN_MOVEMENT_METERS) or (
        odo_delta is not None and odo_delta >= MIN_ODOMETER_DELTA_KM
    )


def segment_points_into_trips(points):
    """
    Split one day's ordered raw points into a list of trip segments (each
    segment is a list of points belonging to one continuous drive).

    A new segment starts when any of these boundaries is detected between
    consecutive points:
    - charging_boundary: was charging/plugged in, now isn't (gap >= 3 min)
    - parked_then_restart: was inactive, is now active, and moved meaningfully
    - long_idle_without_travel: gap >= TRIP_GAP_MINUTES with no real travel

    After initial splitting, very short/motionless segments are filtered out
    (a segment needs real distance or GPS movement to count as a trip).

    Calls: parse_ts(), point_has_meaningful_travel(), is_charging_point(),
    is_active_point(), haversine_m(). Called by: rebuild_trips_for_day(),
    which then wraps each returned segment into a "drive" record (and looks
    for a preceding "charge" segment via build_charging_segment_before_drive()).
    """
    if not points:
        return []

    trips = []
    current = [points[0]]

    for prev, cur in zip(points, points[1:]):
        prev_ts = parse_ts(prev["ts"])
        cur_ts = parse_ts(cur["ts"])
        gap_min = (cur_ts - prev_ts).total_seconds() / 60.0

        meaningful_travel = point_has_meaningful_travel(prev, cur)

        charging_boundary = (
            is_charging_point(prev) and not is_charging_point(cur) and gap_min >= 3
        )

        parked_then_restart = (
            not is_active_point(prev) and is_active_point(cur) and meaningful_travel
        )

        long_idle_without_travel = gap_min >= TRIP_GAP_MINUTES and not meaningful_travel

        should_split_before_cur = (
            charging_boundary or parked_then_restart or long_idle_without_travel
        )

        if should_split_before_cur:
            if current:
                trips.append(current)

            if parked_then_restart:
                current = [prev, cur] if prev != current[-1] else [current[-1], cur]
            else:
                current = [cur]
        else:
            current.append(cur)

    if current:
        deduped = [current[0]]
        for p in current[1:]:
            if p["id"] != deduped[-1]["id"]:
                deduped.append(p)
        trips.append(deduped)

    cleaned = []
    for seg in trips:
        deduped = [seg[0]]
        for p in seg[1:]:
            if p["id"] != deduped[-1]["id"]:
                deduped.append(p)

        if not deduped:
            continue

        if len(deduped) == 1:
            continue

        first = deduped[0]
        last = deduped[-1]

        distance_km = None
        if first.get("odometer_km") is not None and last.get("odometer_km") is not None:
            distance_km = last["odometer_km"] - first["odometer_km"]

        moved_m = haversine_m(
            first.get("lat"), first.get("lon"), last.get("lat"), last.get("lon")
        )

        has_trip_motion = (
            distance_km is not None and distance_km >= MIN_ODOMETER_DELTA_KM
        ) or (moved_m is not None and moved_m >= MIN_MOVEMENT_METERS)

        if has_trip_motion:
            cleaned.append(deduped)

    return cleaned


# =============================================================================
# Charging-stop detection
# =============================================================================

def find_previous_parked_point(start_point):
    """
    Look backwards (up to 50 prior stored points) from the start of a drive
    to find the parked point right before it, matched by either a nearly
    identical odometer reading or a nearby GPS location.

    This is how a charging stop is detected: if the car's battery is higher
    at the start of a new drive than it was at that previous parked point,
    the difference must have come from charging while parked.

    Calls: haversine_m(), is_active_point(). Called by:
    build_charging_segment_before_drive().
    """
    if not start_point:
        return None

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM trip_points
        WHERE id < ?
        ORDER BY id DESC
        LIMIT 50
        """,
        (start_point["id"],),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    start_odo = start_point.get("odometer_km")
    start_lat = start_point.get("lat")
    start_lon = start_point.get("lon")

    for row in rows:
        same_odo = (
            start_odo is not None
            and row.get("odometer_km") is not None
            and abs(row["odometer_km"] - start_odo) < 0.01
        )

        same_place = False
        moved_m = haversine_m(start_lat, start_lon, row.get("lat"), row.get("lon"))
        if moved_m is not None and moved_m < 100:
            same_place = True

        parked = not is_active_point(row)

        if parked and (same_odo or same_place):
            return row

    return None


def build_charging_segment_before_drive(start_point):
    """
    Given the first point of a drive segment, check whether battery percent
    increased since the last time the car was parked, and if so build a
    synthetic "charge" trip record covering that idle/charging period.

    Returns None if there is no matching previous parked point, or if the
    battery percent did not actually increase (i.e. no charging happened).

    Calls: find_previous_parked_point(), parse_ts(). Called by:
    rebuild_trips_for_day(), once per drive segment, to optionally insert a
    "charge" trip immediately before the corresponding "drive" trip.
    """
    previous_parked = find_previous_parked_point(start_point)
    if not previous_parked:
        return None

    prev_batt = previous_parked.get("battery_percent")
    start_batt = start_point.get("battery_percent")

    if prev_batt is None or start_batt is None:
        return None

    if start_batt <= prev_batt:
        return None

    return {
        "kind": "charge",
        "start": previous_parked,
        "end": start_point,
        "distance_km": 0.0,
        "duration_minutes": round(
            (
                parse_ts(start_point["ts"]) - parse_ts(previous_parked["ts"])
            ).total_seconds()
            / 60,
            1,
        ),
        "energy_kwh": round(USABLE_BATTERY_KWH * (start_batt - prev_batt) / 100.0, 2),
        "kwh_per_km": None,
        "arrival_battery_percent": prev_batt,
        "next_departure_battery_percent": start_batt,
        "charge_gain_percent": round(start_batt - prev_batt, 1),
        "next_departure_ts": start_point.get("ts"),
        "point_count": 2,
        "start_location_name": previous_parked.get("location_name"),
        "end_location_name": start_point.get("location_name"),
    }


# =============================================================================
# Trip persistence (rebuild the `trips` table for one day)
# =============================================================================

def rebuild_trips_for_day(day):
    """
    Recompute and overwrite all trips for one day (YYYY-MM-DD).

    Steps:
    1. Load that day's raw points (get_points_for_day) and split them into
       drive segments (segment_points_into_trips).
    2. For each drive segment, optionally prepend a "charge" segment
       (build_charging_segment_before_drive) if the car charged while
       parked just before this drive.
    3. Compute distance/duration/energy for each segment
       (estimate_trip_energy_kwh, parse_ts) and delete + reinsert that day's
       rows in the `trips` table so it always reflects the latest points.

    Called by: poll_loop() (after storing new points for a day) and the
    /admin/rebuild_trips route (manual re-processing).
    Returns a list of the newly created trip dicts (used by the admin route
    to report what was created).
    """
    points = get_points_for_day(day)
    segments = segment_points_into_trips(points)

    output_segments = []

    for seg in segments:
        start = seg[0]
        end = seg[-1]

        charge_seg = build_charging_segment_before_drive(start)
        if charge_seg:
            output_segments.append(charge_seg)

        distance_km = 0.0
        if start.get("odometer_km") is not None and end.get("odometer_km") is not None:
            distance_km = round(end["odometer_km"] - start["odometer_km"], 2)

        duration_minutes = round(
            (parse_ts(end["ts"]) - parse_ts(start["ts"])).total_seconds() / 60, 1
        )

        energy_kwh = estimate_trip_energy_kwh(
            start.get("battery_percent"), end.get("battery_percent")
        )

        kwh_per_km = None
        if energy_kwh is not None and distance_km > 0:
            kwh_per_km = round(energy_kwh / distance_km, 3)

        output_segments.append(
            {
                "kind": "drive",
                "start": start,
                "end": end,
                "distance_km": distance_km,
                "duration_minutes": duration_minutes,
                "energy_kwh": energy_kwh,
                "kwh_per_km": kwh_per_km,
                "arrival_battery_percent": end.get("battery_percent"),
                "next_departure_battery_percent": None,
                "charge_gain_percent": None,
                "next_departure_ts": None,
                "point_count": len(seg),
                "start_location_name": start.get("location_name"),
                "end_location_name": end.get("location_name"),
            }
        )

    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM trips WHERE day = ?", (day,))

    created = []

    for idx, item in enumerate(output_segments, start=1):
        start = item["start"]
        end = item["end"]

        cur.execute(
            """
            INSERT INTO trips (
                day, trip_index, start_ts, end_ts,
                start_point_id, end_point_id,
                vehicle_id, vehicle_name,
                start_lat, start_lon, end_lat, end_lon,
                start_location_name, end_location_name,
                start_odometer_km, end_odometer_km,
                distance_km, duration_minutes, energy_kwh, kwh_per_km, point_count,
                arrival_battery_percent, next_departure_battery_percent,
                charge_gain_percent, next_departure_ts, kind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                day,
                idx,
                start["ts"],
                end["ts"],
                start.get("id"),
                end.get("id"),
                start.get("vehicle_id"),
                start.get("vehicle_name"),
                start.get("lat"),
                start.get("lon"),
                end.get("lat"),
                end.get("lon"),
                item.get("start_location_name"),
                item.get("end_location_name"),
                start.get("odometer_km"),
                end.get("odometer_km"),
                item["distance_km"],
                item["duration_minutes"],
                item["energy_kwh"],
                item["kwh_per_km"],
                item["point_count"],
                item["arrival_battery_percent"],
                item["next_departure_battery_percent"],
                item["charge_gain_percent"],
                item["next_departure_ts"],
                item["kind"],
            ),
        )

        created.append(
            {
                "trip_index": idx,
                "kind": item["kind"],
                "start_ts": start["ts"],
                "end_ts": end["ts"],
                "distance_km": item["distance_km"],
                "duration_minutes": item["duration_minutes"],
                "energy_kwh": item["energy_kwh"],
                "kwh_per_km": item["kwh_per_km"],
                "point_count": item["point_count"],
                "start_location_name": item.get("start_location_name"),
                "end_location_name": item.get("end_location_name"),
                "arrival_battery_percent": item["arrival_battery_percent"],
                "next_departure_battery_percent": item[
                    "next_departure_battery_percent"
                ],
                "charge_gain_percent": item["charge_gain_percent"],
                "next_departure_ts": item["next_departure_ts"],
            }
        )

    conn.commit()
    conn.close()
    return created


# =============================================================================
# Trip queries
# =============================================================================

def get_trips_for_day(day):
    """
    Return all derived trips for one day (YYYY-MM-DD), in trip_index order.

    Called by: get_points_for_trip(), build_home_assistant_day(),
    api_trips() ("/api/trips"), ha_trips() ("/ha/trips"), and ha_day()
    (indirectly, via build_home_assistant_day()).
    """
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


def get_trips_for_range(start_day, end_day):
    """
    Return all derived trips for an inclusive day range (start_day..end_day).

    Called by: api_trips_range() ("/api/trips_range"), used by the frontend's
    date-range map view.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM trips
        WHERE day >= ? AND day <= ?
        ORDER BY day ASC, trip_index ASC
        """,
        (start_day, end_day),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_points_for_trip(day, trip_index):
    """
    Return only the raw points belonging to one derived trip, identified by
    day + trip_index.

    Calls: get_trips_for_day() (to find the trip's point ID range), then
    queries trip_points directly for IDs between start_point_id and
    end_point_id. Called by: api_points() ("/api/points") when a
    trip_index query parameter is supplied.
    """
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


# =============================================================================
# Background polling loop
# =============================================================================

def same_point(a, b):
    """
    Compare two points on their meaningful fields (lat/lon/odometer/battery/
    range) to detect duplicate readings, ignoring fields like timestamp that
    always differ between polls.

    Called by: poll_loop(), to avoid re-inserting a point that hasn't
    actually changed since the last stored one.
    """
    if not a or not b:
        return False
    return (
        a.get("lat") == b.get("lat")
        and a.get("lon") == b.get("lon")
        and a.get("odometer_km") == b.get("odometer_km")
        and a.get("battery_percent") == b.get("battery_percent")
        and a.get("range_km") == b.get("range_km")
    )


def poll_loop():
    """
    Background thread entry point: runs forever, polling the Kia API on an
    adaptive interval and turning new readings into stored points and
    rebuilt trips.

    Each cycle, while holding `lock`:
    1. fetch_vehicle_data() to get the latest reading per vehicle.
    2. For each vehicle, compare against latest_point() using
       point_indicates_movement() / should_store_point() / same_point() to
       decide whether to insert_point() the pending "pre-movement" point
       and/or the new point.
    3. rebuild_trips_for_day() for any day that received new points.
    4. Decide the next sleep interval: MOVING_POLL_INTERVAL_SECONDS while
       recent movement was seen (tracked via fast_poll_countdown, refilled
       from RECENT_MOVEMENT_GRACE_POLLS), otherwise POLL_INTERVAL_SECONDS.

    Errors are caught, logged as JSON, and back off to at least 1 hour if
    the Kia API reports a rate limit ("Exceeds number of requests").

    Calls: fetch_vehicle_data(), latest_point(), point_indicates_movement(),
    should_store_point(), same_point(), insert_point(), rebuild_trips_for_day().
    Called by: started as a daemon thread in the `if __name__ == "__main__":`
    block at the bottom of this file.
    """
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

                for vehicle_point in vehicles:
                    previous_last = dict(last) if last else None

                    moving_now = point_indicates_movement(vehicle_point, last)
                    should_store_now = should_store_point(vehicle_point, last)

                    if moving_now:
                        movement_detected = True

                        if pending_pre_movement_point and not same_point(
                            last, pending_pre_movement_point
                        ):
                            insert_point(pending_pre_movement_point)
                            inserted_days.add(pending_pre_movement_point["ts"][:10])
                            last = pending_pre_movement_point

                        pending_pre_movement_point = None

                        if not should_store_now and not same_point(last, vehicle_point):
                            insert_point(vehicle_point)
                            inserted_days.add(vehicle_point["ts"][:10])
                            last = vehicle_point
                            continue

                    if should_store_now:
                        if (
                            previous_last
                            and not moving_now
                            and not same_point(previous_last, vehicle_point)
                        ):
                            pending_pre_movement_point = dict(previous_last)

                        if not same_point(last, vehicle_point):
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

                if fast_poll_countdown > 0:
                    sleep_seconds = MOVING_POLL_INTERVAL_SECONDS

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


# =============================================================================
# Home Assistant integration helpers
# =============================================================================

def iso_day_utc():
    """
    Return today's date (UTC) as YYYY-MM-DD.

    Called by: ha_day() and ha_trips(), as the default `day` when the
    request does not supply one explicitly.
    """
    return now_utc()[:10]


def build_home_assistant_state():
    """
    Shape the most recent point into a Home Assistant sensor-style payload
    (a "state" string plus an "attributes" dict).

    Calls: latest_point(), now_utc(). Called by: ha_state() ("/ha/state").
    """
    point = latest_point()

    if not point:
        return {
            "state": "unavailable",
            "last_updated": now_utc(),
            "attributes": {
                "tracker_status": "running",
                "vehicle": None,
                "battery_percent": None,
                "range_km": None,
                "odometer_km": None,
                "charging": None,
                "plugged_in": None,
                "charging_power_kw": None,
                "location_name": None,
                "lat": None,
                "lon": None,
                "vehicle_status_ts": None,
            },
        }

    return {
        "state": (
            "charging"
            if point.get("charging")
            else (
                "driving"
                if point.get("engine_running")
                else "plugged_in" if point.get("plugged_in") else "parked"
            )
        ),
        "last_updated": point.get("ts"),
        "attributes": {
            "tracker_status": "running",
            "vehicle": point.get("vehicle_name"),
            "source": point.get("source"),
            "battery_percent": point.get("battery_percent"),
            "range_km": point.get("range_km"),
            "total_range_km": point.get("total_range_km"),
            "odometer_km": point.get("odometer_km"),
            "charging": (
                bool(point.get("charging"))
                if point.get("charging") is not None
                else None
            ),
            "plugged_in": (
                bool(point.get("plugged_in"))
                if point.get("plugged_in") is not None
                else None
            ),
            "charging_power_kw": point.get("charging_power_kw"),
            "charge_port_open": (
                bool(point.get("charge_port_open"))
                if point.get("charge_port_open") is not None
                else None
            ),
            "remaining_charge_minutes": point.get("remaining_charge_minutes"),
            "climate_on": (
                bool(point.get("climate_on"))
                if point.get("climate_on") is not None
                else None
            ),
            "engine_running": (
                bool(point.get("engine_running"))
                if point.get("engine_running") is not None
                else None
            ),
            "locked": (
                bool(point.get("locked")) if point.get("locked") is not None else None
            ),
            "car_battery_percent": point.get("car_battery_percent"),
            "location_name": point.get("location_name"),
            "lat": point.get("lat"),
            "lon": point.get("lon"),
            "vehicle_status_ts": point.get("vehicle_status_ts"),
        },
    }


def build_home_assistant_day(day):
    """
    Shape one day's summary + trip list into a Home Assistant sensor-style
    payload, including a per-day drive efficiency figure (kWh per 100 km)
    instead of raw total consumed/charged kWh.

    Calls: get_points_for_day(), get_trips_for_day(), summarize_day(),
    now_utc(). Called by: ha_day() ("/ha/day").
    """
    points = get_points_for_day(day)
    trips = get_trips_for_day(day)
    # Pass trips into summarize_day() so its energy figures are computed from
    # drive trips only, same reasoning as api_summary() above.
    summary = summarize_day(points, trips)

    drive_trips = [t for t in trips if t.get("kind") != "charge"]
    charge_trips = [t for t in trips if t.get("kind") == "charge"]

    total_drive_distance_km = round(
        sum(float(t.get("distance_km") or 0) for t in drive_trips), 2
    )
    # Sum of each drive trip's estimated energy use, kept as an intermediate
    # value only (not exposed directly) so we can derive a single
    # kWh-per-100km efficiency figure below.
    total_drive_energy_kwh = sum(
        float(t.get("energy_kwh") or 0)
        for t in drive_trips
        if t.get("energy_kwh") is not None
    )
    # Day-level drive efficiency: total energy consumed across all drive
    # trips divided by total distance driven, expressed as kWh per 100 km
    # (same unit convention used for trip-level efficiency elsewhere in the
    # app). None when there was no drive distance to divide by.
    total_drive_kwh_per_100km = (
        round((total_drive_energy_kwh / total_drive_distance_km) * 100, 1)
        if total_drive_distance_km > 0
        else None
    )
    total_charge_gain_percent = round(
        sum(
            float(t.get("charge_gain_percent") or 0)
            for t in charge_trips
            if t.get("charge_gain_percent") is not None
        ),
        1,
    )

    return {
        "state": day,
        "last_updated": now_utc(),
        "attributes": {
            "day": day,
            "point_count": summary.get("point_count"),
            "start_ts": summary.get("start_ts"),
            "end_ts": summary.get("end_ts"),
            "duration_minutes": summary.get("duration_minutes"),
            "start_odometer_km": summary.get("start_odometer_km"),
            "end_odometer_km": summary.get("end_odometer_km"),
            "summary_distance_km": summary.get("distance_km"),
            "summary_energy_kwh": summary.get("energy_kwh"),
            "summary_kwh_per_km": summary.get("kwh_per_km"),
            "drive_trip_count": len(drive_trips),
            "charge_trip_count": len(charge_trips),
            "total_drive_distance_km": total_drive_distance_km,
            "total_drive_kwh_per_100km": total_drive_kwh_per_100km,
            "total_charge_gain_percent": total_charge_gain_percent,
            "trips": trips,
        },
    }


# =============================================================================
# Flask routes: main UI
# =============================================================================

@app.route("/")
def index():
    """Serve the main map UI (renders templates/index.html, the Leaflet page)."""
    return render_template("index.html")


# =============================================================================
# Flask routes: health / status / debug
# =============================================================================

@app.route("/health")
def health():
    """Simple health endpoint for Docker, curl, or Home Assistant checks."""
    return jsonify({"status": "ok", "ts": now_utc()})


@app.route("/status")
def status():
    """
    Return the latest known vehicle status in a compact JSON format.
    Calls: latest_point().
    """
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
    """Debug endpoint showing the 50 most recent stored raw points (direct query, bypasses the day/trip helpers)."""
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM trip_points ORDER BY id DESC LIMIT 50")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(rows)


# =============================================================================
# Flask routes: JSON API used by the map UI's JavaScript
# =============================================================================

@app.route("/api/days")
def api_days():
    """
    List days present in the database together with point counts.
    Called by the frontend's loadDays() to populate the day dropdown.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT substr(ts, 1, 10) AS day, COUNT(*) AS points
        FROM trip_points
        GROUP BY substr(ts, 1, 10)
        ORDER BY day DESC
        """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(rows)


@app.route("/api/points")
def api_points():
    """
    Return raw point data for one day.
    If trip_index is supplied, return only the points for that trip.

    Calls: get_points_for_trip() or get_points_for_day(). Called by the
    frontend whenever it needs points to draw on the map (loadMap,
    loadTripsForDay, the /api/trips_range-driven range map).
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
    """
    Return a whole-day summary derived from raw points, with energy figures
    computed from that day's drive trips (not raw start/end battery_percent)
    so mid-day charging can't offset the reported consumption -- see
    summarize_day()'s docstring for why.
    Calls: get_points_for_day(), get_trips_for_day(), summarize_day(). Called
    by the frontend's loadMap() when no specific trip is selected.
    """
    day = request.args.get("day")
    if not day:
        return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400
    points = get_points_for_day(day)
    trips = get_trips_for_day(day)
    return jsonify(summarize_day(points, trips))


@app.route("/api/trips")
def api_trips():
    """
    Return all derived trips for a specific day.
    Calls: get_trips_for_day(). Called by the frontend's loadTripsForDay()
    to populate the trip dropdown and trip meta text.
    """
    day = request.args.get("day")
    if not day:
        return jsonify({"error": "missing day parameter, use YYYY-MM-DD"}), 400
    return jsonify(get_trips_for_day(day))


@app.route("/api/trips_range")
def api_trips_range():
    """
    Return all derived trips for a selected date range.
    Calls: get_trips_for_range(). Called by the frontend's loadRangeBtn
    click handler for the "Load period trips" date-range map feature.
    """
    start_day = request.args.get("start_day")
    end_day = request.args.get("end_day")

    if not start_day or not end_day:
        return jsonify({"error": "missing start_day or end_day, use YYYY-MM-DD"}), 400

    if start_day > end_day:
        return jsonify({"error": "start_day must be before or equal to end_day"}), 400

    return jsonify(get_trips_for_range(start_day, end_day))


# =============================================================================
# Flask routes: Home Assistant integration
# =============================================================================

@app.route("/ha/state")
def ha_state():
    """Home Assistant sensor endpoint for the vehicle's current state. Calls: build_home_assistant_state()."""
    return jsonify(build_home_assistant_state())


@app.route("/ha/day")
def ha_day():
    """Home Assistant sensor endpoint for one day's summary/trips. Calls: iso_day_utc(), build_home_assistant_day()."""
    day = request.args.get("day") or iso_day_utc()
    return jsonify(build_home_assistant_day(day))


@app.route("/ha/trips")
def ha_trips():
    """Home Assistant sensor endpoint listing one day's trips. Calls: iso_day_utc(), get_trips_for_day(), now_utc()."""
    day = request.args.get("day") or iso_day_utc()
    return jsonify(
        {
            "day": day,
            "trips": get_trips_for_day(day),
            "count": len(get_trips_for_day(day)),
            "last_updated": now_utc(),
        }
    )


# =============================================================================
# Flask routes: admin / maintenance
# =============================================================================

@app.route("/admin/backfill_geocodes")
def admin_backfill_geocodes():
    """
    Maintenance endpoint that fills missing location_name values for older points.
    Limited to a small batch to keep each request predictable.

    Calls: reverse_geocode() per point missing a location_name. Intended to
    be hit manually/repeatedly (e.g. via curl or a cron job) until no more
    points are missing names.
    """
    try:
        conn = db()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, lat, lon
            FROM trip_points
            WHERE location_name IS NULL
              AND lat IS NOT NULL
              AND lon IS NOT NULL
            ORDER BY id ASC
            LIMIT 20
            """)
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
    """
    Maintenance endpoint that rebuilds one day's derived trip table.
    Calls: rebuild_trips_for_day(). Useful for forcing a re-segmentation of
    an existing day, e.g. after adjusting segmentation constants.
    """
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


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    # Startup sequence:
    # 1. initialize the SQLite schema,             -> init_db()
    # 2. start the background poller thread,       -> poll_loop() (daemon thread)
    # 3. start Flask so the UI and API routes are reachable. -> app.run()
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
