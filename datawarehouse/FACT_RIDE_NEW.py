import json
import os
import math
from psycopg2.extras import execute_values
from datetime import datetime
import datawarehouse.student2.python.dwh_tools as dwh
from datawarehouse.student2.python.config.config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

def test_connections(cur_op, cur_dwh):
    try:
        cur_op.execute("SELECT version();")
        print("Connected to operational database: ", cur_op.fetchone())
        cur_dwh.execute("SELECT version();")
        print("Connected to data warehouse: ", cur_dwh.fetchone())
    except Exception as e:
        print(f"Error establishing connection to database: {e}")
        raise

def fetch_cached_data(cur_dwh):
    """Cache dimensiedata voor betere prestaties."""
    print("Caching dimension data for performance optimization...")

    # Cache dim_date
    cur_dwh.execute("SELECT date, date_sk FROM dim_date")
    date_cache = {row[0]: row[1] for row in cur_dwh.fetchall()}

    # Cache dim_weather
    cur_dwh.execute("SELECT LOWER(weather_type), weather_sk FROM dim_weather")
    weather_sk_cache = {row[0]: row[1] for row in cur_dwh.fetchall()}

    # Cache dim_clients
    cur_dwh.execute("""
        SELECT clientid, scd_start::date, COALESCE(scd_end, '9999-12-31')::date, client_sk 
        FROM dim_clients
    """)
    client_cache = {}
    for row in cur_dwh.fetchall():
        clientid, scd_start, scd_end, client_sk = row
        if clientid not in client_cache:
            client_cache[clientid] = []
        client_cache[clientid].append((scd_start, scd_end, client_sk))

    # Cache dim_locks
    cur_dwh.execute("SELECT lockid, lock_sk FROM dim_locks")
    lock_cache = {row[0]: row[1] for row in cur_dwh.fetchall()}

    # Cache lock coordinates
    cur_dwh.execute("SELECT lockid, station_coordinates FROM dim_locks")
    lock_coords = {
        row[0]: tuple(map(float, row[1][1:-1].split(','))) for row in cur_dwh.fetchall() if row[1]
    }

    print("Caching completed.")
    return date_cache, weather_sk_cache, client_cache, lock_cache, lock_coords

def assess_weather_type(starttime, zipcode):
    """Bepaal het weerstype op basis van JSON-bestanden."""
    file_path = f"./weather/{zipcode}_{starttime.date()}_{starttime.hour:02d}h.json"
    if os.path.isfile(file_path):
        with open(file_path, "r") as json_file:
            hourly_report = json.load(json_file)
            sky_clarity = hourly_report["weather"][0]["main"].lower()
            temperature_celsius = hourly_report["main"]["temp"] - 273.15
            if "rain" in sky_clarity or sky_clarity == "snow":
                return "onaangenaam"
            if sky_clarity == "clear" and temperature_celsius > 15:
                return "aangenaam"
            return "neutraal"
    return "weertype onbekend"

def haversine(coord1, coord2):
    """Bereken de afstand tussen 2 punten aan de hand van de Haversine-formule."""
    R = 6371  # Earth radius in km
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def fetch_existing_ride_sks(cur_dwh):
    """Haal bestaande ride_sk waarden op uit fact_rides."""
    print("Fetching existing ride_sk values from fact_rides...")
    cur_dwh.execute("SELECT ride_sk FROM fact_rides")
    existing_ride_sks = {row[0] for row in cur_dwh.fetchall()}
    print(f"Fetched {len(existing_ride_sks)} existing ride_sk values.")
    return existing_ride_sks

def process_fact_rides(cur_op, cur_dwh, db_dwh, batch_size=1000):
    """Verwerk rides data en laad alleen nieuwe records in fact_rides."""
    print("Fetching rides data from operational database...")
    cur_op.execute("""
        SELECT
        r.rideid,
        r.starttime,
        r.endtime,
        r.startlockid,
        r.endlockid,
        st.zipcode,
        sub.userid
    FROM rides r
    JOIN locks l ON r.startlockid = l.lockid
    JOIN stations st ON l.stationid = st.stationid
    JOIN subscriptions sub ON r.subscriptionid = sub.subscriptionid
    WHERE r.starttime IS NOT NULL AND r.endtime IS NOT NULL
    """)
    rides_data = cur_op.fetchall()
    print(f"Fetched {len(rides_data)} rides.")

    print("Caching dimension data...")
    date_cache, weather_sk_cache, client_cache, lock_cache, lock_coords = fetch_cached_data(cur_dwh)

    print("Fetching existing ride_sk values...")
    existing_ride_sks = fetch_existing_ride_sks(cur_dwh)

    fact_rides = []
    for ride in rides_data:
        rideid, starttime, endtime, startlockid, endlockid, zipcode, userid = ride

        if rideid in existing_ride_sks:
            print(f"Skipping ride ID {rideid}: already exists in fact_rides.")
            continue

        if starttime > endtime:
            print(f"Skipping ride ID {rideid}: starttime ({starttime}) is later than endtime ({endtime})")
            continue

        # Fetch date_sk
        ride_date = starttime.date()
        date_sk = date_cache.get(ride_date)
        if not date_sk:
            print(f"date_sk not found for date {ride_date}. Skipping record.")
            continue

        # Fetch weather_sk
        weather_type = assess_weather_type(starttime, zipcode)
        weather_sk = weather_sk_cache.get(weather_type)
        if not weather_sk:
            print(f"weather_sk not found for weather type {weather_type}. Skipping ride ID {rideid}.")
            continue

        # Fetch client_sk
        client_sk = None
        if userid in client_cache:
            for scd_start, scd_end, sk in client_cache[userid]:
                if scd_start <= starttime.date() <= scd_end:
                    client_sk = sk
                    break
        if not client_sk:
            print(f"client_sk not found for user ID {userid}. Skipping record.")
            continue

        # Fetch lock_sk
        start_lock_sk = lock_cache.get(startlockid)
        end_lock_sk = lock_cache.get(endlockid)
        if not start_lock_sk or not end_lock_sk:
            print(f"lock_sk not found for startlockid {startlockid} or endlockid {endlockid}. Skipping record.")
            continue

        # Calculate duration and distance
        duration = endtime - starttime
        start_coords = lock_coords.get(startlockid)
        end_coords = lock_coords.get(endlockid)
        distance = haversine(start_coords, end_coords) if start_coords and end_coords else None

        fact_rides.append((rideid, date_sk, weather_sk, client_sk, start_lock_sk, end_lock_sk, duration, distance))

    print(f"Prepared {len(fact_rides)} new fact rides records for insertion.")

    # Batch insert
    if fact_rides:
        print("Inserting records into fact_rides table...")
        insert_query = """
            INSERT INTO fact_rides (
                ride_sk, date_sk, weather_sk, client_sk, start_lock_sk, end_lock_sk, duration, distance
            ) VALUES %s
        """
        for i in range(0, len(fact_rides), batch_size):
            batch = fact_rides[i:i + batch_size]
            execute_values(cur_dwh, insert_query, batch)
            print(f"Inserted batch {i // batch_size + 1} with {len(batch)} records.")

        db_dwh.commit()
        print("Fact rides data successfully loaded.")
    else:
        print("No fact rides records to load.")

def main():
    db_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, PORT)
    db_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
    cur_op = db_op.cursor()
    cur_dwh = db_dwh.cursor()

    try:
        test_connections(cur_op, cur_dwh)
        process_fact_rides(cur_op, cur_dwh, db_dwh)
    except Exception as e:
        print(f"Error during processing: {e}")
        db_dwh.rollback()
    finally:
        cur_op.close()
        cur_dwh.close()
        db_op.close()
        db_dwh.close()
        print("Connections to databases closed.")

if __name__ == "__main__":
    main()
