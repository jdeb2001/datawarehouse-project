import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

def test_connections(cur_op, cur_dwh):
    """
    Test de verbindingen naar de operationele databank en het datawarehouse.
    """
    try:
        cur_op.execute("SELECT version();")
        print("Connected to operational database: ", cur_op.fetchone())

        cur_dwh.execute("SELECT version();")
        print("Connected to data warehouse: ", cur_dwh.fetchone())
    except Exception as e:
        print(f"Error establishing connection to databases: {e}")
        raise

def fetch_step_lock_sk(cur_dwh):
    """
    Haal de lock_sk op voor het record dat 'Geen slot' vertegenwoordigt.
    """
    query = """
        SELECT lock_sk
        FROM dim_locks
        WHERE lockID IS NULL
    """
    cur_dwh.execute(query)
    result = cur_dwh.fetchone()
    if result:
        return result[0]
    else:
        raise ValueError("Geen 'Geen slot' record gevonden in dim_locks.")

def fetch_rides_data(cur_op):
    """
    Haal alle ritgegevens op uit de operationele databank.
    """
    query = """
        SELECT
            r.rideid,
            r.starttime,
            r.endtime,
            s.userid,
            r.startlockid,
            r.endlockid
        FROM rides r 
        JOIN subscriptions s ON r.subscriptionid = s.subscriptionid
    """
    cur_op.execute(query)
    return cur_op.fetchall()

def process_fact_rides(cur_op, cur_dwh, db_dwh, step_lock_sk, batch_size=1000):
    """
    Verwerk de ritgegevens en laad ze in de fact_rides tabel.
    """
    print("Fetching rides data...")
    rides_data = fetch_rides_data(cur_op)
    if not rides_data:
        print("No rides data found. Exiting.")
        return

    print(f"Fetched {len(rides_data)} rides records.")
    fact_rides = []

    for ride in rides_data:
        rideid, starttime, endtime, userid, startlockid, endlockid = ride

        # Bereken de duur van de rit
        duration = endtime - starttime

        # Haal DIM_DATE (date_sk) op
        cur_dwh.execute("""
            SELECT date_sk
            FROM dim_date
            WHERE date = %s
        """, (starttime.date(),))
        date_sk_result = cur_dwh.fetchone()
        if not date_sk_result:
            print(f"Date {starttime.date()} not found in dim_date. Skipping ride {rideid}.")
            continue
        date_sk = date_sk_result[0]

        # Haal DIM_WEATHER (weather_sk) op
        cur_dwh.execute("""
            SELECT weather_sk
            FROM dim_weather
            WHERE date = %s
            AND zipCode = (SELECT stationZipCode FROM dim_locks WHERE lockID = %s LIMIT 1)
            AND hour = EXTRACT(HOUR FROM %s)
        """, (starttime.date(), startlockid, starttime))
        weather_sk_result = cur_dwh.fetchone()
        weather_sk = weather_sk_result[0] if weather_sk_result else None

        # Haal DIM_CLIENTS (client_sk) op
        cur_dwh.execute("""
            SELECT client_sk
            FROM dim_clients
            WHERE clientID = %s
            AND %s BETWEEN scd_start AND COALESCE(scd_end, '9999-12-31')
        """, (userid, starttime))
        client_sk_result = cur_dwh.fetchone()
        if not client_sk_result:
            print(f"Client {userid} not found in dim_clients. Skipping ride {rideid}.")
            continue
        client_sk = client_sk_result[0]

        # Haal DIM_LOCKS (start_lock_sk, end_lock_sk) op
        cur_dwh.execute("""
            SELECT lock_sk
            FROM dim_locks
            WHERE lockID = %s
        """, (startlockid,))
        start_lock_result = cur_dwh.fetchone()
        start_lock_sk = start_lock_result[0] if start_lock_result else step_lock_sk

        cur_dwh.execute("""
            SELECT lock_sk
            FROM dim_locks
            WHERE lockID = %s
        """, (endlockid,))
        end_lock_result = cur_dwh.fetchone()
        end_lock_sk = end_lock_result[0] if end_lock_result else step_lock_sk

        # Voeg het record toe aan de fact_rides lijst
        fact_rides.append((
            rideid, date_sk, weather_sk, client_sk, start_lock_sk, end_lock_sk, duration, None
        ))

    print(f"Processing {len(fact_rides)} records into fact_rides...")

    # Batch-inserts
    insert_query = """
        INSERT INTO fact_rides (
            ride_sk, date_sk, weather_sk, client_sk, start_lock_sk, end_lock_sk, duration, distance
        ) VALUES %s
    """
    for i in range(0, len(fact_rides), batch_size):
        batch = fact_rides[i:i + batch_size]
        try:
            execute_values(cur_dwh, insert_query, batch)
            print(f"Inserted batch {i // batch_size + 1} with {len(batch)} records.")
        except Exception as e:
            print(f"Error inserting batch {i // batch_size + 1}: {e}")
            db_dwh.rollback()
            continue

    db_dwh.commit()
    print("Fact rides data successfully loaded.")

def main():
    db_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, PORT)
    db_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
    cur_op = db_op.cursor()
    cur_dwh = db_dwh.cursor()

    try:
        test_connections(cur_op, cur_dwh)

        # Haal de "Geen slot" lock_sk op
        step_lock_sk = fetch_step_lock_sk(cur_dwh)

        # Verwerk de ritgegevens
        process_fact_rides(cur_op, cur_dwh, db_dwh, step_lock_sk)
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
