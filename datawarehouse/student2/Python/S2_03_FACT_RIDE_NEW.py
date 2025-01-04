import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

def test_connections(cur_op, cur_dwh):
    try:
        cur_op.execute("SELECT version();")
        print("Connected to operational database. ", cur_op.fetchone())

        cur_dwh.execute("SELECT version();")
        print("Connected to data warehouse. ", cur_dwh.fetchone())
    except Exception as e:
        print(f"Error establishing connection to database: {e}")
        raise

def fetch_rides_data(cur_op):
    print("Fetching rides data from operational database...")
    query = """
    SELECT
        r.rideid,
        r.starttime,
        r.endtime,
        r.startlockid,
        r.endlockid,
        s.userid
    FROM rides r
    JOIN subscriptions s ON r.subscriptionid = s.subscriptionid
    WHERE r.starttime IS NOT NULL AND r.endtime IS NOT NULL
    AND r.starttime BETWEEN '2019-09-22' AND '2019-09-24'
    """
    cur_op.execute(query)
    rides = cur_op.fetchall()
    print(f"Fetched {len(rides)} rides records.")
    return rides

def fetch_date_sk(cur_dwh, ride_start_date):
    print(f"Fetching date_SK for ride start date {ride_start_date}...")
    query = """
    SELECT date_sk FROM dim_date WHERE date = %s
    """
    cur_dwh.execute(query, (ride_start_date,))
    result = cur_dwh.fetchone()
    if result:
        return result[0]
    else:
        print(f"date_SK not found for date {ride_start_date}. Skipping record.")
        return None

def fetch_client_sk(cur_dwh, userid, ride_start_time):
    print(f"Fetching client_SK for user ID {userid} at time {ride_start_time}...")
    query = """
    SELECT client_sk FROM dim_clients
    WHERE clientid = %s
      AND %s BETWEEN scd_start AND COALESCE(scd_end, '9999-12-31')
    """
    cur_dwh.execute(query, (userid, ride_start_time))
    result = cur_dwh.fetchone()
    if result:
        return result[0]
    else:
        print(f"client_SK not found for user ID {userid}. Skipping record.")
        return None

def fetch_lock_sk(cur_dwh, lockid):
    print(f"Fetching lock_SK for lock ID {lockid}...")
    query = """
    SELECT lock_sk FROM dim_locks WHERE lockid = %s
    """
    cur_dwh.execute(query, (lockid,))
    result = cur_dwh.fetchone()
    if result:
        return result[0]
    else:
        print(f"lock_SK not found for lock ID {lockid}. Defaulting to 'Geen slot'.")
        return None

def process_fact_rides(cur_op, cur_dwh, db_dwh, rides_data, batch_size=1000):
    print("Processing rides data...")
    fact_rides = []

    for ride in rides_data:
        rideid, starttime, endtime, startlockid, endlockid, userid = ride

        ride_start_date = starttime.date()
        date_sk = fetch_date_sk(cur_dwh, ride_start_date)
        if not date_sk:
            continue

        client_sk = fetch_client_sk(cur_dwh, userid, starttime)
        if not client_sk:
            continue

        start_lock_sk = fetch_lock_sk(cur_dwh, startlockid)
        if start_lock_sk is None:
            continue

        end_lock_sk = fetch_lock_sk(cur_dwh, endlockid)
        if end_lock_sk is None:
            continue

        duration = endtime - starttime
        distance = None  # Optional, can be calculated if data is available

        fact_rides.append((rideid, date_sk, client_sk, start_lock_sk, end_lock_sk, duration, distance))

    print(f"Transformed {len(fact_rides)} records for fact_rides.")

    if fact_rides:
        print("Loading fact_rides into data warehouse...")
        insert_query = """
        INSERT INTO fact_rides (
            ride_sk, date_sk, client_sk, start_lock_sk, end_lock_sk, duration, distance
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
        print("Fetching rides data from operational database...")
        rides_data = fetch_rides_data(cur_op)
        print("Processing and loading fact rides...")
        process_fact_rides(cur_op, cur_dwh, db_dwh, rides_data)
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
