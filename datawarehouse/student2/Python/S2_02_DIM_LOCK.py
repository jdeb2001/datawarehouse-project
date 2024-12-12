import psycopg2
# vergeet niet om wachtwoord te veranderen met eigen wachtwoord!
source_db_config = {
    'dbname': 'velodb',
    'user': 'postgres',
    'password': '<PASSWORD>',
    'host': 'localhost',
    'port': '5432',
}

target_db_config = {
    'dbname': 'dwh_bike_analytics',
    'user': 'postgres',
    'password': '<PASSWORD>',
    'host': 'localhost',
    'port': '5432',
}

def connect_to_db(config):
    try:
        conn = psycopg2.connect(**config)
        print("Connection to database establishes")
        return conn
    except Exception as e:
        print(f"Unable to connect to database: {e}")

def transfer_locks_to_dim_lock(source_conn, target_conn):
    try:
        source_cursor = source_conn.cursor()
        source_query = """
            SELECT 
                l.lockid,
                l.stationlocknr,
                s.street || ' ' || COALESCE(s.number, '') AS station_address,
                s.zipcode,
                s.district,
                s.gpscoord,
                s.type AS station_type
            FROM locks l
            JOIN stations s ON l.stationid = s.stationid;
        """
        source_cursor.execute(source_query)
        locks_data = source_cursor.fetchall()

        target_cursor = target_conn.cursor()

        insert_query = """
            INSERT INTO dim_locks (
                lockID, stationLockNr, stationAddress, zipCode, district, GPSCoord, stationType, isStep
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for lock in locks_data:
            target_cursor.execute(insert_query, (
                lock[0],
                lock[1],
                lock[2],
                lock[3],
                lock[4],
                lock[5],
                lock[6],
                False
            ))


        # extra record voor geen slot
        target_cursor.execute(insert_query, (
            None,
            None,
            'Geen locatie',
            None,
            None,
            None,
            None,
            True # true voor ritten zonder slot
        ))

        # wijzigingen opslaan
        target_conn.commit()
        print("Transfered data successfully to dim_lock")
    except Exception as e:
        print(f"Unable to transfer data to dim_lock: {e}")
        target_conn.rollback()


def main():
    source_conn = connect_to_db(source_db_config)
    target_conn = connect_to_db(target_db_config)

    if not source_conn or not target_conn:
        return

    try:
        transfer_locks_to_dim_lock(source_conn, target_conn)
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()
        print("Closed connections.")

main()