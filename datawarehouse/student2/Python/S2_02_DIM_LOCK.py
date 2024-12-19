import psycopg2
from psycopg2.extras import execute_values
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

def transfer_locks_to_dim_lock(source_conn, target_conn, batch_size=1000):
    try:
        # === EXTRACT ===
        print("Starting data extraction...")
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

        # === TRANSFORM ===
        print("Starting data transformation...")
        transformed_data = []
        unique_lock_ids = set()

        for lock in locks_data:
            lockid, stationlocknr, station_address, zipcode, district, gpscoord, station_type = lock

            # Standaardiseer waarden
            station_address = station_address.strip().title() if station_address else "Onbekend"
            zipcode = zipcode.strip() if zipcode else "0000"
            district = district.strip().title() if district else "Onbekend"
            station_type = station_type.strip().title() if station_type else "Onbekend"

            # Vervang missende waarden
            gpscoord = gpscoord if gpscoord else '(0,0)'

            # Controleer op duplicaten
            if lockid in unique_lock_ids:
                continue
            unique_lock_ids.add(lockid)

            # Voeg getransformeerde data toe
            transformed_data.append((
                lockid, stationlocknr, station_address, zipcode, district, gpscoord, station_type
            ))

        # Voeg extra record toe voor "Geen slot"
        # geen idee of dit eigenlijk nog steeds moet of niet?
        transformed_data.append((
            None, None, "Geen locatie", "0000", "Onbekend", "(0,0)", "Geen Slot"
        ))

        print(f"Transformed {len(transformed_data)} records.")

        # === LOADING ===
        # hier en in andere scripts moeten we gebruik maken van batch-inserts om de performantie van data inladen te verhogen
        # dit is zeker nodig als we met gigantische hoeveelheden data gaan werken, wat zeker het geval gaat zijn bij het feit en de dimensie client ook
        target_cursor = target_conn.cursor()

        print("Start data-loading met batch-inserts...")
        target_cursor = target_conn.cursor()

        insert_query = """
                    INSERT INTO dim_locks (
                        lockID, stationLockNr, stationAddress, stationZipCode, stationDistrict, stationCoordinations, stationType
                    ) VALUES %s
                """

        # Batch-inserts uitvoeren
        for i in range(0, len(transformed_data), batch_size):
            batch = transformed_data[i:i + batch_size]
            execute_values(target_cursor, insert_query, batch)
            print(f"Loading batch {i // batch_size + 1}...")

        # Wijzigingen opslaan
        target_conn.commit()
        print("Data succesvol geladen in dim_locks.")


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