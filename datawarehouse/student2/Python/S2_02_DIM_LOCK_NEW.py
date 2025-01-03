import psycopg2
from psycopg2.extras import execute_values
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

def fetch_all_locks_data(cur_op):
    """
    Alle slotgegevens en gerelateeerde stationsinformatie wordt hier opgehaald.
    """
    query = """
    SELECT 
            l.lockid,
            l.stationlocknr,
            s.street || ' ' || COALESCE(s.number, '') AS station_address,
            s.zipcode,
            s.district,
            s.gpscoord,
            s.type AS station_type
        FROM locks l
        JOIN stations s ON l.stationid = s.stationid
    """
    cur_op.execute(query)
    return cur_op.fetchall()

def fetch_existing_locks(cur_dwh):
    """
    Hier worden alle bestaande lock-records uit de DWH opgehaald.
    """
    query = """
    SELECT lockID, stationLockNr, stationAddress, stationZipCode, stationDistrict, stationCoordinations, stationType
        FROM dim_locks
        """

    cur_dwh.execute(query)
    return {row[0]: row for row in cur_dwh.fetchall()}

def transform_locks_data(locks_data, existing_locks):
    """
    Opgehaalde data wordt hier getransformeerd en gecontroleerd op wijzigingen.
    """
    transformed_data = []
    unique_lock_ids = set(existing_locks.keys())

    for lock in locks_data:
        lockid, stationlocknr, station_address, zipcode, district, gpscoord, station_type = lock

        station_address = station_address.strip().title() if station_address else "Onbekend"
        zipcode = zipcode.strip() if zipcode else "0000"
        district = district.strip().title() if district else "Onbekend"
        station_type = station_type.strip().title() if station_type else "Onbekend"
        gpscoord = gpscoord if gpscoord else "(0,0)"

        if lockid in unique_lock_ids:
            continue

        transformed_data.append((
            lockid, stationlocknr, station_address, zipcode, district, gpscoord, station_type
        ))
        unique_lock_ids.add(lockid)

    # Voeg extra record toe voor "Geen slot" buiten de lus
    if None not in unique_lock_ids:
        transformed_data.append((None, None, "Geen locatie", "0000", "Onbekend", "(0,0)", "Geen slot"))

    return transformed_data


def load_locks_data(cur_dwh, db_dwh, transformed_data, batch_size=1000):
    """
    Hier gaan we batchinserts doen om de lock-gegevens in de databank op te laden (loading).
    """
    insert_query = """
    INSERT INTO dim_locks (lockID, stationLockNr, stationAddress, stationZipCode, stationDistrict, stationCoordinations, stationType)
    VALUES %s"""

    for i in range (0, len(transformed_data), batch_size):
        batch = transformed_data[i:i + batch_size]
        execute_values(cur_dwh, insert_query, batch)
        print(f"Loadded batch {i // batch_size + 1} with {len(batch)} records.")

    db_dwh.commit()
    print("Locks data successfully loaded into dim_locks.")

def process_locks(cur_op, cur_dwh, db_dwh):
    """
    Hier zetten we al de verschillende stappen die gebeuren op een rij.
    """
    # Extracting
    print("Fetching lock data...")
    locks_data = fetch_all_locks_data(cur_op)
    print(f"Extracted {len(locks_data)} locks.")

    print("Fetching existing locks...")
    existing_locks = fetch_existing_locks(cur_dwh)

    # Transforming
    print ("Transforming locks data...")
    transformed_data = transform_locks_data(locks_data, existing_locks)
    print(f"Transformed {len(transformed_data)} lock records.")

    # Loading
    print("Loading locks data into DWH...")
    load_locks_data(cur_dwh, db_dwh, transformed_data)

def main():
    db_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, PORT)
    db_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
    cur_op = db_op.cursor()
    cur_dwh = db_dwh.cursor()

    try:
        test_connections(cur_op, cur_dwh)
        process_locks(cur_op, cur_dwh, db_dwh)
    except Exception as e:
        print(f"Error during processing occurred: {e}")
        db_dwh.rollback()
    finally:
        cur_op.close()
        cur_dwh.close()
        db_op.close()
        db_dwh.close()
        print("Connections to databases closed.")

if __name__ == "__main__":
    main()