from datetime import datetime
import psycopg2
import dwh_tools as dwh
from config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

# Test de verbindingen naar de database
def test_connections(cur_op, cur_dwh):
    try:
        cur_op.execute("SELECT version();")
        print("Connected to operational database. ", cur_op.fetchone())

        cur_dwh.execute("SELECT version();")
        print("Connected to data warehouse. ", cur_dwh.fetchone())
    except Exception as e:
        print(f"Error establishing connection to database: {e}")
        raise

def fetch_all_client_data(cur_op):
    query = """
            SELECT vu.userid, vu.name, concat(vu.street, ' ', vu.number, '\n', vu.zipcode, ' ', vu.city, '\n', vu.country_code) AS address,
            s.subscriptiontypeid, s.validfrom
            FROM velo_users vu
            JOIN subscriptions s ON vu.userid = s.userid
            """

    cur_op.execute(query)
    return cur_op.fetchall()

def fetch_first_ride_date(cur_op, userid, validfrom):
    cur_op.execute("""
        SELECT MIN(starttime)
        FROM rides
        WHERE subscriptionid IN (
            SELECT subscriptionid
            FROM subscriptions
            WHERE userid = %s)""", (userid,))

    result = cur_op.fetchone()
    print(f"First ride date result: {result}")
    return result[0] if result and result[0] else validfrom

# functie om huidige gegevens over de klant uit het DWH te halen
def fetch_dwh_customer_data(cur_dwh, userid):
    cur_dwh.execute("""
    SELECT address, scd_version
    FROM dim_clients
    WHERE clientID = %s AND isActive""", (userid,))
    rows = cur_dwh.fetchall()
    print(f"DWH customer data result: {rows}")
    return rows if rows else []

def insert_new_dwh_client(cur_dwh, userid, name, address, subscriptiontypeid, first_ride_date, validfrom):
    query = """
    INSERT INTO dim_clients (clientID, name, address, subscriptionType, scd_start, scd_end, scd_version, isActive, last_ride_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cur_dwh.execute(query, (userid, name, address, subscriptiontypeid, first_ride_date, '2040-01-01', 1, True, validfrom))

def update_and_insert_dwh_client(cur_dwh, userid, name, address, subscriptiontypeid, first_ride_date, validfrom, address_latest, scd_version_latest):
    query = """
    UPDATE dim_clients
    SET scd_end = %s, isActive = False
    WHERE clientID = %s AND isActive = True"""
    cur_dwh.execute(query, (datetime.now(), userid))

    insert = """
    INSERT INTO dim_clients(clientID, name, address, subscriptionType, scd_start, scd_end, scd_version, isActive, last_ride_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cur_dwh.execute(insert, (userid, name, address, subscriptiontypeid, first_ride_date, '2040-01-01', scd_version_latest + 1, True, validfrom))

def process_client_data(cur_op, cur_dwh, db_dwh, results):
    for userid, name, address, subscriptiontypeid, validfrom in results:
        first_ride_date = fetch_first_ride_date(cur_op, userid, validfrom)
        rows = fetch_dwh_customer_data(cur_dwh, userid)

        if not rows:
            insert_new_dwh_client(cur_dwh, userid, name, address, subscriptiontypeid, first_ride_date, validfrom)
        else:
            address_latest, scd_version_latest = rows[0]
            if address != address_latest:
                update_and_insert_dwh_client(cur_dwh, userid, name, address, subscriptiontypeid, first_ride_date,
                                             validfrom, address_latest, scd_version_latest)
        db_dwh.commit()

def close_connection(cur_op, cur_dwh, db_op, db_dwh):
    cur_op.close()
    cur_dwh.close()
    db_dwh.close()
    db_op.close()

def main():
    # Eerst proberen we een verbinding te maken met onze 2 databanken:
    db_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, PORT)
    db_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
    cur_op = db_op.cursor()
    cur_dwh = db_dwh.cursor()

    try:
        test_connections(cur_op, cur_dwh)
        results = fetch_all_client_data(cur_op)
        process_client_data(cur_op, cur_dwh, db_dwh, results)
        print("Processed client data successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        close_connection(cur_op, cur_dwh, db_op, db_dwh)

if __name__ == "__main__":
    main()

