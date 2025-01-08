from datetime import datetime
from psycopg2.extras import execute_values
import datawarehouse.student1.python.dwh_tools as dwh
from datawarehouse.student1.python.config import SERVER, DATABASE_OP, DATABASE_DWH, USERNAME, PASSWORD, PORT

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
    """Haal alle gebruikers- en abonnementsdata op."""
    query = """
        SELECT vu.userid, vu.name, 
               concat(vu.street, ' ', vu.number, '\n', vu.zipcode, ' ', vu.city) AS address,
               vu.country_code, s.subscriptiontypeid, MIN(s.validfrom) AS validfrom
        FROM velo_users vu
        JOIN subscriptions s ON vu.userid = s.userid
        GROUP BY vu.userid, vu.name, vu.street, vu.number, vu.zipcode, vu.city, vu.country_code, s.subscriptiontypeid
    """
    cur_op.execute(query)
    return cur_op.fetchall()

def fetch_first_ride_dates(cur_op):
    """Haal de eerste ritdatums van alle gebruikers in bulk op."""
    query = """
        SELECT s.userid, MIN(r.starttime) AS first_ride_date
        FROM rides r
        JOIN subscriptions s ON r.subscriptionid = s.subscriptionid
        GROUP BY s.userid
    """
    cur_op.execute(query)
    return {row[0]: row[1] for row in cur_op.fetchall()}

def fetch_existing_clients(cur_dwh):
    """Haal alle bestaande actieve klanten uit de DWH."""
    query = """
        SELECT clientID, address, country_code, scd_version 
        FROM dim_clients
        WHERE scd_active = TRUE
    """
    cur_dwh.execute(query)
    return {row[0]: (row[1], row[2], row[3]) for row in cur_dwh.fetchall()}

def process_clients(cur_op, cur_dwh, db_dwh):
    """Verwerk alle gebruikersdata en voer batchinserts/updates uit."""
    print("Fetching client and ride data...")
    client_data = fetch_all_client_data(cur_op)
    first_ride_dates = fetch_first_ride_dates(cur_op)
    existing_clients = fetch_existing_clients(cur_dwh)

    print("Processing client data...")
    new_records = []
    updates = []

    for userid, name, address, country_code, subscriptiontypeid, validfrom in client_data:
        first_ride_date = first_ride_dates.get(userid, validfrom) or validfrom
        scd_start = first_ride_date.strftime('%Y-%m-%d')
        scd_end = None
        scd_active = True

        if userid in existing_clients:
            existing_address, existing_country_code, scd_version = existing_clients[userid]
            if address != existing_address or country_code != existing_country_code:
                # Update bestaande klant (SCD2)
                update_query = """
                    UPDATE dim_clients
                    SET scd_end = %s, scd_active = FALSE
                    WHERE clientID = %s AND scd_active = TRUE
                """
                cur_dwh.execute(update_query, (datetime.now(), userid))
                updates.append(userid)  # Logging of monitoring purposes

                # Voeg nieuwe record toe
                new_records.append((userid, name, address, country_code, subscriptiontypeid, scd_start, '2040-01-01', scd_version + 1, scd_active, validfrom))
        else:
            # Nieuwe klant toevoegen
            new_records.append((userid, name, address, country_code, subscriptiontypeid, scd_start, '2040-01-01', 1, scd_active, validfrom))

    print(f"Updated {len(updates)} existing records.")

    # Batchinserts uitvoeren
    if new_records:
        print(f"Inserting {len(new_records)} new records...")
        insert_query = """
            INSERT INTO dim_clients (clientID, name, address, country_code, subscription_type, scd_start, scd_end, scd_version, scd_active, last_ride_date)
            VALUES %s
        """
        execute_values(cur_dwh, insert_query, new_records)

    # Commit wijzigingen
    db_dwh.commit()
    print("Client data successfully processed.")

def main():
    db_op = dwh.establish_connection(SERVER, DATABASE_OP, USERNAME, PASSWORD, PORT)
    db_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
    cur_op = db_op.cursor()
    cur_dwh = db_dwh.cursor()

    try:
        process_clients(cur_op, cur_dwh, db_dwh)
    except Exception as e:
        print(f"Error during processing: {e}")
        db_dwh.rollback()
    finally:
        cur_op.close()
        cur_dwh.close()
        db_op.close()
        db_dwh.close()
        print("Connections closed.")

if __name__ == "__main__":
    main()