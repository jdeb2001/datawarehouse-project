# dit is een script om alle data die in de veloDB databank zit over te zetten naar onze eigen data warehouse databank, zodat we hiermee op onze manier aan de slag kunnen
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
# vergeet niet om wachtwoord te veranderen met eigen wachtwoord!
source_db_config = {
    'dbname': 'velodb',
    'user': 'postgres',
    'password': 'Goldyke001',
    'host': 'localhost',
    'port': '5432',
}

target_db_config = {
    'dbname': 'dwh_bike_analytics',
    'user': 'postgres',
    'password': 'Goldyke001',
    'host': 'localhost',
    'port': '5432',
}

def get_first_ride_date(source_cursor, userid):
    """
    Haalt de datum van de eerste rit op voor een gebruiker.
    """
    source_cursor.execute("""
        SELECT MIN(starttime) 
        FROM rides 
        WHERE subscriptionid IN (
            SELECT subscriptionid 
            FROM subscriptions 
            WHERE userid = %s
        )
    """, (userid,))
    result = source_cursor.fetchone()
    return result[0] if result and result[0] else None

def update_dim_client(target_cursor, userid, transformed_record):
    """
    Controleert of een klant al bestaat en implementeert SCD Type 2
    met expliciete versiebeheer (`scd_version`).
    """
    # Controleer bestaande actieve records
    target_cursor.execute("""
        SELECT email, subscriptionType, scd_version
        FROM dim_clients
        WHERE clientID = %s AND isActive = TRUE
    """, (userid,))
    existing_record = target_cursor.fetchone()

    if not existing_record:
        return "insert"

    # Vergelijk velden om wijzigingen te detecteren
    email_latest, subscription_latest, scd_version_latest = existing_record
    email, subscription_type = transformed_record[2], transformed_record[9]

    if email != email_latest or subscription_type != subscription_latest:
        # Sluit het huidige record
        target_cursor.execute("""
            UPDATE dim_clients
            SET scd_end = CURRENT_DATE, isActive = FALSE
            WHERE clientID = %s AND isActive = TRUE
        """, (userid,))
        return scd_version_latest + 1  # Nieuwe versie

    return "no_change"

def transfer_users_to_dim_client(source_conn, target_conn, batch_size=1000):
    """Verwerkt gebruikersgegevens naar de `dim_clients` tabel met SCD Type 2."""
    try:
        # Extract
        print("Starting data extraction...")
        source_cursor = source_conn.cursor()
        source_cursor.execute("""
            SELECT 
                u.userid, u.name, u.email, u.street, u.number, u.city, u.zipcode, u.country_code, 
                s.subscriptiontypeid, MIN(s.validfrom)
            FROM 
                velo_users u
            LEFT JOIN 
                subscriptions s ON u.userid = s.userid
            GROUP BY 
                u.userid, u.name, u.email, u.street, u.number, u.zipcode, u.city, u.country_code, s.subscriptiontypeid
        """)
        users_data = source_cursor.fetchall()
        print(f"Extracted {len(users_data)} records.")

        # Transform
        print("Starting data transformation...")
        transformed_data = []
        for user in users_data:
            userid, name, email, street, number, city, postal_code, country_code, subscription_type, valid_from = user

            # Haal de datum van de eerste rit op
            first_ride_date = get_first_ride_date(source_cursor, userid)
            scd_start = first_ride_date or valid_from or datetime.now().strftime("%Y-%m-%d")
            scd_end = None
            scd_version = 1
            is_active = True

            transformed_record = (
                userid, name, email, street, number, city, postal_code, country_code,
                subscription_type, scd_start, scd_end, scd_version, is_active
            )
            transformed_data.append(transformed_record)
        print(f"Transformed {len(transformed_data)} records.")

        # Load
        print("Starting data loading...")
        target_cursor = target_conn.cursor()

        for record in transformed_data:
            userid = record[0]
            action = update_dim_client(target_cursor, userid, record)

            if action == "insert":
                insert_query = """
                    INSERT INTO dim_clients (
                        clientID, name, email, street, number, city, postal_code, country_code, subscriptionType,
                        scd_start, scd_end, scd_version, isActive
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_query, record)
            elif isinstance(action, int):  # Nieuwe versie
                scd_version = action
                record = record[:-2] + (scd_version, True)  # Update versie en status
                insert_query = """
                    INSERT INTO dim_clients (
                        clientID, name, email, street, number, city, postal_code, country_code, subscriptionType,
                        scd_start, scd_end, scd_version, isActive
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_query, record)

        # Commit wijzigingen
        target_conn.commit()
        print("Successfully updated dim_clients with SCD Type 2.")
    except Exception as e:
        print(f"Error during data transfer: {e}")
        target_conn.rollback()

def connect_to_db(config):
    try:
        conn = psycopg2.connect(**config)
        print("Connection to database established")
        return conn
    except Exception as e:
        print(f"Error establishing connection to database: {e}")

def main():
    source_conn = connect_to_db(source_db_config)
    target_conn = connect_to_db(target_db_config)

    if not source_conn or not target_conn:
        return

    try:
        transfer_users_to_dim_client(source_conn, target_conn)
    finally:
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()
        print("Closed connections")

if __name__ == "__main__":
    main()
