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

def transfer_users_to_dim_client(source_conn, target_conn, batch_size=1000):
    try:
        # EXTRACTIE VAN DATA
        print("Starting data extraction...")
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT userid, name, email, street, number, city, zipcode, country_code FROM velo_users")
        users_data = source_cursor.fetchall()

        # TRANSFORMATIE VAN DATA
        # checken of dim_client al bestaat etc
        print("Starting data transformation...")
        transformed_data = []
        for user in users_data:
            userid, name, email, street, number, city, postal_code, country_code = user

            name = name.strip().title() if name else "Unknown"
            email = email.lower() if email else "unknown@example.com"
            street = street.strip().title() if street else "Unknown"
            number = number.strip() if number else "Unknown"
            city = city.strip().title() if city else "Unknown"
            postal_code = postal_code.strip() if postal_code else "0000"
            country_code = country_code.strip() if country_code else "N/A"

            valid_from = datetime.now().strftime("%Y-%m-%d")
            valid_to = None
            is_active = True

            if not userid:
                print("Skipping records with missing User IDs")
                continue

            transformed_data.append((userid, name, email, street, number, city, postal_code, country_code, valid_from, valid_to, is_active))
        print(f"Transformed {len(transformed_data)} records")


        # LOADING
        print("Starting data loading...")
        target_cursor = target_conn.cursor()
        insert_query = """
            INSERT INTO dim_clients (
                clientID, name, email, street, number, city, postal_code, country_code, validFrom, validTo, isActive
            )
            VALUES %s
        """

        # batch loading met execute_values uit psycopg2
        for i in range(0, len(transformed_data), batch_size):
            batch = transformed_data[i:i + batch_size]
            execute_values(target_cursor, insert_query, batch)
            print(f"Batch {i}/{len(transformed_data)} loaded successfully")


        target_conn.commit()
        print("Successfully inserted users in dim_client")
    except Exception as e:
        print(f"Error with inserting data: {e}")
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

main()
