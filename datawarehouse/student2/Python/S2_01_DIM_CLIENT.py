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

def transfer_users_to_dim_client(source_conn, target_conn):
    try:
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT userid, street || ' ' || number AS address, zipcode, city FROM velo_users")
        users_data = source_cursor.fetchall()

        target_cursor = target_conn.cursor()

        insert_query = """
            INSERT INTO dim_client (
                CustomerID, Address, City, PostalCode, SubscriptionType, ValidFrom, ValidTo, IsActive
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for user in users_data:
            userid, address, zipcode, city = user
            target_cursor.execute(insert_query, (userid, address, zipcode, city, 'Basic', '2019-09-21', None, False))

        target_conn.commit()
        print("Successfully inserted users in DIM_CLIENT")
    except Exception as e:
        print(f"Error with transferring data: {e}")
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
