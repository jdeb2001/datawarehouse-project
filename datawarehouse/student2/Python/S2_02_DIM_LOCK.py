import psycopg2

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
