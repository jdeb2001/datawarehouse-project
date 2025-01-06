import psycopg2
import dwh_tools as dwh
from config import SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT


def fill_table_dim_weather(cursor_dwh, table_name='dim_weather'):
    # reset_statement = f"""
    #     TRUNCATE {table_name} CASCADE;
    #     """
    insert_query = f"""
        INSERT INTO {table_name} (weather_type, weather_description)
        VALUES (%s, %s);
        """
    # cursor_dwh.execute(reset_statement)
    cursor_dwh.execute(insert_query, (
        "onaangenaam", "neerslag"
    ))
    cursor_dwh.execute(insert_query, (
        "aangenaam", "+15 C en de zon schijnt"
    ))
    cursor_dwh.execute(insert_query, (
        "neutraal", "niet aangenaam noch onaangenaam"
    ))
    cursor_dwh.execute(insert_query, (
        "weertype onbekend", ""
    ))

    # Commit txn
    cursor_dwh.connection.commit()


def main():
    try:
        # Connect to the 'dwh_bike_analytics' database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
        cursor_dwh = conn_dwh.cursor()

        # Fill the 'dim_weather' table
        fill_table_dim_weather(cursor_dwh)

        # Close connections
        cursor_dwh.close()
        conn_dwh.close()

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()