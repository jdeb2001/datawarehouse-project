import psycopg2
import pandas as pd
import dwh_tools as dwh
from config import SERVER, DATABASE_VELO, DATABASE_DWH, USERNAME, PASSWORD, PORT


def fetch_min_start_date(cursor_op):
    """
    Fetches the minimum start date from the 'rides' table.
    Args:
        cursor_op: The cursor object for the 'velo_db' database.
    Returns:
        str: The minimum order date.
    """
    cursor_op.execute('SELECT MIN(starttime) FROM rides')
    return cursor_op.fetchone()[0]

def fetch_max_start_date(cursor_op):
    cursor_op.execute('SELECT MAX(starttime) FROM rides')
    return cursor_op.fetchone()[0]

def fill_table_dim_date(cursor_dwh, start_date, end_date='2040-01-01', table_name='dim_date'):
    """
    Fills the 'dim_date' table with date-related data.
    Args:
        cursor_dwh: The cursor object for the 'dwh_bike_analytics' database.
        start_date (str): The start date for filling the table.
        end_date (str): The end date for filling the table (default is '2040-01-01').
        table_name (str): The name of the table (default is 'dim_date').
    """
    insert_query = f"""
    INSERT INTO {table_name} (date, day_of_month, month, year, day_of_week, day_of_year, weekday, month_name, quarter)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    current_date = pd.to_datetime(start_date)
    # TODO: use fetch_max_start_date() for end_date
    end_date = pd.to_datetime(end_date)
    while current_date <= end_date:
        day_of_month = current_date.day
        month = current_date.month
        year = current_date.year
        day_of_week = current_date.dayofweek
        day_of_year = current_date.timetuple().tm_yday
        weekday = current_date.strftime('%A')
        month_name = current_date.strftime('%B')
        quarter = (current_date.month - 1) // 3 + 1  # Calculate quarter based on the month

        # Execute the INSERT query
        cursor_dwh.execute(insert_query, (
            current_date, day_of_month, month, year, day_of_week, day_of_year, weekday, month_name, quarter
        ))

        # Commit the transaction
        cursor_dwh.connection.commit()
        current_date += pd.Timedelta(days=1)

def main():
    try:
        # Connect to the 'velo_db' database
        conn_op = dwh.establish_connection(SERVER, DATABASE_VELO, USERNAME, PASSWORD, PORT)
        cursor_op = conn_op.cursor()

        # Connect to the 'dwh_bike_analytics' database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
        cursor_dwh = conn_dwh.cursor()

        # Fetch minimum start date
        start_date = fetch_min_start_date(cursor_op)
        print(f"Minimum start date: {start_date}")

        # Fill the 'dim_date' table
        fill_table_dim_date(cursor_dwh, start_date, '2100-01-01', 'dim_date')

        # Close the connections
        cursor_op.close()
        conn_op.close()
        cursor_dwh.close()
        conn_dwh.close()

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
