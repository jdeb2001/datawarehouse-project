import psycopg2
import pandas as pd
import dwh_tools as dwh
from config import SERVER, DATABASE_VELO, DATABASE_DWH, USERNAME, PASSWORD, PORT


def fetch_min_order_date(cursor_velo):
    """
    Fetches the minimum order date from the 'sales' table.
    Args:
        cursor_velo: The cursor object for the 'tutorial_op' database.
    Returns:
        str: The minimum order date.
    """
    cursor_velo.execute('SELECT MIN(starttime) FROM rides')
    return cursor_velo.fetchone()[0]

def fill_table_dim_date(cursor_dwh, start_date, end_date='2040-01-01', table_name='dim_date'):
    """
    Fills the 'dim_date' table with date-related data.
    Args:
        cursor_dwh: The cursor object for the 'tutorial_dwh' database.
        start_date (str): The start date for filling the table.
        end_date (str): The end date for filling the table (default is '2040-01-01').
        table_name (str): The name of the table (default is 'dim_date').
    """
    insert_query = f"""
    INSERT INTO {table_name} (date, day_of_month, month, year, day_of_week, day_of_year, weekday, month_name, quarter)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    current_date = pd.to_datetime(start_date)
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
        # Connect to the 'tutorial_op' database
        conn_velo = dwh.establish_connection(SERVER, DATABASE_VELO, USERNAME, PASSWORD, PORT)
        cursor_velo = conn_velo.cursor()

        # Connect to the 'tutorial_dwh' database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
        cursor_dwh = conn_dwh.cursor()

        # Fetch minimum order date
        start_date = fetch_min_order_date(cursor_velo)
        print(f"Minimum order date: {start_date}")

        # Fill the 'dim_day' table
        fill_table_dim_date(cursor_dwh, start_date, '2100-01-01', 'dim_date')

        # Close the connections
        cursor_velo.close()
        conn_velo.close()
        cursor_dwh.close()
        conn_dwh.close()

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")

if __name__ == "__main__":
    main()
