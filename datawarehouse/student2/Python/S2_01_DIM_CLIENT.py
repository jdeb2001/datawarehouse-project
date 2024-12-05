import psycopg2
import pandas as pd

db_config = {
    'dbname': 'datawarehouse',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432',
}
# Klantdata (voorbeeldrecords)
# dit moet opgehaald worden van de users CSV
customers = [
    {
        'CustomerID': 1,
        'Address': 'Main Street 123',
        'City': 'Cityville',
        'PostalCode': '12345',
        'SubscriptionType': 'Premium',
        'ValidFrom': '2024-01-01',
        'ValidTo': None,  # Huidige actieve rij
        'IsActive': True
    },
    {
        'CustomerID': 2,
        'Address': 'Second Avenue 456',
        'City': 'Townsville',
        'PostalCode': '67890',
        'SubscriptionType': 'Basic',
        'ValidFrom': '2024-01-01',
        'ValidTo': '2024-06-01',
        'IsActive': False
    },
    {
        'CustomerID': 2,
        'Address': 'Third Street 789',
        'City': 'Newtown',
        'PostalCode': '67891',
        'SubscriptionType': 'Premium',
        'ValidFrom': '2024-06-02',
        'ValidTo': None,
        'IsActive': True
    }
]

def connect_to_db(config):
    try:
        conn = psycopg2.connect(**config)
        print("Connection to database established")
        return conn
    except Exception as e:
        print(f"Fout bij verbinden met database: {e}")

# functie om CSV bestanden in te lezen, deze gaan we nodig hebben om onze clients uit VeloDB te importeren
def insert_data_from_csv(conn, table_name, csv_file, columns):
    try:
        data = pd.read_csv(csv_file)

        cursor = conn.cursor()

        for _, row in data.iterrows():
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            cursor.execute(query, tuple(row[col] for col in columns))

        conn.commit()
        print(f"Insert data from {csv_file} complete")
    except Exception as e:
        print(f"Fout bij verbinden met database: {e}")
        conn.rollback()


def fill_table_dim_client(cursor_dwh, table_name='dim_client'):
    insert_query = f"""
    INSERT INTO {table_name} (
     CustomerID, Address, City, PostalCode, SubscriptionType, ValidFrom, ValidTo, IsActive
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    for customer in customers:
        try:
            cursor_dwh.execute(insert_query, (
                customer['CustomerID'],
                customer['Address'],
                customer['City'],
                customer['PostalCode'],
                customer['SubscriptionType'],
                customer['ValidFrom'],
                customer['ValidTo'],
                customer['IsActive']
            ))
            print(f"Klant {customer['CustomerID']} is inserted successfully")
        except Exception as e:
            print(f"Error adding client: {e}")


def main():
    conn = connect_to_db(db_config)
    if conn is None:
        return

    try:
        cursor_dwh = conn.cursor()

        fill_table_dim_client(cursor_dwh)

        conn.commit()
        print("All changes saved successfully")

    except Exception as e:
        print(f"Error during operating: {e}")
        conn.rollback()
    finally:
        if conn:
            cursor_dwh.close()
            conn.close()
            print("Closed connection to database.")

main()
