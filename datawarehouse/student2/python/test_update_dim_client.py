import psycopg2
import datetime
import datawarehouse.student2.python.dwh_tools as dwh
from datawarehouse.student2.python.config import SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT

def test_scd_update_direct():
    try:
        # Verbind met de DWH database
        conn_dwh = dwh.establish_connection(SERVER, DATABASE_DWH, USERNAME, PASSWORD, PORT)
        cursor_dwh = conn_dwh.cursor()
        print("Connection established to DWH database")

        # Stap 1: We specifieren hier een test klant en geven deze een nieuw adres
        test_client_id = 1  # Specifieke klant ID
        new_address = "Teststraat 123, 2610 Wilrijk"

        # Stap 2: Haal huidige gegevens op
        cursor_dwh.execute("""
            SELECT address, country_code, scd_active, scd_version
            FROM dim_clients
            WHERE clientID = %s AND scd_active = True
        """, (test_client_id,))
        current_record = cursor_dwh.fetchone()

        if current_record:
            # Unpack gegevens
            current_address, country_code, is_active, scd_version = current_record

            # Klantgegevens uitprinten
            print(f"Current data for client {test_client_id}: {current_address}\n SCD version: {scd_version}")

            # We gaan hier het oude record updaten en deze niet actief zetten
            if current_address != new_address:
                update_query = """
                    UPDATE dim_clients
                    SET scd_end = %s, scd_active = False
                    WHERE clientID = %s AND scd_active = True
                """
                cursor_dwh.execute(update_query, (datetime.datetime.now(), test_client_id))
                print(f"Old record was deactivated for client ID: {test_client_id}.")

                # Stap 4: Voeg een nieuw record toe met de nieuwe gegevens
                insert_query = """
                    INSERT INTO dim_clients (clientID, name, address, country_code, subscription_type, scd_start, scd_end, scd_version, scd_active, last_ride_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor_dwh.execute(insert_query, (
                    test_client_id, "Bouman Lars", new_address, "BE", 1,
                    datetime.datetime.now(), "2040-01-01", scd_version + 1, True, datetime.datetime.now()
                ))
                print(f"New record added for client: {test_client_id}.")
            else:
                print(f"No changes necessary for client: {test_client_id} (address is still the same).")
        else:
            print(f"No current record found for client ID {test_client_id}.")

        # Commit de transactie
        conn_dwh.commit()

    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Sluit de verbinding
        if conn_dwh:
            cursor_dwh.close()
            conn_dwh.close()
            print("Connection was closed.")

# Roep de functie aan
test_scd_update_direct()
