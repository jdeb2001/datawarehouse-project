# TODO: volgorde van scripts kan nog veranderen, dit zeker in het oog houden in filenaam
# vergeet niet zelf om het juiste wachtwoord voor jouw eigen postgres bank in te geven!
import psycopg2

# Databaseconfiguratie
source_db_config = {
    'dbname': 'velodb',
    'user': 'postgres',
    'password': '<PASSWORD>',
    'host': 'localhost',
    'port': 5432
}

target_db_config = {
    'dbname': 'dwh_bike_analytics',
    'user': 'postgres',
    'password': '<PASSWORD>',
    'host': 'localhost',
    'port': 5432
}

# Functie om verbinding te maken met de database
def connect_to_db(config):
    try:
        conn = psycopg2.connect(**config)
        print(f"Verbonden met database {config['dbname']}")
        return conn
    except Exception as e:
        print(f"Fout bij verbinden met database {config['dbname']}: {e}")
        return None

# Functie om FACT_RIDE te vullen
def populate_fact_ride(source_conn, target_conn):
    try:
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()

        # Haal ritgegevens op uit de brondatabase
        source_cursor.execute("""
            SELECT
                rideid,
                starttime,
                endtime,
                vehicleid,
                subscriptionid,
                startlockid,
                endlockid
            FROM rides
        """)
        rides = source_cursor.fetchall()

        for ride in rides:
            rideid, starttime, endtime, vehicleid, subscriptionid, startlockid, endlockid = ride

            # Bereken de duur van de rit
            duration = endtime - starttime

            # Haal DATE_SK op uit DIM_DATE
            target_cursor.execute("""
                SELECT DATE_SK FROM DIM_DATE WHERE Date = %s
            """, (starttime.date(),))
            date_result = target_cursor.fetchone()
            if date_result:
                date_sk = date_result[0]
            else:
                print(f"Datum {starttime.date()} niet gevonden in DIM_DATE.")
                continue

            # Haal WEATHER_SK op (hier gebruik ik 'Onbekend')
            # TODO: aanpassen naarmate requirements opgesteld door S1
            target_cursor.execute("""
                SELECT WEATHER_SK FROM DIM_WEATHER WHERE WeatherType = 'Onbekend'
            """)
            weather_sk = target_cursor.fetchone()[0]

            # Haal CUSTOMER_SK op uit DIM_CUSTOMER via SubscriptionId
            source_cursor.execute("""
                SELECT userid FROM subscriptions WHERE subscriptionid = %s
            """, (subscriptionid,))
            subscription_result = source_cursor.fetchone()
            if subscription_result:
                userid = subscription_result[0]
            else:
                print(f"Subscription {subscriptionid} niet gevonden.")
                continue

            target_cursor.execute("""
                SELECT CUSTOMER_SK FROM DIM_CUSTOMER WHERE CustomerID = %s AND IsActive = TRUE
            """, (userid,))
            customer_result = target_cursor.fetchone()
            if customer_result:
                customer_sk = customer_result[0]
            else:
                print(f"CustomerID {userid} niet gevonden in DIM_CUSTOMER.")
                continue

            # Haal START_LOCK_SK op uit DIM_LOCK
            if startlockid is not None:
                target_cursor.execute("""
                    SELECT LOCK_SK FROM DIM_LOCK WHERE LockID = %s
                """, (startlockid,))
                start_lock_result = target_cursor.fetchone()
                if start_lock_result:
                    start_lock_sk = start_lock_result[0]
                else:
                    print(f"Start LockID {startlockid} niet gevonden in DIM_LOCK.")
                    continue
            else:
                # Gebruik "Geen slot"
                target_cursor.execute("""
                    SELECT LOCK_SK FROM DIM_LOCK WHERE IsStep = TRUE
                """)
                start_lock_sk = target_cursor.fetchone()[0]

            # Haal END_LOCK_SK op uit DIM_LOCK
            if endlockid is not None:
                target_cursor.execute("""
                    SELECT LOCK_SK FROM DIM_LOCK WHERE LockID = %s
                """, (endlockid,))
                end_lock_result = target_cursor.fetchone()
                if end_lock_result:
                    end_lock_sk = end_lock_result[0]
                else:
                    print(f"End LockID {endlockid} niet gevonden in DIM_LOCK.")
                    continue
            else:
                # Gebruik "Geen slot"
                target_cursor.execute("""
                    SELECT LOCK_SK FROM DIM_LOCK WHERE IsStep = TRUE
                """)
                end_lock_sk = target_cursor.fetchone()[0]

            # Afstand is optioneel; zet op NULL of bereken indien mogelijk
            distance = None

            # Voeg record in FACT_RIDE in
            insert_query = """
                INSERT INTO FACT_RIDE (
                    DATE_SK, WEATHER_SK, CUSTOMER_SK, START_LOCK_SK, END_LOCK_SK, Duration, Distance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            target_cursor.execute(insert_query, (
                date_sk,
                weather_sk,
                customer_sk,
                start_lock_sk,
                end_lock_sk,
                duration,
                distance
            ))

        # Wijzigingen opslaan
        target_conn.commit()
        print("FACT_RIDE tabel succesvol gevuld.")

    except Exception as e:
        print(f"Fout bij het vullen van FACT_RIDE: {e}")
        target_conn.rollback()

# Hoofdprogramma
def main():
    source_conn = connect_to_db(source_db_config)
    target_conn = connect_to_db(target_db_config)

    if not source_conn or not target_conn:
        print("Kan geen verbinding maken met databases.")
        return

    try:
        populate_fact_ride(source_conn, target_conn)
    finally:
        source_conn.close()
        target_conn.close()
        print("Databaseverbindingen gesloten.")

if __name__ == "__main__":
    main()
