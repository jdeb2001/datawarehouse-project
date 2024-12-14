import psycopg2
from datetime import datetime

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
                SELECT date_sk FROM dim_date WHERE Date = %s
            """, (starttime.date(),))
            date_result = target_cursor.fetchone()
            if date_result:
                date_sk = date_result[0]
            else:
                print(f"Datum {starttime.date()} niet gevonden in DIM_DATE.")
                continue

            # Haal WEATHER_SK op (hier gebruik ik 'Onbekend')
            target_cursor.execute("""
                SELECT weather_sk FROM dim_weather WHERE WeatherType = 'Onbekend'
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
                SELECT client_sk FROM dim_clients WHERE clientID = %s AND isActive = TRUE
            """, (userid,))
            customer_result = target_cursor.fetchone()
            if customer_result:
                customer_sk = customer_result[0]
            else:
                print(f"CustomerID {userid} niet gevonden in DIM_CUSTOMER.")
                continue

            # Controleer of de rit een step betreft (geen slot of slot-ID = 0)
            if startlockid is None or startlockid == 0:
                target_cursor.execute("""
                    SELECT lock_sk FROM dim_lock WHERE IsStep = TRUE
                """)
                start_lock_sk = target_cursor.fetchone()[0]
            else:
                # Haal START_LOCK_SK op uit DIM_LOCK
                target_cursor.execute("""
                    SELECT lock_sk FROM dim_lock WHERE lockID = %s
                """, (startlockid,))
                start_lock_result = target_cursor.fetchone()
                if start_lock_result:
                    start_lock_sk = start_lock_result[0]
                else:
                    print(f"Start LockID {startlockid} niet gevonden in DIM_LOCK.")
                    continue

            # Controleer of de rit een step betreft (geen slot of slot-ID = 0)
            if endlockid is None or endlockid == 0:
                target_cursor.execute("""
                    SELECT lock_sk FROM dim_lock WHERE IsStep = TRUE
                """)
                end_lock_sk = target_cursor.fetchone()[0]
            else:
                # Haal END_LOCK_SK op uit DIM_LOCK
                target_cursor.execute("""
                    SELECT lock_sk FROM dim_lock WHERE lockID = %s
                """, (endlockid,))
                end_lock_result = target_cursor.fetchone()
                if end_lock_result:
                    end_lock_sk = end_lock_result[0]
                else:
                    print(f"End LockID {endlockid} niet gevonden in DIM_LOCK.")
                    continue

            # Afstand is optioneel; zet op NULL of bereken indien mogelijk
            distance = None

            # Voeg record in FACT_RIDE in
            insert_query = """
                INSERT INTO fact_ride (
                    date_sk, weather_sk, customer_sk, start_lock_sk, end_lock_sk, duration, distance
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
