DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_client;
DROP TABLE IF EXISTS dim_lock;
DROP TABLE IF EXISTS fact_ride;

CREATE TABLE dim_dates (
    date_SK SERIAL PRIMARY KEY,
    date DATE,
    day_of_month SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    weekday VARCHAR(10),
    month_name VARCHAR(15),
    quarter SMALLINT NOT NULL
);

CREATE TABLE dim_clients (
    client_sk SERIAL PRIMARY KEY,
    clientID INTEGER NOT NULL,
    name VARCHAR(100),
    email VARCHAR(100),
    street VARCHAR(100),
    number VARCHAR(10),
    city VARCHAR(100),
    postal_code VARCHAR(255),
    country_code VARCHAR(3),
    --subscriptionType VARCHAR(50),
    validFrom DATE NOT NULL,
    validTo DATE,
    isActive BOOLEAN NOT NULL DEFAULT true --klopt default true hier?
);

--TOOD: steppen moeten aangeduid word door lock id 0
CREATE TABLE dim_locks (
    lock_sk SERIAL PRIMARY KEY,         -- Surrogaatsleutel
    lockID INTEGER,                     -- Originele slot-ID
    stationLockNr INTEGER,              -- Slotnummer binnen het station
    stationAddress VARCHAR(255),        -- Samengesteld adres van het station
    stationZipCode VARCHAR(10),                -- Postcode van het station
    stationDistrict VARCHAR(100),              -- District waar het station zich bevindt
    stationCoordinations POINT,                     -- GPS-coördinaten van het station, klopt POINT hiervoeor?
    stationType VARCHAR(20)             -- Type station (bijv. 'Standard', 'Large')
);

--andere dimensies moeten nog aangemaakt worden om dit in orde te krijgen!
CREATE TABLE fact_rides (
    ride_sk       SERIAL PRIMARY KEY, -- Surrogaatsleutel
    date_sk       INTEGER  NOT NULL,  -- Verwijzing naar DIM_DATE
    weather_sk    INTEGER  NOT NULL,  -- Verwijzing naar DIM_WEATHER
    client_sk   INTEGER  NOT NULL,  -- Verwijzing naar DIM_CUSTOMER
    start_lock_sk INTEGER  NOT NULL,  -- Verwijzing naar DIM_LOCK (startslot)
    end_lock_sk   INTEGER  NOT NULL,  -- Verwijzing naar DIM_LOCK (eindslot)
    duration      INTERVAL NOT NULL,  -- Duur van de rit
    distance      DECIMAL(10, 2)     -- Afstand van de rit in kilometer
);

ALTER TABLE fact_rides
    ADD CONSTRAINT fact_rides_dim_date_fk FOREIGN KEY (date_sk)
        REFERENCES dim_date (date_sk);

ALTER TABLE fact_rides
    ADD CONSTRAINT fact_rides_dim_weather_fk FOREIGN KEY (weather_sk)
        REFERENCES dim_weather (weather_sk);

ALTER TABLE fact_rides
    ADD CONSTRAINT fact_rides_dim_startlock_fk FOREIGN KEY (start_lock_sk)
        REFERENCES dim_locks (lock_sk);

ALTER TABLE fact_rides
    ADD CONSTRAINT fact_rides_dim_endlock_fk FOREIGN KEY (end_lock_sk)
        REFERENCES dim_locks (lock_sk);

ALTER TABLE fact_rides
    ADD CONSTRAINT fact_rides_dim_client FOREIGN KEY (client_sk)
        REFERENCES dim_clients (client_sk);
