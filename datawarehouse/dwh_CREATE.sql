DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_weather CASCADE;
DROP TABLE IF EXISTS dim_clients CASCADE;
DROP TABLE IF EXISTS dim_locks CASCADE;
DROP TABLE IF EXISTS fact_rides CASCADE;

CREATE TABLE dim_date
(
    date_SK      SERIAL PRIMARY KEY,
    date         DATE,
    day_of_month SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    year         SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,
    day_of_year  SMALLINT NOT NULL,
    weekday      VARCHAR(10),
    month_name   VARCHAR(15),
    quarter      SMALLINT NOT NULL
);

CREATE TABLE dim_weather
(
    weather_SK          SERIAL PRIMARY KEY,
    weather_type        VARCHAR(30),
    weather_description VARCHAR(50)
);

CREATE TABLE dim_clients
(
    client_SK    SERIAL PRIMARY KEY,
    clientID     INTEGER NOT NULL,
    name         VARCHAR(100),
    email        VARCHAR(100),
    address      VARCHAR(255),
    /*name         VARCHAR(100),
    email        VARCHAR(100),
    street       VARCHAR(100),
    number       VARCHAR(10),
    city         VARCHAR(100),
    postal_code  VARCHAR(255),*/
    country_code VARCHAR(3),
    subscriptionType VARCHAR(50),
    scd_start    DATE    NOT NULL, -- naar scd_start en scd_end hernoemen ipv validFrom en validTo
    scd_end      DATE,
    scd_version INTEGER NOT NULL DEFAULT 1,
    isActive     BOOLEAN NOT NULL DEFAULT true,
    last_ride_date DATE
);



--TOOD: steppen moeten aangeduid word door lock id 0
CREATE TABLE dim_locks
(
    lock_SK              SERIAL PRIMARY KEY, -- Surrogaatsleutel
    lockID               INTEGER,            -- Originele slot-ID
    stationLockNr        INTEGER,            -- Slotnummer binnen het station
    stationAddress       VARCHAR(255),       -- Samengesteld adres van het station
    stationZipCode       VARCHAR(10),        -- Postcode van het station
    stationDistrict      VARCHAR(100),       -- District waar het station zich bevindt
    stationCoordinations POINT,              -- GPS-coördinaten van het station, klopt POINT hiervoeor?
    stationType          VARCHAR(20)         -- Type station (bijv. 'Standard', 'Large')
);

--andere dimensies moeten nog aangemaakt worden om dit in orde te krijgen!
CREATE TABLE fact_rides
(
    ride_SK       SERIAL PRIMARY KEY, -- Surrogaatsleutel
    date_SK       INTEGER
        CONSTRAINT fact_rides_dim_date_fk REFERENCES dim_date (date_SK)
                           NOT NULL,  -- Verwijzing naar DIM_DATE
    weather_SK    INTEGER
        CONSTRAINT fact_rides_dim_weather_fk REFERENCES dim_weather (weather_SK)
                           NOT NULL,  -- Verwijzing naar DIM_WEATHER
    client_SK     INTEGER
        CONSTRAINT fact_rides_dim_client REFERENCES dim_clients (client_SK)
                           NOT NULL,  -- Verwijzing naar DIM_CUSTOMER
    start_lock_SK INTEGER
        CONSTRAINT fact_rides_dim_startlock_fk REFERENCES dim_locks (lock_SK)
                           NOT NULL,  -- Verwijzing naar DIM_LOCK (startslot)
    end_lock_SK   INTEGER
        CONSTRAINT fact_rides_dim_endlock_fk REFERENCES dim_locks (lock_SK)
                           NOT NULL,  -- Verwijzing naar DIM_LOCK (eindslot)
    duration      INTERVAL NOT NULL,  -- Duur van de rit
    distance      DECIMAL(10, 2)      -- Afstand van de rit in kilometer
);
