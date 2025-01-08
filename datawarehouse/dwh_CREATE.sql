DROP TABLE IF EXISTS fact_rides;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_weather;
DROP TABLE IF EXISTS dim_clients;
DROP TABLE IF EXISTS dim_locks;

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
    country_code VARCHAR(3),
    subscription_type VARCHAR(50),
    scd_start    DATE    NOT NULL, -- naar scd_start en scd_end hernoemen ipv validFrom en validTo
    scd_end      DATE,
    scd_version INTEGER NOT NULL DEFAULT 1,
    scd_active     BOOLEAN NOT NULL DEFAULT true,
    last_ride_date DATE
);

--TOOD: steppen moeten aangeduid word door lock id 0
CREATE TABLE dim_locks
(
    lock_SK               SERIAL PRIMARY KEY, -- Surrogaatsleutel
    lockID                INTEGER,            -- Originele slot-ID
    station_lock_nr       INTEGER,            -- Slotnummer binnen het station
    station_address       VARCHAR(255),       -- Samengesteld adres van het station
    station_zipcode       VARCHAR(10),        -- Postcode van het station
    station_district      VARCHAR(100),       -- District waar het station zich bevindt
    station_coordinates   POINT,              -- GPS-coördinaten van het station, klopt POINT hiervoor?
    station_type          VARCHAR(20)         -- Type station (bijv. 'Standard', 'Large')
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

CREATE INDEX idx_dim_date_month_name ON dim_date(month_name);
CREATE INDEX idx_dim_date_date ON dim_date(date);
CREATE INDEX idx_dim_locks_station_zipcode ON dim_locks(station_zipcode);