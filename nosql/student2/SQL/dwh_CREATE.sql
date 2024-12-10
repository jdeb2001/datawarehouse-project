DROP TABLE IF EXISTS dim_client;
DROP TABLE IF EXISTS dim_lock;

CREATE TABLE dim_client (
    customer_SK SERIAL PRIMARY KEY,
    customerID INTEGER NOT NULL,
    city VARCHAR(100),
    address VARCHAR(255),
    postalCode VARCHAR(255),
    subscriptionType VARCHAR(50),
    validFrom DATE NOT NULL,
    validTo DATE,
    isActive BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE dim_lock (
                          LOCK_SK SERIAL PRIMARY KEY,         -- Surrogaatsleutel
                          lockID INTEGER,                     -- Originele slot-ID
                          stationLockNr INTEGER,              -- Slotnummer binnen het station
                          stationAddress VARCHAR(255),        -- Samengesteld adres van het station
                          zipCode VARCHAR(10),                -- Postcode van het station
                          district VARCHAR(100),              -- District waar het station zich bevindt
                          GPSCoord POINT,                     -- GPS-coördinaten van het station
                          stationType VARCHAR(20),            -- Type station (bijv. 'Standard', 'Large')
                          isStep BOOLEAN DEFAULT FALSE        -- TRUE voor ritten zonder slot
);

--andere dimensies moeten nog aangemaakt worden om dit in orde te krijgen!
CREATE TABLE FACT_RIDE (
                           RIDE_SK SERIAL PRIMARY KEY,       -- Surrogaatsleutel
                           DATE_SK INTEGER NOT NULL,         -- Verwijzing naar DIM_DATE
                           WEATHER_SK INTEGER NOT NULL,      -- Verwijzing naar DIM_WEATHER
                           CUSTOMER_SK INTEGER NOT NULL,     -- Verwijzing naar DIM_CUSTOMER
                           START_LOCK_SK INTEGER NOT NULL,   -- Verwijzing naar DIM_LOCK (startslot)
                           END_LOCK_SK INTEGER NOT NULL,     -- Verwijzing naar DIM_LOCK (eindslot)
                           Duration INTERVAL NOT NULL,       -- Duur van de rit
                           Distance DECIMAL(10,2),           -- Afstand van de rit in kilometer
                           FOREIGN KEY (DATE_SK) REFERENCES DIM_DATE(DATE_SK),
                           FOREIGN KEY (WEATHER_SK) REFERENCES DIM_WEATHER(WEATHER_SK),
                           FOREIGN KEY (CUSTOMER_SK) REFERENCES DIM_CUSTOMER(CUSTOMER_SK),
                           FOREIGN KEY (START_LOCK_SK) REFERENCES DIM_LOCK(LOCK_SK),
                           FOREIGN KEY (END_LOCK_SK) REFERENCES DIM_LOCK(LOCK_SK)
);
