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
