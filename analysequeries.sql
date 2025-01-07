-- Wat is de invloed wan de woonplaats van de gebruikers op het gebruik van de vehicles?
SELECT
    REGEXP_MATCH(dc.address, '[0-9]{4} ([A-Za-z ]+)') AS residence_city,
    COUNT(fr.ride_sk) AS ride_count
FROM
    fact_rides fr
JOIN
    dim_clients dc ON fr.client_sk = dc.client_sk
GROUP BY
    residence_city
ORDER BY
    ride_count DESC;

-- Welke sloten hebben preventief onderhoud nodig?
SELECT
    dl.lockID,
    dl.station_address,
    COUNT(fr.start_lock_sk) + COUNT(fr.end_lock_sk) AS usage_count
FROM
    fact_rides fr
LEFT JOIN
    dim_locks dl ON fr.start_lock_sk = dl.lock_sk OR fr.end_lock_sk = dl.lock_sk
WHERE dl.lockid IS NOT NULL
GROUP BY
    dl.lockID, dl.station_Address
ORDER BY
    usage_count DESC;

-- Wat is de invloed van abonnementen op stationsgebruik bij opzegging?
SELECT
    dl.station_address,
    SUM(CASE WHEN dd.date <= dc.scd_end THEN 1 ELSE 0 END) AS rides_before_end,
    SUM(CASE WHEN dd.date > dc.scd_end THEN 1 ELSE 0 END) AS rides_after_end
FROM
    fact_rides fr
JOIN
    dim_clients dc ON fr.client_sk = dc.client_sk
JOIN
    dim_locks dl ON fr.start_lock_sk = dl.lock_sk
JOIN
    dim_date dd ON fr.date_sk = dd.date_sk
WHERE
    dc.scd_end IS NOT NULL  -- Alleen klanten met beëindigde abonnementen
    AND dl.station_address != 'Geen locatie'
GROUP BY
    dl.station_address
ORDER BY
    rides_before_end DESC, rides_after_end DESC;


SELECT * FROM dim_clients WHERE scd_end IS NOT NULL;


-- Bijkomende vragen
-- Welke voertuigen zijn populairder? Hoe verschilt dit per seizoen?
SELECT
    dl.station_address,
    COUNT(fr.ride_sk) AS ride_count
FROM
    fact_rides fr
JOIN
    dim_clients dc ON fr.client_sk = dc.client_sk
JOIN
    dim_locks dl ON fr.start_lock_sk = dl.lock_sk
WHERE
    dc.scd_end IS NOT NULL  -- Alleen opgezegde abonnementen
GROUP BY
    dl.station_address
ORDER BY
    ride_count DESC;

-- Welke dagen hebben gemiddeld de langste ritduur?
SELECT
    dd.weekday,
    AVG(fr.duration) AS avg_duration
FROM
    fact_rides fr
JOIN
    dim_date dd ON fr.date_sk = dd.date_sk
GROUP BY
    dd.weekday
ORDER BY
    avg_duration DESC;