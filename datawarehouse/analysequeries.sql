-- Student 1

-- 1. Wat zijn de drukke momenten (op dagbasis) in de week t.o.v. het weekend?
SELECT dd.weekday AS "Day", COUNT(ride_sk) AS "Number of rides"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
WHERE LOWER(dd.weekday) NOT IN ('saturday', 'sunday')
GROUP BY dd.weekday
ORDER BY 2 DESC;

-- 2. Hebben datumparameters invloed op de afgelegde afstand?
-- De dag van de week heeft wél een invloed!
SELECT dd.day_of_week, dd.weekday AS "Day", SUM(distance) AS "Distance travelled"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
GROUP BY dd.day_of_week , dd.weekday
ORDER BY 1;

-- Per quarter: ~2.5% hoger in Q4 (niet zo significant)
SELECT dd.quarter AS "Quarter", SUM(distance) AS "Distance Travelled"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
GROUP BY dd.quarter
ORDER BY 1;
-- Een beetje invloed bv. afstand ~9% hoger in januari tov december
SELECT dd.month AS "Month", SUM(distance) AS "Distance Travelled"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
GROUP BY dd.month
ORDER BY 1;

-- Afstand t.o.v. zomervakantie => zo goed als geen verschil!
SELECT SUM(distance) AS "Total distance"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
WHERE dd.date BETWEEN TO_DATE('2021-03-01', 'YYYY-MM-DD') AND TO_DATE('2021-04-30', 'YYYY-MM-DD');

SELECT SUM(distance) AS "Total distance"
FROM fact_rides
JOIN public.dim_date dd on dd.date_sk = fact_rides.date_sk
WHERE dd.date BETWEEN '2021-07-01' AND '2021-08-31';

-- 3. Heeft weer invloed op ritten?
SELECT fr.weather_sk, dw.weather_type, COUNT(ride_sk) AS "number of rides"
FROM fact_rides fr
JOIN public.dim_weather dw on dw.weather_sk = fr.weather_sk
WHERE fr.weather_sk IN (1, 2, 3)
GROUP BY (fr.weather_sk, dw.weather_type);




-- Student 2

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