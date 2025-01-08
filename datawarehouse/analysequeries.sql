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


-- 4. Verschillen de drukste maanden per gemeente?
SELECT dl.station_zipcode, dd.month_name AS "month",  COUNT(ride_sk) AS "no. of rides"
FROM fact_rides fr
    JOIN public.dim_date dd on dd.date_sk = fr.date_sk
    JOIN public.dim_locks dl on dl.lock_sk = fr.start_lock_sk
GROUP BY dl.station_zipcode, dd.month_name
HAVING (dd.month_name, COUNT(ride_sk)) IN (SELECT dates.month_name,  COUNT(ride_sk)
                                          FROM fact_rides
                                              JOIN public.dim_date dates on dates.date_sk = fact_rides.date_sk
                                              JOIN public.dim_locks locks on locks.lock_sk = fact_rides.start_lock_sk
                                          WHERE locks.station_zipcode = dl.station_zipcode
                                          GROUP BY dates.month_name
                                          ORDER BY 2 DESC
                                          FETCH FIRST 1 ROW ONLY
                                          );



-- Student 2
-- Wat is de invloed wan de woonplaats van de gebruikers op het gebruik van de vehicles?
SELECT
    TRIM(SUBSTRING(dc.address FROM '[0-9]{4} ([A-Za-z ]+)')) AS residence_city,
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
-- Dit zouden we te weten kunnen komen door het aantal keer dat de lock voor een rit gebruikt is geweest:
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
-- Deze query zal nog niet veel geven op dit moment, omdat bij het maken van deze dimensies nog geen klant is gemarkeerd als inactief.
-- Het is pas wanneer deze gegevens later ingevuld worden dat deze query daadwerkelijk betekenisvolle resultaten zal geven.
SELECT
    dl.station_address,
    COUNT(DISTINCT CASE WHEN dd.date <= DATE(dc.scd_end) THEN fr.ride_sk ELSE NULL END) AS rides_before_end,
    COUNT(DISTINCT CASE WHEN dd.date > DATE(dc.scd_end) THEN fr.ride_sk ELSE NULL END) AS rides_after_end,
    COUNT(DISTINCT CASE WHEN dd.date > DATE(dc.scd_end) THEN fr.ride_sk ELSE NULL END) -
    COUNT(DISTINCT CASE WHEN dd.date <= DATE(dc.scd_end) THEN fr.ride_sk ELSE NULL END) AS effect_on_station
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
    AND dl.station_address IS NOT NULL
    AND dl.station_address != 'Geen locatie'
GROUP BY
    dl.station_address
ORDER BY
    effect_on_station DESC, rides_after_end DESC;




-- Bijkomende vragen
-- Welke dagen van de maand zijn het meest populair voor ritten, afhankelijk van seizoen?
-- We gaan hier het aantal ritten op een bepaalde dag van de maand ophalen en op basis van de maand van de rit categoriseren per seizoen:
SELECT
    dd.month,
    dd.day_of_month,
    CASE
        WHEN dd.month IN (12, 1, 2) THEN 'Winter'
        WHEN dd.month IN (3, 4, 5) THEN 'Spring'
        WHEN dd.month IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    COUNT(fr.ride_sk) AS ride_count
FROM
    fact_rides fr
JOIN
    dim_date dd ON fr.date_sk = dd.date_sk
GROUP BY
    dd.month, dd.day_of_month, season
ORDER BY
    season, ride_count DESC;

-- Welke dagen van de week hebben het hoogste percentage aan ritten naar dezelfde eindlocatie als de startlocatie?
SELECT
    dd.weekday,
    COUNT(*) AS total_rides,
    SUM(CASE WHEN fr.start_lock_sk = fr.end_lock_sk THEN 1 ELSE 0 END) AS same_location_rides,
    (SUM(CASE WHEN fr.start_lock_sk = fr.end_lock_sk THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS same_location_percentage
FROM
    fact_rides fr
JOIN
    dim_date dd ON fr.date_sk = dd.date_sk
GROUP BY
    dd.weekday
ORDER BY
    same_location_percentage DESC;
