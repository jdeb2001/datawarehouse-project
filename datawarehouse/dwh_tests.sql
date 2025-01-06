SELECT *
FROM rides
WHERE startlockid IN (SELECT lockid
                      FROM locks
                      WHERE stationid in (SELECT stations.stationid
                                          FROM stations
                                          WHERE zipcode = '2030')
                      )
AND
    DATE_PART('year', starttime) = 2020 AND
    DATE_PART('month', starttime) = 7
ORDER BY 1;


SELECT DISTINCT(zipcode)
FROM stations
GROUP BY zipcode;

SELECT ride_sk, weather_sk, client_sk, start_lock_sk, dd.date_sk, date
FROM fact_rides fr
JOIN public.dim_date dd on dd.date_sk = fr.date_sk
WHERE weather_sk = 2;