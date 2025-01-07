SELECT rideid, starttime, endtime, startlockid, endlockid
FROM rides
WHERE startlockid IN (SELECT lockid
                      FROM locks
                      WHERE stationid IN (SELECT stations.stationid
                                          FROM stations
                                          WHERE zipcode = '2660')
                      )
AND
    DATE_PART('year', starttime) = 2022 AND
    DATE_PART('month', starttime) = 11 AND
    DATE_PART('day', starttime) = 27
ORDER BY starttime;


SELECT DISTINCT(zipcode)
FROM stations
GROUP BY zipcode;

SELECT ride_sk, weather_sk, client_sk, start_lock_sk, dd.date_sk, date
FROM fact_rides fr
JOIN public.dim_date dd on dd.date_sk = fr.date_sk
JOIN public.dim_locks dl on dl.lock_sk = fr.start_lock_sk
WHERE weather_sk = 3
AND dl.stationzipcode = '2060';