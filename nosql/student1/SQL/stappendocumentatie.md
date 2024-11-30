# Project VeloDB MongoDB
Hieronder de stappen die genomen zijn voor het opzetten van de databank in MongoDB.

## Subset VeloDB exporteren naar JSON
Testdata om mee te beginnen.
```sql
SELECT row_to_json(t)
FROM (SELECT rideid, startpoint, endpoint, starttime, endtime,
             (SELECT array_to_json(array_agg(row_to_json(v)))
              FROM (SELECT vehicleid, serialnumber,
                           (SELECT array_to_json(array_agg(row_to_json(blot)))
                            FROM (SELECT b.bikelotid, b.deliverydate,
                                         (SELECT array_to_json(array_agg(row_to_json(btyp)))
                                          FROM (SELECT *
                                                FROM bike_types
                                                WHERE bike_types.biketypeid = b.biketypeid
                                                ) AS "btyp"
                                          ) AS "bike_type"
                                  FROM bikelots AS "b"
                                  WHERE b.bikelotid = vehicles.bikelotid
                                  ) AS "blot"
                            ) AS "bike_lot",
                           lastmaintenanceon, lockid, position
                    FROM vehicles
                    WHERE r.vehicleid = vehicles.vehicleid
                    ) AS "v"
              ) AS "vehicle_info", -- array met alle info over elk voertuig
             subscriptionid, startlockid, endlockid
      FROM rides AS "r"
      WHERE date(starttime) BETWEEN to_date('2019-09-22', 'YYYY-MM-DD') AND to_date('2019-09-24', 'YYYY-MM-DD')
      LIMIT 20000
      ) AS t;
```
