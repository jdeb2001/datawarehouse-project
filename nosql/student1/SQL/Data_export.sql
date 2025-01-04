SELECT MAX(starttime), MIN(starttime)
FROM rides;

SELECT DISTINCT(zipcode)
FROM stations
GROUP BY zipcode;


COPY (
    SELECT JSON_AGG(ROW_TO_JSON(data_part_1))
    FROM (SELECT rideid,
                 startpoint,
                 endpoint,
                 starttime,
                 endtime,
                 (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(v)))
                  FROM (SELECT vehicleid,
                               serialnumber,
                               (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(blot)))
                                FROM (SELECT b.bikelotid,
                                             b.deliverydate,
                                             (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(btyp)))
                                              FROM (SELECT *
                                                    FROM bike_types
                                                    WHERE bike_types.biketypeid = b.biketypeid
                                                    ) AS "btyp"
                                              ) AS "bike_type"
                                      FROM bikelots AS "b"
                                      WHERE b.bikelotid = vehicles.bikelotid
                                      ) AS "blot"
                                ) AS "bike_lot"
                        FROM vehicles
                        WHERE r.vehicleid = vehicles.vehicleid
                        ) AS "v"
                  ) AS "vehicle_info" -- array met alle info over elk voertuig
          FROM rides AS "r"
           WHERE DATE(starttime) BETWEEN TO_DATE('2015-09-22', 'YYYY-MM-DD') AND TO_DATE('2019-09-22', 'YYYY-MM-DD')
          ) AS "data_part_1"
) TO 'C:\kdg\DB_2\Project_P2\output_json\data_part_1.json';


COPY (
    SELECT JSON_AGG(ROW_TO_JSON(data_part_2))
    FROM (SELECT rideid,
                 startpoint,
                 endpoint,
                 starttime,
                 endtime,
                 (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(v)))
                  FROM (SELECT vehicleid,
                               serialnumber,
                               (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(blot)))
                                FROM (SELECT b.bikelotid,
                                             b.deliverydate,
                                             (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(btyp)))
                                              FROM (SELECT *
                                                    FROM bike_types
                                                    WHERE bike_types.biketypeid = b.biketypeid
                                                    ) AS "btyp"
                                              ) AS "bike_type"
                                      FROM bikelots AS "b"
                                      WHERE b.bikelotid = vehicles.bikelotid
                                      ) AS "blot"
                                ) AS "bike_lot"
                        FROM vehicles
                        WHERE r.vehicleid = vehicles.vehicleid
                        ) AS "v"
                  ) AS "vehicle_info" -- array met alle info over elk voertuig
          FROM rides AS "r"
           WHERE DATE(starttime) BETWEEN TO_DATE('2019-09-22', 'YYYY-MM-DD') AND TO_DATE('2021-09-22', 'YYYY-MM-DD')
          ) AS "data_part_2"
) TO 'C:\kdg\DB_2\Project_P2\output_json\data_part_2.json';

COPY (
    SELECT JSON_AGG(ROW_TO_JSON(data_part_3))
    FROM (SELECT rideid,
                 startpoint,
                 endpoint,
                 starttime,
                 endtime,
                 (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(v)))
                  FROM (SELECT vehicleid,
                               serialnumber,
                               (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(blot)))
                                FROM (SELECT b.bikelotid,
                                             b.deliverydate,
                                             (SELECT ARRAY_TO_JSON(ARRAY_AGG(ROW_TO_JSON(btyp)))
                                              FROM (SELECT *
                                                    FROM bike_types
                                                    WHERE bike_types.biketypeid = b.biketypeid
                                                    ) AS "btyp"
                                              ) AS "bike_type"
                                      FROM bikelots AS "b"
                                      WHERE b.bikelotid = vehicles.bikelotid
                                      ) AS "blot"
                                ) AS "bike_lot"
                        FROM vehicles
                        WHERE r.vehicleid = vehicles.vehicleid
                        ) AS "v"
                  ) AS "vehicle_info" -- array met alle info over elk voertuig
          FROM rides AS "r"
           WHERE DATE(starttime) > TO_DATE('2021-09-22', 'YYYY-MM-DD')
          ) AS "data_part_3"
) TO 'C:\kdg\DB_2\Project_P2\output_json\data_part_3.json';