# Project VeloDB Neo4J
Bij dit project hoort nog een aparte PDF/Word document met iets diepere documentatie van wat ik gedaan heb. In deze markdown kan u de basis vinden van wat ik exact gedaan heb op dit moment.
## Startdata
Eerst importeren we onze datasets die we uit onze Postgres databank gehaald hebben. In mijn geval heb ik dit met 3 aparte .csv bestanden gedaan voor Stations, Ritten, en Gebruikers.
```cypher
LOAD CSV WITH HEADERS FROM 'file:///stations.csv' AS row
MERGE (district:District {name: row.district})
CREATE (s:Station {
stationid: row.stationid,
number: row.number,
type: row.type,
street: row.street,
number: row.number,
zipcode: row.zipcode,
gpscoord: row.gpscoord,
additionalinfo: row.additionalinfo
})
MERGE (station)-[:LOCATED_IN]->(district);
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///vehicles.csv' AS row
CREATE (v:Vehicle {
id: row.vehicleid,
serialNumber: row.serialnumber,
type: row.vehicletype,
city: row.city,
lastMaintenance: row.lastmaintenanceon
});
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS row
CREATE (u:User {
id: row.userid,
name: row.name,
email: row.email,
street: row.street,
number: row.number,
zipcode: row.zipcode,
city: row.city,
countryCode: row.country_code
});
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///rides.csv' AS row
// Optioneel stations matchen, alleen als startlockid of endlockid niet null is
OPTIONAL MATCH (startStation:Station {id: row.startlockid})
OPTIONAL MATCH (endStation:Station {id: row.endlockid})
OPTIONAL MATCH (vehicle:Vehicle {id: row.vehicleid})
OPTIONAL MATCH (user:User {id: row.userid})

// Maak de Ride node aan
CREATE (ride:Ride {
id: row.rideid,
startTime: datetime(replace(row.starttime, " ", "T")), // Verzeker de juiste conversie naar datetime
endTime: datetime(replace(row.endtime, " ", "T")) // Verzeker de juiste conversie naar datetime
})

// Koppel rit aan start en eind station, maar alleen als de stations niet null zijn
WITH ride, startStation, endStation, vehicle, user
// Koppel rit aan start station, als het station bestaat
FOREACH (_ IN CASE WHEN startStation IS NOT NULL THEN [1] ELSE [] END |
MERGE (ride)-[:STARTS_AT]->(startStation)
)
// Koppel rit aan eind station, als het station bestaat
FOREACH (_ IN CASE WHEN endStation IS NOT NULL THEN [1] ELSE [] END |
MERGE (ride)-[:ENDS_AT]->(endStation)
)

// Koppel rit aan voertuig, als het voertuig bestaat
MERGE (ride)-[:USES]->(vehicle)

// Koppel rit aan gebruiker
MERGE (ride)-[:INITIATED_BY]->(user)
```

## Antwoorden op de verschillende queries:
### Wat zijn de meest gebruikte voertuigen?
Om een antwoord op deze query te kunnen voorzien, moeten we een relatie kunnen leggen tussen Voertuigen en Ritten. Dat kunnen we doen aan de hand van de USES relatie die we daarnet aangemaakt hebben:
```cypher
MATCH (v:Vehicle)<-[:USES]-(r:Ride)
RETURN v.id AS VehicleId, COUNT(r) AS UsageCount
ORDER BY UsageCount DESC
LIMIT 10;

```

### Identificeer voor een station en uur naar welke buurt fietsers voornamelijk rijden.
```cypher
MATCH (startStation:Station {id: 'station_id'})<-[:STARTS_AT]-(r:Ride)-[:ENDS_AT]->(endStation:Station)
MATCH (endStation)-[:LOCATED_IN]->(district:District)
WHERE r.startTime >= datetime('YYYY-MM-DDTHH:MM:00') AND r.startTime < datetime('YYYY-MM-DDTHH:MM:59')
RETURN district.name AS District, COUNT(r) AS RideCount
ORDER BY RideCount DESC
LIMIT 5;

```

### Welke buurten zijn het sterkst met elkaar verbonden?
```cypher
MATCH (startStation)-[:LOCATED_IN]->(startDistrict:District)
MATCH (endStation)-[:LOCATED_IN]->(endDistrict:District)
MATCH (r:Ride)-[:STARTS_AT]->(startStation)
MATCH (r)-[:ENDS_AT]->(endStation)
RETURN startDistrict.name AS StartDistrict, endDistrict.name AS EndDistrict, COUNT(r) AS RideCount
ORDER BY RideCount DESC
LIMIT 10;
```

### Eigen query: hoe kan je het kortste pad vinden tussen verschillende stations?
```cypher
MATCH (startStation:Station), (endStation:Station)
WHERE startStation <> endStation
WITH startStation, endStation
MATCH p = shortestPath((startStation)-[:STARTS_AT|ENDS_AT*]-(endStation))
RETURN startStation.id AS StartStation, endStation.id AS EndStation, length(p) AS PathLength
ORDER BY PathLength ASC
LIMIT 10;
```