# Project VeloDB Neo4J
Bij dit project hoort nog een apart Word document met iets diepgaandere documentatie van wat ik hier gedaan heb. In deze markdown staan enkel de scripts die ik heb uitgevoerd om tot mijn resultaten te komen, dus is het beter om de documentatie zelf te bekijken voor uitgebreidere informatie.
## Startdata
Eerst importeren we onze datasets die we uit onze Postgres databank gehaald hebben. In mijn geval heb ik dit met 5 aparte .csv bestanden gedaan voor Stations, Rides, Locks, Vehicles en Users.

### Stations
```cypher
LOAD CSV WITH HEADERS FROM 'file:///stations.csv' AS row
MERGE (d:District {name: row.district})
MERGE (s:Station {stationId: row.stationid})
ON CREATE SET s.type = row.type,
s.street = row.street,
s.number = row.number,
s.zipcode = row.zipcode,
s.gpsCoord = row.gpscoord,
s.additionalInfo = row.additionalinfo
MERGE (s)-[:LOCATED_IN]->(d);
```


### Locks
```cypher
LOAD CSV WITH HEADERS FROM 'file:///locks.csv' AS row
MERGE (s:Station {stationId: row.stationid})
MERGE (l:Lock {lockId: row.lockid})
ON CREATE SET l.stationLockNr = row.stationlocknr
MERGE (l)-[:LOCATED_IN]->(s)
WITH l, row
FOREACH (_ IN CASE WHEN row.vehicleid IS NOT NULL AND row.vehicleid <> "" THEN [1] ELSE [] END |
MERGE (v:Vehicle {vehicleId: row.vehicleid})
MERGE (l)-[:HOLDS]->(v)
);
```

### Vehicles 
```cypher
LOAD CSV WITH HEADERS FROM 'file:///vehicles.csv' AS row
MERGE (v:Vehicle {vehicleId: row.vehicleid})
ON CREATE SET v.serialNumber = row.serialnumber,
v.type = row.vehicletype,
v.city = row.city,
v.lastMaintenance = datetime(replace(row.lastmaintenanceon, " ", "T"));
```

### Users
```cypher
CREATE CONSTRAINT user_id_unique IF NOT EXISTS
FOR (u:User) REQUIRE u.id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///users.csv' AS row
MERGE (u:User {id: row.userid})
ON CREATE SET u.email = row.email,
u.street = row.street,
u.number = row.number,
u.zipcode = row.zipcode,
u.city = row.city,
u.countryCode = row.country_code;
```

### Rides
```cypher
LOAD CSV WITH HEADERS FROM 'file:///rides.csv' AS row
OPTIONAL MATCH (startLock:Lock {lockId: row.startlockid})-[:LOCATED_IN]->(startStation:Station)
OPTIONAL MATCH (endLock:Lock {lockId: row.endlockid})-[:LOCATED_IN]->(endStation:Station)
OPTIONAL MATCH (v:Vehicle {vehicleId: row.vehicleid})
OPTIONAL MATCH (u:User {id: row.userid})

// Maak de Ride node aan
CREATE (r:Ride {
rideId: row.rideid,
startTime: datetime(replace(row.starttime, " ", "T")),
endTime: datetime(replace(row.endtime, " ", "T"))
})
WITH r, startLock, endLock, v, u
FOREACH (_ IN CASE WHEN startLock IS NOT NULL THEN [1] ELSE [] END |
MERGE (r)-[:STARTS_AT]->(startLock)
)
FOREACH (_ IN CASE WHEN endLock IS NOT NULL THEN [1] ELSE [] END |
MERGE (r)-[:ENDS_AT]->(endLock)
)
FOREACH (_ IN CASE WHEN v IS NOT NULL THEN [1] ELSE [] END |
MERGE (r)-[:USES]->(v)
)
FOREACH (_ IN CASE WHEN u IS NOT NULL THEN [1] ELSE [] END |
MERGE (r)-[:INITIATED_BY]->(u)
);

```

## Antwoorden op de verschillende queries:
### Wat zijn de meest gebruikte voertuigen?
Om een antwoord op deze query te kunnen voorzien, moeten we een relatie kunnen leggen tussen Voertuigen en Ritten. Dat kunnen we doen aan de hand van de USES relatie die we daarnet aangemaakt hebben:
```cypher
MATCH (v:Vehicle)<-[:USES]-(r:Ride)
RETURN v.vehicleId AS VehicleID, COUNT(r) AS RideCount
ORDER BY RideCount DESC
LIMIT 5;
```

### Identificeer voor een station en uur naar welke buurt fietsers voornamelijk rijden.
In dit geval kies ik voor een station met stationId 1 en filteren we tussen 8u en 9u:
```cypher
MATCH (s:Station {stationId: "1"})<-[:LOCATED_IN]-(lStart:Lock)<-[:STARTS_AT]-(r:Ride)-[:ENDS_AT]->(lEnd:Lock)-[:LOCATED_IN]->(endStation:Station)-[:LOCATED_IN]->(d:District)
WHERE time(r.startTime) >= time("08:00:00") AND time(r.startTime) < time("09:00:00")
RETURN d.name AS DestinationDistrict, COUNT(r) AS RideCount
ORDER BY RideCount DESC;
```

### Welke buurten zijn het sterkst met elkaar verbonden?
```cypher
MATCH (startDistrict:District)<-[:LOCATED_IN]-(:Station)<-[:LOCATED_IN]-(lStart:Lock)<-[:STARTS_AT]-(r:Ride)-[:ENDS_AT]->(lEnd:Lock)-[:LOCATED_IN]->(:Station)-[:LOCATED_IN]->(endDistrict:District)
WHERE startDistrict.name <> endDistrict.name
RETURN startDistrict.name AS StartDistrict, endDistrict.name AS EndDistrict, COUNT(r) AS RideCount
ORDER BY RideCount DESC
LIMIT 10;
```

### Eigen query: vind de voertuigen die het langste stilstaan in een bepaald station na hun laatste rit
```cypher
// Vind de laatste rit van elk voertuig
MATCH (r:Ride)-[:ENDS_AT]->(lEnd:Lock)-[:LOCATED_IN]->(sEnd:Station),
(r)-[:USES]->(v:Vehicle)
WITH v.vehicleId AS VehicleID, 
MAX(r.endTime) AS LastRideTime, 
sEnd.stationId AS LastStation

// Controleer of het voertuig geen nieuwe rit gestart heeft sinds de laatste rit
OPTIONAL MATCH (v)<-[:USES]-(newRide:Ride)-[:STARTS_AT]->(lStart:Lock)
WHERE newRide.startTime > LastRideTime
WITH VehicleID, LastRideTime, LastStation, COUNT(newRide) AS NewRides
WHERE NewRides = 0 // Alleen voertuigen zonder nieuwe ritten

// Bereken hoe lang het voertuig stilstaat en formatteer de IdleTime
WITH VehicleID, LastStation, duration.between(LastRideTime, datetime()) AS IdleTime
RETURN VehicleID, 
LastStation AS StationID,
IdleTime.days AS DaysIdle, 
IdleTime.hours AS HoursIdle, 
IdleTime.minutes AS MinutesIdle
ORDER BY DaysIdle DESC, HoursIdle DESC, MinutesIdle DESC
LIMIT 10;
```