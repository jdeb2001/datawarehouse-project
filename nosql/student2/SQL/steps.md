# Project VeloDB Neo4J
Bij dit project hoort nog een aparte PDF/Word document met iets diepere documentatie van wat ik gedaan heb. In deze markdown allesinds kan u de basis vinden van wat ik exact gedaan heb op dit moment.
## Startdata
Eerst importeren we onze datasets die we uit onze Postgres databank gehaald hebben. In mijn geval heb ik dit met 3 aparte .csv bestanden gedaan voor Stations, Ritten, en Gebruikers.
```cypher
LOAD CSV WITH HEADERS FROM 'file:///stations.csv' AS row
MERGE (s:Station {stationid: row.stationid, name: row.name})
MERGE (b:Buurt {name: row.district})
MERGE (s)-[:BEVINDT_ZICH_IN]->(b);
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///ritten.csv' AS row
MERGE (r:Rit {rideid: row.rideid})
SET r.starttime = row.starttime, 
    r.endtime = row.endtime
WITH row, r
MATCH (start:Station {stationid: row.startlockid})  // Correct kolom
MERGE (r)-[:STARTS_AT]->(start)
WITH row, r
MATCH (end:Station {stationid: row.endlockid})  // Correct kolom
MERGE (r)-[:ENDS_AT]->(end);
```


```cypher
LOAD CSV WITH HEADERS FROM 'file:///ritten.csv' AS row
MERGE (v:Voertuig {vehicleid: row.vehicleid})
WITH row, v
MATCH (r:Rit {rideid: row.rideid})
MERGE (r)-[:USES]->(v);
```

```cypher
LOAD CSV WITH HEADERS FROM 'file:///gebruikers.csv' AS row
MERGE (g:Gebruiker {userid: row.userid, name: row.name})
WITH row, g
MATCH (r:Rit {rideid: row.rideid})
MERGE (r)-[:PERFORMED_BY]->(g);
```

## Antwoorden op de verschillende queries:
### Wat zijn de meest gebruikte voertuigen?
Om een antwoord op deze query te kunnen voorzien, moeten we een relatie kunnen leggen tussen Voertuigen en Ritten. Dat kunnen we doen aan de hand van de USES relatie die we daarnet aangemaakt hebben:
```cypher
MATCH (v:Voertuig)<-[:USES]-(r:Rit)
RETURN v.vehicleid, COUNT(r) AS ritten
ORDER BY ritten DESC
LIMIT 5;
```

### Identificeer voor een station en uur naar welke buurt fietsers voornamelijk rijden.
```cypher
MATCH (b1:Buurt)<-[:BEVINDT_ZICH_IN]-(s1:Station)<-[:ENDS_AT]-(r:Rit)-[:STARTS_AT]->(s2:Station)-[:BEVINDT_ZICH_IN]->(b2:Buurt)
RETURN b1.name AS buurt1, b2.name AS buurt2, COUNT(r) AS verbindingen
ORDER BY verbindingen DESC
LIMIT 5;
```

### Welke buurten zijn het sterkst met elkaar verbonden?
```cypher
MATCH (b1:Buurt)<-[:BEVINDT_ZICH_IN]-(s1:Station)<-[:ENDS_AT]-(r:Rit)-[:STARTS_AT]->(s2:Station)-[:BEVINDT_ZICH_IN]->(b2:Buurt)
RETURN b1.name AS buurt1, b2.name AS buurt2, COUNT(r) AS verbindingen
ORDER BY verbindingen DESC
LIMIT 5;
```

### Welke buurten hebben vergelijkbare start- en eindpatronen?
```cypher
MATCH (b1:Buurt)<-[:BEVINDT_ZICH_IN]-(s1:Station)<-[:ENDS_AT]-(r:Rit)-[:STARTS_AT]->(s2:Station)-[:BEVINDT_ZICH_IN]->(b2:Buurt)
WITH b1, b2, COUNT(r) AS verbindingen
WHERE b1 <> b2
RETURN b1.name, b2.name, verbindingen
ORDER BY verbindingen DESC;
```