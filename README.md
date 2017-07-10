# Concept Reader/Writer for Neo4j (concept-rw-neo4j) 

[![Circle CI](https://circleci.com/gh/Financial-Times/concepts-rw-neo4j.svg?style=shield)](https://circleci.com/gh/Financial-Times/concepts-rw-neo4j)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/concepts-rw-neo4j)](https://goreportcard.com/report/github.com/Financial-Times/concepts-rw-neo4j)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/concepts-rw-neo4j/badge.svg)](https://coveralls.io/github/Financial-Times/concepts-rw-neo4j)

__An API for reading/writing concepts into Neo4j.__ 

## Installation

For the first time:

`go get github.com/Financial-Times/concepts-rw-neo4j`

or update:

`go get -u github.com/Financial-Times/concepts-rw-neo4j`

## Running

`$GOPATH/bin/concepts-rw-neo4j --neo-url={neo4jUrl} --port={port} --batchSize=50 --graphiteTCPAddress=graphite.ft.com:2003 --graphitePrefix=content.{env}c.concepts.rw.neo4j.{hostname} --logMetrics=false

All arguments are optional, they default to a local Neo4j install on the default port (7474), application running on port 8080, batchSize of 1024, graphiteTCPAddress of "" (meaning metrics won't be written to Graphite), graphitePrefix of "" and logMetrics false.

## Deployment

This deploys to our delivery cluster via the normal mechnism https://sites.google.com/a/ft.com/technology/systems/dynamic-semantic-publishing/coco/deploy-process

## Service Endpoints

Taxonomy refers to the concept type. Phase 1 is only the simplest TME taxonomies of Genres, Topics, Subjects, SpecialReports, Locations, Sections

Coming soon more complex taxonomies including concorded people and orgs

### PUT /{taxonomy}/{uuid}

The mandatory fields are the prefUUID, prefLabel, type, and sourceRepresentations. Inside each sourceRepresentation uuid, prefLabel, type, authority and authorityValue. 
Failure to provide mandatory fields will return 400 bad request.

Every request results in an attempt to update that concept

A successful PUT results in 200.

We run queries in batches. If a batch fails, all failing requests will get a 500 server error response.

Example PUT request:

    `curl -XPUT localhost:8080/sections/4c41f314-4548-4fb6-ac48-4618fcbfa84c \
         -H "X-Request-Id: 123" \
         -H "Content-Type: application/json" \
         -d '{
             	"prefUUID": "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
             	"prefLabel": "Some pref label",
             	"type": "Section",
             	"sourceRepresentations": [{
             		"uuid": "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
             		"prefLabel": "Some pref label",
             		"type": "Section",
             		"authority": "TME",
             		"authorityValue": "1234578fdh"
             	}]
             }'`

The type field is not currently validated against the path

"TME", "UPP" and "Smartlogic" are the only valid authorities, any other Authority will result in a 400 bad request response.

Invalid JSON body input or UUIDs that don't match between the path and the body will result in a 400 bad request response.

### GET /{taxonomy}/{uuid}
The internal read should return what got written 

If not found, you'll get a 404 response.

Empty fields are omitted from the response.
`curl -H "X-Request-Id: 123" localhost:8080/sections/3fa70485-3a57-3b9b-9449-774b001cd965`

### DELETE
Not currently supported

### COUNT
*This functionality is not currently correct and needs to return the specific taxonomy but at the moment it only returns the count of the number of concepts in Neo4J"
Will return the number of concepts in Neo4J 

### Admin endpoints
Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)
Ping: [http://localhost:8080/ping](http://localhost:8080/ping) or [http://localhost:8080/__ping](http://localhost:8080/__ping)
Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)
Build-Info: [http://localhost:8080/build-info](http://localhost:8080/build-info)

### Logging
 the application uses logrus, the logfile is initialised in main.go.
 logging requires an env app parameter, for all environments  other than local logs are written to file
 when running locally logging is written to console (if you want to log locally to file you need to pass in an env parameter that is != local)
 NOTE: build-info end point is not logged as it is called every second from varnish and this information is not needed in  logs/splunk

