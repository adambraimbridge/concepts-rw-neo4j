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

`$GOPATH/bin/concepts-rw-neo4j --neo-url={neo4jUrl} --port={port} --batchSize=50 --graphiteTCPAddress=graphite.ft.com:2003 --graphitePrefix=content.{env}c.concepts.rw.neo4j.{hostname} --logMetrics=false --logLevel=Info --requestLoggingOn=true

All arguments are optional, they default to a local Neo4j install on the default port (7474), application running on port 8080, batchSize of 1024, graphiteTCPAddress of "" (meaning metrics won't be written to Graphite), graphitePrefix of "" and logMetrics false.

## Deployment

This deploys to our delivery cluster via the normal mechanism https://sites.google.com/a/ft.com/technology/systems/dynamic-semantic-publishing/coco/deploy-process

## Service Endpoints

Taxonomy refers to the concept type. Phase 1 is only the simplest TME taxonomies of Genres, Topics, Subjects, SpecialReports, Locations, Sections

Coming soon more complex taxonomies including concorded people and orgs

### PUT /{taxonomy}/{uuid}

The mandatory fields are the prefUUID, prefLabel, type, and sourceRepresentations. Inside each sourceRepresentation uuid, prefLabel, type, authority and authorityValue. 
Failure to provide mandatory fields will return 400 bad request.

Every request results in an attempt to update that concept

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

A successful PUT results in 200 and returns a json response of all updated uuids.

Example response from above request:

    `{
         "UpdatedIds": [
             "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
         ]
     }`

The type field is not currently validated against the path

"TME", "UPP" and "Smartlogic" are the only valid authorities, any other Authority will result in a 400 bad request response.

Invalid JSON body input or UUIDs that don't match between the path and the body will result in a 400 bad request response.

### GET /{taxonomy}/{uuid}
The internal read should return what got written 

If not found, you'll get a 404 response.

Empty fields are omitted from the response.
`curl -H "X-Request-Id: 123" localhost:8080/sections/3fa70485-3a57-3b9b-9449-774b001cd965`

### Admin endpoints
Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)
Good to Go: [http://localhost:8080/__gtg](http://localhost:8080/__gtg)
Build-Info: [http://localhost:8080/build-info](http://localhost:8080/build-info)

### Logging
This application uses logrus, the logfile is initialised in main.go and is configurable on runtime parameters

- logLevel run parameter can be set to debug for more detailed logging
- requestLoggingOn can be set to false to stop logging of http requests

