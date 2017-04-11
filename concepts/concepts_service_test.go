package concepts

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"time"

	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/content-rw-neo4j/content"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

const (
	contentUUID = "3fc9fe3e-af8c-4f7f-961a-e5065392bb31"
)

var basicConcept = Concept{
	UUID:           "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
	PrefLabel:      "basic concept label",
	Type:           "Section",
	Authority:      "TME",
	AuthorityValue: "1234",
}

var basicAggregatedConcept = AggregatedConcept{
	UUID:      basicConcept.UUID,
	PrefLabel: basicConcept.PrefLabel,

	SourceRepresentations: []Concept{basicConcept},
}

var anotherBasicConcept = Concept{
	UUID:           "4c41f314-4548-4fb6-ac48-4618fcbfa84c",
	PrefLabel:      "another basic concept label",
	Type:           "Section",
	Authority:      "TME",
	AuthorityValue: "987456321",
}

var anotherBasicAggregatedConcept = AggregatedConcept{
	UUID:      anotherBasicConcept.UUID,
	PrefLabel: anotherBasicConcept.PrefLabel,

	SourceRepresentations: []Concept{anotherBasicConcept},
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first

	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, _ := neoutils.Connect(url, conf)
	service := NewConceptService(db)
	service.Initialise()

	duration := 5 * time.Second
	time.Sleep(duration)
}

func TestConnectivityCheck(t *testing.T) {
	conceptsDriver := getConceptService(t)
	err := conceptsDriver.Check()
	assert.NoError(t, err, "Unexpected error on connectivity check")
}

func TestCreateAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID}, db, t, assert)
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")
	readConceptAndCompare(basicAggregatedConcept, t, db)
}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID}, db, t, assert)

	basicConceptToWrite := basicConcept
	basicConceptToWrite.PrefLabel = "Herr Ümlaut und Frau Groß"

	basicAggregatedConceptToWrite := basicAggregatedConcept
	basicAggregatedConceptToWrite.PrefLabel = "Herr Ümlaut und Frau Groß"
	basicAggregatedConceptToWrite.SourceRepresentations = []Concept{basicConceptToWrite}

	assert.NoError(conceptsDriver.Write(basicAggregatedConceptToWrite), "Failed to write concept")

	readConceptAndCompare(basicAggregatedConceptToWrite, t, db)
}

func TestAddingConceptWithExistingIdentifiersShouldFail(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	newBasicAggConcept := basicAggregatedConcept

	newBasicConcept := basicConcept
	newBasicConcept.UUID = "122333333"
	newBasicAggConcept.UUID = newBasicConcept.UUID
	newBasicAggConcept.SourceRepresentations = []Concept{newBasicConcept}

	defer cleanDB([]string{basicConcept.UUID, newBasicConcept.UUID}, db, t, assert)

	assert.NoError(conceptsDriver.Write(basicAggregatedConcept))
	err := conceptsDriver.Write(newBasicAggConcept)
	assert.Error(err)
	assert.IsType(rwapi.ConstraintOrTransactionError{}, err)
}

func TestUnknownAuthorityGivesError(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID}, db, t, assert)

	newBasicAggConcept := basicAggregatedConcept

	newBasicConcept := basicConcept
	newBasicConcept.Authority = "Nicky"
	newBasicAggConcept.SourceRepresentations = []Concept{newBasicConcept}

	err := conceptsDriver.Write(newBasicAggConcept)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Unknown authority, therefore unable to add the relevant Identifier node: %s", newBasicConcept.Authority))
}

func TestDeleteWillDeleteEntireNodeIfNoRelationship(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID, contentUUID}, db, t, assert)

	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	found, err := conceptsDriver.Delete(basicAggregatedConcept.UUID)
	assert.True(found, "Didn't manage to delete concept for uuid %", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error deleting concept for uuid %s", basicAggregatedConcept)

	concept, found, err := conceptsDriver.Read(basicAggregatedConcept.UUID)

	assert.Equal(AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(found, "Found concept for uuid %s who should have been deleted", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error trying to find concept for uuid %s", basicAggregatedConcept.UUID)
	assert.Equal(false, doesThingExistAtAll(basicAggregatedConcept.UUID, db, t, assert), "Found thing who should have been deleted uuid: %s", basicAggregatedConcept.UUID)
}

func TestDeleteWithRelationshipsMaintainsRelationshipsButDumbsDownToThing(t *testing.T) {
	assert := assert.New(t)

	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID, contentUUID}, db, t, assert)

	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	writeContent(assert, db)
	writeAnnotation(assert, db)

	found, err := conceptsDriver.Delete(basicAggregatedConcept.UUID)

	assert.True(found, "Didn't manage to delete concept for uuid %", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error deleting concept for uuid %s", basicAggregatedConcept.UUID)

	concept, found, err := conceptsDriver.Read(basicAggregatedConcept.UUID)

	assert.Equal(AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(found, "Found concept for uuid %s who should have been deleted", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error trying to find concept for uuid %s", basicAggregatedConcept.UUID)
	assert.Equal(true, doesThingExistWithIdentifiers(basicAggregatedConcept.UUID, db, t, assert), "Unable to find a Thing with any Identifiers, uuid: %s", basicAggregatedConcept.UUID)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)

	defer cleanDB([]string{basicConcept.UUID, anotherBasicConcept.UUID}, db, t, assert)
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	nr, err := conceptsDriver.Count()
	assert.Equal(1, nr, "Should be 1 concept in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	assert.NoError(conceptsDriver.Write(anotherBasicAggregatedConcept), "Failed to write concept")

	nr, err = conceptsDriver.Count()
	assert.Equal(2, nr, "Should be 2 subjects in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")
}

func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	conceptsDriver := NewConceptService(db)
	defer cleanDB([]string{basicConcept.UUID}, db, t, assert)

	anotherObj := basicAggregatedConcept

	anotherObj.PrefLabel = ""
	err := conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no prefLabel has been supplied for: %s", anotherObj.UUID))

	anotherObj.PrefLabel = "Pref Label"
	anotherObj.SourceRepresentations = nil
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation has been supplied for: %s", anotherObj.UUID))

	yetAnotherBasicConcept := basicConcept
	yetAnotherBasicConcept.PrefLabel = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.prefLabel has been supplied for: %s", anotherObj.UUID))

	yetAnotherBasicConcept = basicConcept
	yetAnotherBasicConcept.Type = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.type has been supplied for: %s", anotherObj.UUID))

	yetAnotherBasicConcept = basicConcept
	yetAnotherBasicConcept.AuthorityValue = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.authorityValue has been supplied for: %s", anotherObj.UUID))

	yetAnotherBasicConcept = basicConcept
	yetAnotherBasicConcept.Type = "TEST_TYPE"
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("The source representation of uuid: %s has an unknown type of: %s", yetAnotherBasicConcept.UUID, yetAnotherBasicConcept.Type))


}

func readConceptAndCompare(expected AggregatedConcept, t *testing.T, db neoutils.NeoConnection) {
	assert := assert.New(t)
	conceptsDriver := NewConceptService(db)

	actual, found, err := conceptsDriver.Read(expected.UUID)

	assert.NoError(err)
	assert.True(found)
	actualConcept := actual.(AggregatedConcept)
	assert.EqualValues(expected, actualConcept)
}

func getDatabaseConnectionAndCheckClean(t *testing.T, assert *assert.Assertions) neoutils.NeoConnection {
	db := getDatabaseConnection(assert)
	checkDbClean([]string{basicAggregatedConcept.UUID, basicConcept.UUID}, db, t)
	return db
}

func getDatabaseConnection(assert *assert.Assertions) neoutils.NeoConnection {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(err, "Failed to connect to Neo4j")
	return db
}

func getConceptService(t *testing.T) service {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewConceptService(db)
	service.Initialise()
	return service
}

func checkDbClean(uuidsCleaned []string, db neoutils.NeoConnection, t *testing.T) {
	assert := assert.New(t)

	result := []struct {
		Uuid string `json:"thing.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (thing) WHERE thing.uuid in {uuids} RETURN thing.uuid
		`,
		Parameters: neoism.Props{
			"uuids": uuidsCleaned,
		},
		Result: &result,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.Empty(result)
}

func cleanDB(uuidsToClean []string, db neoutils.NeoConnection, t *testing.T, assert *assert.Assertions) {
	qs := make([]*neoism.CypherQuery, len(uuidsToClean))
	for i, uuid := range uuidsToClean {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel]-(i)
			DELETE rel, i
			DETACH DELETE a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func doesThingExistAtAll(uuid string, db neoutils.NeoConnection, t *testing.T, assert *assert.Assertions) bool {
	result := []struct {
		Uuid string `json:"thing.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: "%s"}) return a.uuid
		`,
		Parameters: neoism.Props{
			"uuid": uuid,
		},
		Result: &result,
	}

	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)

	if len(result) == 0 {
		return false
	}

	return true
}

func doesThingExistWithIdentifiers(uuid string, db neoutils.NeoConnection, t *testing.T, assert *assert.Assertions) bool {
	result := []struct {
		uuid string
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: "%s"})-[:IDENTIFIES]-(:Identifier)
			WITH collect(distinct a.uuid) as uuid
			RETURN uuid
		`,
		Parameters: neoism.Props{
			"uuid": uuid,
		},
		Result: &result,
	}

	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)

	if len(result) == 0 {
		return false
	}

	return true
}

func writeAnnotation(assert *assert.Assertions, db neoutils.NeoConnection) annotations.Service {
	annotationsRW := annotations.NewCypherAnnotationsService(db, "v1", "annotations-v1")
	assert.NoError(annotationsRW.Initialise())
	writeJSONToAnnotationsService(annotationsRW, contentUUID, "./fixtures/Annotations-3fc9fe3e-af8c-4f7f-961a-e5065392bb31-v2.json", assert)
	return annotationsRW
}

func writeContent(assert *assert.Assertions, db neoutils.NeoConnection) baseftrwapp.Service {
	contentRW := content.NewCypherContentService(db)
	assert.NoError(contentRW.Initialise())
	writeJSONToService(contentRW, "./fixtures/Content-3fc9fe3e-af8c-4f7f-961a-e5065392bb31.json", assert)
	return contentRW
}

func writeJSONToAnnotationsService(service annotations.Service, contentUUID string, pathToJSONFile string, assert *assert.Assertions) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(err)
	dec := json.NewDecoder(f)
	inst, errr := service.DecodeJSON(dec)
	assert.NoError(errr, "Error parsing file %s", pathToJSONFile)
	errrr := service.Write(contentUUID, inst)
	assert.NoError(errrr)
}

func writeJSONToService(service baseftrwapp.Service, pathToJSONFile string, assert *assert.Assertions) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(err)
	dec := json.NewDecoder(f)
	inst, _, errr := service.DecodeJSON(dec)
	assert.NoError(errr)
	errrr := service.Write(inst)
	assert.NoError(errrr)
}
