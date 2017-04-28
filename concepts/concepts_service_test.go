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
	"sort"
)

//all uuids to be cleaned from DB
const (
	contentUUID             = "3fc9fe3e-af8c-4f7f-961a-e5065392bb31"
	basicConceptUUID        = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	newBasicConceptUUID     = "122333333"
)

//Reusable Neo4J connection
var db neoutils.NeoConnection

//Concept Service under test
var conceptsDriver service

func BasicConcept() Concept {
	basicConcept := Concept{
		UUID:           basicConceptUUID,
		PrefLabel:      "basic concept label",
		Type:           "Section",
		Authority:      "TME",
		AuthorityValue: "1234",
	}
	return basicConcept
}

func BasicAggregatedConcept() AggregatedConcept {
	basicConcept := BasicConcept()
	return AggregatedConcept{
		UUID:      basicConcept.UUID,
		PrefLabel: basicConcept.PrefLabel,
		Type:      "Section",
		SourceRepresentations: []Concept{basicConcept},
	}
}

func AnotherBasicConcept() Concept {
	return Concept{
		UUID:           anotherBasicConceptUUID,
		PrefLabel:      "another basic concept label",
		Type:           "Section",
		Authority:      "TME",
		AuthorityValue: "987456321",
	}
}

func AnotherBasicAggregatedConcept() AggregatedConcept {
	anotherBasicConcept := AnotherBasicConcept()
	return AggregatedConcept{
		UUID:      anotherBasicConcept.UUID,
		PrefLabel: anotherBasicConcept.PrefLabel,
		Type:      "Section",
		SourceRepresentations: []Concept{anotherBasicConcept},
	}
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first
	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, _ = neoutils.Connect(neoUrl(), conf)
	if db == nil {
		panic("Cannot connect to Neo4J")
	}
	conceptsDriver = NewConceptService(db)
	conceptsDriver.Initialise()

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
	cleanDB(assert)

	basicAggregatedConcept := BasicAggregatedConcept()

	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")
	readConceptAndCompare(basicAggregatedConcept, assert)
}

//func TestMultipleConceptsAreCreatedForMultipleSourceRepresentations(t *testing.T) {
//	assert := assert.New(t)
//      cleanDB(assert)
//
//	fmt.Printf("\n %v \n", aggregatedConceptWithMultipleConcepts)
//	assert.NoError(conceptsDriver.Write(aggregatedConceptWithMultipleConcepts), "Failed to write concept")
//
//	readConceptAndCompare(aggregatedConceptWithMultipleConcepts, assert)
//}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	basicAggregatedConcept := BasicAggregatedConcept()

	basicConceptToWrite := BasicConcept()
	basicConceptToWrite.PrefLabel = "Herr Ümlaut und Frau Groß"

	basicAggregatedConceptToWrite := basicAggregatedConcept
	basicAggregatedConceptToWrite.PrefLabel = "Herr Ümlaut und Frau Groß"
	basicAggregatedConceptToWrite.SourceRepresentations = []Concept{basicConceptToWrite}

	assert.NoError(conceptsDriver.Write(basicAggregatedConceptToWrite), "Failed to write concept")

	readConceptAndCompare(basicAggregatedConceptToWrite, assert)
}

func TestAddingConceptWithExistingIdentifiersShouldFail(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	newBasicAggConcept := BasicAggregatedConcept()

	alternateBasicConcept := Concept{
		UUID:           anotherBasicConceptUUID,
		PrefLabel:      "basic concept label",
		Type:           "Section",
		Authority:      "TME",
		AuthorityValue: "1234",
	}

	alternateAggConcept := BasicAggregatedConcept()
	alternateAggConcept.SourceRepresentations = []Concept{alternateBasicConcept}

	assert.NoError(conceptsDriver.Write(newBasicAggConcept))
	err := conceptsDriver.Write(alternateAggConcept)
	assert.Error(err)
	assert.IsType(rwapi.ConstraintOrTransactionError{}, err)
}

func TestIdentifierNodesCreatedForBasicConcept(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	conceptsDriver := NewConceptService(db)

	basicAggregatedConcept := BasicAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	actualValue := getIdentifierValue(assert, basicAggregatedConcept.UUID, "UPPIdentifier")
	assert.Equal(basicAggregatedConcept.SourceRepresentations[0].UUID, actualValue)

	actualValue = getIdentifierValue(assert, basicAggregatedConcept.UUID, "TMEIdentifier")
	assert.Equal(basicAggregatedConcept.SourceRepresentations[0].AuthorityValue, actualValue)
}

func TestIdentifierNodesUpdatedForBasicConcept(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	bac := BasicAggregatedConcept()
	assert.NoError(conceptsDriver.Write(bac), "Failed to write concept")

	expectedNewTMEIdentifierValue := "UpdatedAuthorityValue"
	bac.SourceRepresentations[0].AuthorityValue = expectedNewTMEIdentifierValue

	assert.NoError(conceptsDriver.Write(bac), "Failed to write concept")

	actualValue := getIdentifierValue(assert, bac.UUID, "TMEIdentifier")
	assert.Equal(expectedNewTMEIdentifierValue, actualValue, "Failed to unpdate TMEIdentifier value")
}

func TestUnknownAuthorityGivesError(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	newBasicAggConcept := BasicAggregatedConcept()

	newBasicConcept := BasicConcept()
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
	cleanDB(assert)

	basicAggregatedConcept := BasicAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	found, err := conceptsDriver.Delete(basicAggregatedConcept.UUID)
	assert.True(found, "Didn't manage to delete concept for uuid %", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error deleting concept for uuid %s", basicAggregatedConcept)

	concept, found, err := conceptsDriver.Read(basicAggregatedConcept.UUID)

	assert.Equal(AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(found, "Found concept for uuid %s who should have been deleted", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error trying to find concept for uuid %s", basicAggregatedConcept.UUID)
	assert.Equal(false, doesThingExistAtAll(basicAggregatedConcept.UUID, assert), "Found thing who should have been deleted uuid: %s", basicAggregatedConcept.UUID)
}

func TestDeleteWithRelationshipsMaintainsRelationshipsButDumbsDownToThing(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	basicAggregatedConcept := BasicAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	writeContent(assert)
	writeAnnotation(assert)

	found, err := conceptsDriver.Delete(basicAggregatedConcept.UUID)

	assert.True(found, "Didn't manage to delete concept for uuid %", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error deleting concept for uuid %s", basicAggregatedConcept.UUID)

	concept, found, err := conceptsDriver.Read(basicAggregatedConcept.UUID)

	assert.Equal(AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(found, "Found concept for uuid %s who should have been deleted", basicAggregatedConcept.UUID)
	assert.NoError(err, "Error trying to find concept for uuid %s", basicAggregatedConcept.UUID)
	assert.Equal(true, doesThingExistWithIdentifiers(basicAggregatedConcept.UUID, assert), "Unable to find a Thing with any Identifiers, uuid: %s", basicAggregatedConcept.UUID)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	basicAggregatedConcept := BasicAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	nr, err := conceptsDriver.Count()
	assert.Equal(1, nr, "Should be 1 concept in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	assert.NoError(conceptsDriver.Write(AnotherBasicAggregatedConcept()), "Failed to write concept")

	nr, err = conceptsDriver.Count()
	assert.Equal(2, nr, "Should be 2 subjects in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")
}

func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	assert := assert.New(t)
	cleanDB(assert)

	anotherObj := BasicAggregatedConcept()

	anotherObj.PrefLabel = ""
	err := conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no prefLabel has been supplied for: %s", anotherObj.UUID))

	anotherObj.PrefLabel = "Pref Label"
	anotherObj.Type = ""
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no type has been supplied for: %s", anotherObj.UUID))

	anotherObj.Type = "Type"
	anotherObj.SourceRepresentations = nil
	err = conceptsDriver.Write(anotherObj)
	assert.Error(err)
	assert.IsType(requestError{}, err)
	assert.EqualError(err, "Invalid Request")
	assert.Equal(err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation has been supplied for: %s", anotherObj.UUID))

	basicConcept := BasicConcept()
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

func readConceptAndCompare(expected AggregatedConcept, assert *assert.Assertions) {
	actual, found, err := conceptsDriver.Read(expected.UUID)
	actualConcept := actual.(AggregatedConcept)
	sort.Slice(expected.SourceRepresentations, func(i, j int) bool {
		return expected.SourceRepresentations[i].UUID < expected.SourceRepresentations[j].UUID
	})

	sort.Slice(actualConcept.SourceRepresentations, func(i, j int) bool {
		return actualConcept.SourceRepresentations[i].UUID < actualConcept.SourceRepresentations[j].UUID
	})

	assert.NoError(err)
	assert.True(found)
	assert.EqualValues(expected, actualConcept)
}

func neoUrl() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}
	return url
}

func getConceptService(t *testing.T) service {
	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(neoUrl(), conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewConceptService(db)
	service.Initialise()
	return service
}

func cleanDB(assert *assert.Assertions) {
	uuidsToClean := []string{basicConceptUUID, anotherBasicConceptUUID, newBasicConceptUUID, contentUUID}
	qs := make([]*neoism.CypherQuery, len(uuidsToClean))
	for i, uuid := range uuidsToClean {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			DELETE rel, i
			DETACH DELETE i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func doesThingExistAtAll(uuid string, assert *assert.Assertions) bool {
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

func doesThingExistWithIdentifiers(uuid string, assert *assert.Assertions) bool {
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

func writeAnnotation(assert *assert.Assertions) annotations.Service {
	annotationsRW := annotations.NewCypherAnnotationsService(db, "v1", "annotations-v1")
	assert.NoError(annotationsRW.Initialise())
	writeJSONToAnnotationsService(annotationsRW, contentUUID, "./fixtures/Annotations-3fc9fe3e-af8c-4f7f-961a-e5065392bb31-v2.json", assert)
	return annotationsRW
}

func writeContent(assert *assert.Assertions) baseftrwapp.Service {
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

func getIdentifierValue(assert *assert.Assertions, uuid string, label string) string {
	results := []struct {
		Value string `json:"i.value"`
	}{}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			match (c:Concept {uuid :{uuid}})-[r:IDENTIFIES]-(i:%s) return i.value
		`, label),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := db.CypherBatch([]*neoism.CypherQuery{query})
	assert.NoError(err, fmt.Sprintf("Error while retrieving %s", label))
	return results[0].Value
}
