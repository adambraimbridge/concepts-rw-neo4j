package concepts

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"time"

	"sort"

	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/content-rw-neo4j/content"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"

	"github.com/jmcvetta/neoism"

	"reflect"
)

//all uuids to be cleaned from DB
const (
	contentUUID             = "3fc9fe3e-af8c-4f7f-961a-e5065392bb31"
	basicConceptUUID        = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
)

//Reusable Neo4J connection
var db neoutils.NeoConnection

//Concept Service under test
var conceptsDriver Service

// A lone concept should always have matching pref labels and uuid at the src system level and the top level - We are
// currently missing validation around this
func getFullLoneAggregatedConcept() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "Concept PrefLabel",
		Type:           "Section",
		Strapline:      "Some strapline",
		DescriptionXML: "Some description",
		ImageURL:       "Some image url",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Strapline:      "Some strapline",
			DescriptionXML: "Some description",
			ImageURL:       "Some image url",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getAnotherFullLoneAggregatedConcept() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  anotherBasicConceptUUID,
		PrefLabel: "Concept PrefLabel",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           anotherBasicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "4321",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getFullConcordedAggregatedConcept() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "Concept PrefLabel",
		Type:           "Section",
		Strapline:      "Some strapline",
		DescriptionXML: "Some description",
		ImageURL:       "Some image url",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{{
			UUID:           anotherBasicConceptUUID,
			PrefLabel:      "Another Concept PrefLabel",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: anotherBasicConceptUUID,
			Strapline:      "Some strapline",
			DescriptionXML: "Some description",
			ImageURL:       "Some image url",
			Aliases:        []string{"anotheroneLabel", "anothersecondLabel"},
		}, {
			UUID:           basicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func updateLoneSourceSystemPrefLabel(prefLabel string) AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: prefLabel,
		Type:      "Section",
		Aliases:   []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      prefLabel,
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}}}
}

func getConcordedConceptWithConflictedIdentifier() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Concept PrefLabel",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           anotherBasicConceptUUID,
			PrefLabel:      "Another Concept PrefLabel",
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"anotheroneLabel", "anothersecondLabel"},
		}, {
			UUID:           basicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getUnknownAuthority() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Pref Label",
			Type:           "Section",
			Authority:      "BooHalloo",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}}}
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

func TestWriteService(t *testing.T) {
	//defer cleanDB(t, basicConceptUUID, anotherBasicConceptUUID)
	tests := []struct {
		testName          string
		aggregatedConcept AggregatedConcept
		errStr            string
	}{
		{"Creates All Values Present for a Lone Concept", getFullLoneAggregatedConcept(), ""},
		{"Creates All Values Present for a Concorded Concept", getFullConcordedAggregatedConcept(), ""},
		{"Creates Handles Special Characters", updateLoneSourceSystemPrefLabel("Herr Ümlaut und Frau Groß"), ""},
		{"Adding Concept with existing Identifiers fails", getConcordedConceptWithConflictedIdentifier(), "already exists with label TMEIdentifier and property \"value\"=[1234]"},
		{"Unknown Authority Should Fail", getUnknownAuthority(), "Invalid Request"},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			cleanDB(t, basicConceptUUID, anotherBasicConceptUUID)
			err := conceptsDriver.Write(test.aggregatedConcept)

			if test.errStr == "" {
				assert.NoError(t, err, "Failed to write concept")
				readConceptAndCompare(t, test.aggregatedConcept, test.testName)

				// Check lone nodes and leaf nodes for identifiers nodes
				// lone node
				if len(test.aggregatedConcept.SourceRepresentations) == 1 {

				} else {
					// Check leaf nodes for Identifiers
					for _, leaf := range test.aggregatedConcept.SourceRepresentations {
						actualValue := getIdentifierValue(t, "uuid", leaf.UUID, fmt.Sprintf("%vIdentifier", leaf.Authority))
						assert.Equal(t, leaf.AuthorityValue, actualValue, "Identifier value incorrect")
					}

					// Check Canonical node doesn't have a Identifier node
					actualValue := getIdentifierValue(t, "prefUUID", test.aggregatedConcept.PrefUUID, "UPPIdentifier")
					assert.Equal(t, "", actualValue, "Identifier nodes should not be related to Canonical Nodes")
				}

			} else {
				// TODO: Check these errors better
				assert.Error(t, err, "Error was expected")
				assert.Contains(t, err.Error(), test.errStr, "Error message is not correct")
			}
		})
	}
}

func TestDeleteWillDeleteEntireNodeIfNoRelationship(t *testing.T) {
	defer cleanDB(t, basicConceptUUID)
	basicAggregatedConcept := getFullLoneAggregatedConcept()
	assert.NoError(t, conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	found, err := conceptsDriver.Delete(basicConceptUUID)
	assert.True(t, found, "Didn't manage to delete concept for uuid %s", basicConceptUUID)
	assert.NoError(t, err, "Error deleting concept for uuid %s", basicAggregatedConcept)

	concept, found, err := conceptsDriver.Read(basicConceptUUID)

	assert.Equal(t, AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(t, found, "Found concept for uuid %s who should have been deleted", basicConceptUUID)
	assert.NoError(t, err, "Error trying to find concept for uuid %s", basicConceptUUID)
	assert.Equal(t, false, doesThingExistAtAll(t, basicConceptUUID), "Found thing who should have been deleted uuid: %s", basicConceptUUID)
}

func TestDeleteWithRelationshipsMaintainsRelationshipsButDumbsDownToThing(t *testing.T) {
	defer cleanDB(t, basicConceptUUID)
	basicAggregatedConcept := getFullLoneAggregatedConcept()
	assert.NoError(t, conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	writeContent(t)
	writeAnnotation(t)

	found, err := conceptsDriver.Delete(basicConceptUUID)

	assert.True(t, found, "Didn't manage to delete concept for uuid %", basicConceptUUID)
	assert.NoError(t, err, "Error deleting concept for uuid %s", basicConceptUUID)

	concept, found, err := conceptsDriver.Read(basicConceptUUID)

	assert.Equal(t, AggregatedConcept{}, concept, "Found concept %s who should have been deleted", concept)
	assert.False(t, found, "Found concept for uuid %s who should have been deleted", basicConceptUUID)
	assert.NoError(t, err, "Error trying to find concept for uuid %s", basicConceptUUID)
	assert.Equal(t, true, doesThingExistWithIdentifiers(t, basicConceptUUID), "Unable to find a Thing with any Identifiers, uuid: %s", basicConceptUUID)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	defer cleanDB(t, basicConceptUUID, anotherBasicConceptUUID)

	basicAggregatedConcept := getFullLoneAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept), "Failed to write concept")

	nr, err := conceptsDriver.Count()
	assert.Equal(1, nr, "Should be 1 concept in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	assert.NoError(conceptsDriver.Write(getAnotherFullLoneAggregatedConcept()), "Failed to write concept")

	nr, err = conceptsDriver.Count()
	assert.Equal(2, nr, "Should be 2 subjects in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")
}

// TODO do these tests in a loop
func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	defer cleanDB(t, basicConceptUUID)

	anotherObj := getFullLoneAggregatedConcept()

	anotherObj.PrefLabel = ""
	err := conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no prefLabel has been supplied for: %s", basicConceptUUID))

	anotherObj.PrefLabel = "Pref Label"
	anotherObj.Type = ""
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no type has been supplied for: %s", basicConceptUUID))

	anotherObj.Type = "Type"
	anotherObj.SourceRepresentations = nil
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation has been supplied for: %s", basicConceptUUID))

	yetAnotherBasicConcept := Concept{
		UUID:           basicConceptUUID,
		PrefLabel:      "Concept PrefLabel",
		Type:           "Section",
		Authority:      "TME",
		AuthorityValue: "1234",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
	}
	yetAnotherBasicConcept.PrefLabel = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.prefLabel has been supplied for: %s", yetAnotherBasicConcept.UUID))

	yetAnotherBasicConcept.PrefLabel = "Pref Label"
	yetAnotherBasicConcept.Type = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.type has been supplied for: %s", yetAnotherBasicConcept.UUID))

	yetAnotherBasicConcept.Type = "Section"
	yetAnotherBasicConcept.AuthorityValue = ""
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("Invalid request, no sourceRepresentation.authorityValue has been supplied for: %s", yetAnotherBasicConcept.UUID))

	yetAnotherBasicConcept.AuthorityValue = "UPP"
	yetAnotherBasicConcept.Type = "TEST_TYPE"
	anotherObj.SourceRepresentations = []Concept{yetAnotherBasicConcept}
	err = conceptsDriver.Write(anotherObj)
	assert.Error(t, err)
	assert.IsType(t, requestError{}, err)
	assert.EqualError(t, err, "Invalid Request")
	assert.Equal(t, err.(requestError).details, fmt.Sprintf("The source representation of uuid: %s has an unknown type of: %s", yetAnotherBasicConcept.UUID, yetAnotherBasicConcept.Type))
}

func readConceptAndCompare(t *testing.T, expected AggregatedConcept, testName string) {
	actual, found, err := conceptsDriver.Read(expected.PrefUUID)
	actualConcept := actual.(AggregatedConcept)
	sort.Slice(expected.SourceRepresentations, func(i, j int) bool {
		return expected.SourceRepresentations[i].UUID < expected.SourceRepresentations[j].UUID
	})

	sort.Slice(actualConcept.SourceRepresentations, func(i, j int) bool {
		return actualConcept.SourceRepresentations[i].UUID < actualConcept.SourceRepresentations[j].UUID
	})

	assert.NoError(t, err, "Unexpected Error occurred")
	assert.True(t, found, "Concept has not been found")

	assert.Equal(t, expected.PrefLabel, actualConcept.PrefLabel, "Actual aggregated concept pref label differs from expected")
	assert.Equal(t, expected.Type, actualConcept.Type, "Actual aggregated  concept type differs from expected")
	assert.Equal(t, expected.PrefUUID, actualConcept.PrefUUID, "Actual aggregated  concept uuid differs from expected")
	assert.Equal(t, expected.DescriptionXML, actualConcept.DescriptionXML, "Actual aggregated concept descriptionXML differs from expected")
	assert.Equal(t, expected.ImageURL, actualConcept.ImageURL, "Actual aggregated image url differs from expected")
	assert.Equal(t, expected.Strapline, actualConcept.Strapline, "Actual aggregated strapline differs from expected")

	if len(expected.SourceRepresentations) > 0 && len(actualConcept.SourceRepresentations) > 0 {
		var concepts []Concept
		for i, concept := range actualConcept.SourceRepresentations {
			assert.NotEqual(t, 0, concept.LastModifiedEpoch, "Actual concept lastModifiedEpoch differs from expected")

			// Remove the last modified date time now that we have checked it so we can compare the rest of the model
			concept.LastModifiedEpoch = 0
			concepts = append(concepts, concept)
			assert.Equal(t, expected.SourceRepresentations[i].PrefLabel, concept.PrefLabel, fmt.Sprintf("Actual concept pref label differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].Type, concept.Type, fmt.Sprintf("Actual concept type differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].UUID, concept.UUID, fmt.Sprintf("Actual concept uuid differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].DescriptionXML, concept.DescriptionXML, fmt.Sprintf("Actual concept descriptionXML differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].ImageURL, concept.ImageURL, fmt.Sprintf("Actual concept image url differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].Strapline, concept.Strapline, fmt.Sprintf("Actual concept strapline differs from expected: ConceptId: %s", concept.UUID))
			assert.True(t, reflect.DeepEqual(expected.SourceRepresentations[i], concept), fmt.Sprintf("Actual concept differs from expected: ConceptId: %s", concept.UUID))
		}
		actualConcept.SourceRepresentations = concepts
	}
	assert.True(t, reflect.DeepEqual(expected, actualConcept), "Actual agrregated concept differs from expected")
}

func neoUrl() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7777/db/data"
	}
	return url
}

func getConceptService(t *testing.T) Service {
	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(neoUrl(), conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewConceptService(db)
	service.Initialise()
	return service
}

func cleanDB(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel]-(i)
			DELETE rel, i
			DETACH DELETE i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func doesThingExistAtAll(t *testing.T, uuid string) bool {
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
	assert.NoError(t, err)

	if len(result) == 0 {
		return false
	}

	return true
}

func doesThingExistWithIdentifiers(t *testing.T, uuid string) bool {
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
	assert.NoError(t, err)

	if len(result) == 0 {
		return false
	}
	return true
}

func writeAnnotation(t *testing.T) annotations.Service {
	annotationsRW := annotations.NewCypherAnnotationsService(db, "v1", "annotations-v1")
	assert.NoError(t, annotationsRW.Initialise())
	writeJSONToAnnotationsService(t, annotationsRW, contentUUID, "./fixtures/Annotations-3fc9fe3e-af8c-4f7f-961a-e5065392bb31-v2.json")
	return annotationsRW
}

func writeContent(t *testing.T) baseftrwapp.Service {
	contentRW := content.NewCypherContentService(db)
	assert.NoError(t, contentRW.Initialise())
	writeJSONToService(t, contentRW, "./fixtures/Content-3fc9fe3e-af8c-4f7f-961a-e5065392bb31.json")
	return contentRW
}

func writeJSONToAnnotationsService(t *testing.T, service annotations.Service, contentUUID string, pathToJSONFile string) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, errr := service.DecodeJSON(dec)
	assert.NoError(t, errr, "Error parsing file %s", pathToJSONFile)
	errrr := service.Write(contentUUID, inst)
	assert.NoError(t, errrr)
}

func writeJSONToService(t *testing.T, service baseftrwapp.Service, pathToJSONFile string) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(t, err)
	dec := json.NewDecoder(f)
	inst, _, errr := service.DecodeJSON(dec)
	assert.NoError(t, errr)
	errrr := service.Write(inst)
	assert.NoError(t, errrr)
}

func getIdentifierValue(t *testing.T, uuidPropertyName string, uuid string, label string) string {
	results := []struct {
		Value string `json:"i.value"`
	}{}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			match (c:Concept {%s :{uuid}})-[r:IDENTIFIES]-(i:%s) return i.value
		`, uuidPropertyName, label),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{query})
	assert.NoError(t, err, fmt.Sprintf("Error while retrieving %s", label))

	if len(results) > 0 {
		return results[0].Value
	}
	return ""
}
