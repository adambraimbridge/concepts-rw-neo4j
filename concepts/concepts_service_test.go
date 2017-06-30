package concepts

import (
	"fmt"
	"os"
	"testing"

	"time"

	"sort"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"

	"github.com/jmcvetta/neoism"

	"reflect"
	"errors"
)

//all uuids to be cleaned from DB
const (
	basicConceptUUID        = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	parentUuid              = "2ef39c2a-da9c-4263-8209-ebfd490d3101"

	sourceId_1 = "74c94c35-e16b-4527-8ef1-c8bcdcc8f05b"
	sourceId_2 = "de3bcb30-992c-424e-8891-73f5bd9a7d3a"
	sourceId_3 = "5b1d8c31-dfe4-4326-b6a9-6227cb59af1f"
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
			ParentUUIDs:    []string{parentUuid},
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
	defer cleanDB(t)

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
			defer cleanDB(t)
			err := conceptsDriver.Write(test.aggregatedConcept, "")

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
				if err != nil {
					assert.Error(t, err, "Error was expected")
					assert.Contains(t, err.Error(), test.errStr, "Error message is not correct")
				}
				// TODO: Check these errors better
			}
		})
	}
}

func TestHandleUnconcordance(t *testing.T) {
	aggConcept_1 := AggregatedConcept{ PrefUUID:  sourceId_1, SourceRepresentations: []Concept{{UUID: sourceId_1}}}
	aggConcept_2 := AggregatedConcept{ PrefUUID:  sourceId_1, SourceRepresentations: []Concept{{UUID: sourceId_1}, {UUID: sourceId_2}}}
	aggConcept_3 := AggregatedConcept{ PrefUUID:  sourceId_1, SourceRepresentations: []Concept{{UUID: sourceId_1}, {UUID: sourceId_2}, {UUID: sourceId_3}}}
	aggConcept_4 := AggregatedConcept{ PrefUUID:  sourceId_1, SourceRepresentations: []Concept{{UUID: sourceId_3}, {UUID: sourceId_1}, {UUID: sourceId_2}}}

	type testStruct struct {
		testName 		string
		uuidsToUpdateList 	[]string
		existingAggConcept 	AggregatedConcept
		listToUnconcord 	[]Concept
	}

	emptyWhenAggConceptsAreTheSame := testStruct{testName: "emptyWhenAggConceptsAreTheSame", uuidsToUpdateList: []string{sourceId_1}, existingAggConcept: aggConcept_1, listToUnconcord: []Concept{}}
	emptyWhenAggConceptsAreTheSameButInDifferentOrder := testStruct{testName: "emptyWhenAggConceptsAreTheSameButInDifferentOrder", uuidsToUpdateList: []string{sourceId_1, sourceId_2, sourceId_3}, existingAggConcept: aggConcept_4, listToUnconcord: []Concept{}}
	hasSource2WhenSource2IsUnconcorded := testStruct{testName: "listToUnconcordIsEmptyWhenAggConceptsAreTheSame", uuidsToUpdateList: []string{sourceId_1}, existingAggConcept: aggConcept_2, listToUnconcord: []Concept{{UUID: sourceId_2}}}
	hasSource2And3WhenSource2And3AreUnconcorded := testStruct{testName: "hasSource2And3WhenSource2And3AreUnconcorded", uuidsToUpdateList: []string{sourceId_1}, existingAggConcept: aggConcept_3, listToUnconcord: []Concept{{UUID: sourceId_2}, {UUID: sourceId_3}}}

	Scenarios := []testStruct{emptyWhenAggConceptsAreTheSame, emptyWhenAggConceptsAreTheSameButInDifferentOrder, hasSource2WhenSource2IsUnconcorded, hasSource2And3WhenSource2And3AreUnconcorded}

	for _, scenario := range Scenarios {
		returnedList := handleUnconcordance(scenario.uuidsToUpdateList, scenario.existingAggConcept)
		assert.Equal(t, scenario.listToUnconcord, returnedList, "Failure")
	}
}

func TestTransferConcordance(t *testing.T) {
	statement := `MERGE (a:Thing{prefUUID:"1"}) MERGE (b:Thing{uuid:"1"}) MERGE (c:Thing{uuid:"2"}) MERGE (d:Thing{uuid:"3"}) MERGE (w:Thing{prefUUID:"4"}) MERGE (y:Thing{uuid:"5"}) MERGE (j:Thing{prefUUID:"6"}) MERGE (k:Thing{uuid:"6"}) MERGE (c)-[:EQUIVALENT_TO]->(a)<-[:EQUIVALENT_TO]-(b) MERGE (w)<-[:EQUIVALENT_TO]-(d) MERGE (j)<-[:EQUIVALENT_TO]-(k)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	emptyQuery := []*neoism.CypherQuery{}

	type testStruct struct {
		testName 		string
		updatedSourceIds 	[]string
		returnResult            bool
		returnedError 		error
	}

	nodeHasNoConconcordance := testStruct{testName: "nodeHasNoConconcordance", updatedSourceIds: []string{"5"}, returnedError: nil}
	nodeHasExistingConcordanceWhichNeedsToBeReWritten := testStruct{testName: "nodeHasExistingConcordanceWhichNeedsToBeReWritten", updatedSourceIds: []string{"2"}, returnedError: errors.New("Need to re-write concordance with prefUuid: 1 as removing source 2 may change canonical fields")}
	nodeHasInvalidConcordance := testStruct{testName: "nodeHasInvalidConcordance", updatedSourceIds: []string{"3"}, returnedError: errors.New("This source id: 3 the only concordance to a non-matching node with prefUuid: 4")}
	nodeIsPrefUuidForExistingConcordance := testStruct{testName: "nodeIsPrefUuidForExistingConcordance", updatedSourceIds: []string{"1"}, returnedError: errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1")}
	nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted := testStruct{testName: "nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted", updatedSourceIds: []string{"6"}, returnResult: true, returnedError: nil}

	scenarios := []testStruct{nodeHasNoConconcordance, nodeHasExistingConcordanceWhichNeedsToBeReWritten, nodeHasInvalidConcordance, nodeIsPrefUuidForExistingConcordance, nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, "", "")
		assert.Equal(t, scenario.returnedError, err, "Scenario " + scenario.testName + " returned unexpected error")
		if scenario.returnResult == true {
			assert.NotEqual(t, emptyQuery, returnedQueryList, "Scenario " + scenario.testName + " results do not match")
			break
		}
		assert.Equal(t, emptyQuery, returnedQueryList, "Scenario " + scenario.testName + " results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5", "6")
	defer deleteConcordedNodes(t, "1", "4", "6")
}

func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	defer cleanDB(t)

	type testStruct struct {
		testName 		string
		aggConcept 		AggregatedConcept
		returnedError 		string
	}

	aggregateConceptNoPrefLabel := AggregatedConcept{PrefUUID: basicConceptUUID}
	aggregateConceptNoType := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label"}
	aggregateConceptNoSourceReps := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand"}
	sourceRepNoPrefLabel := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand", SourceRepresentations: []Concept{{UUID: basicConceptUUID}}}
	sourceRepNoType := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand", SourceRepresentations: []Concept{{UUID: basicConceptUUID, PrefLabel: "The Best Label"}}}
	sourceRepNoAuthorityValue := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand", SourceRepresentations: []Concept{{UUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand"}}}
	returnNoError := AggregatedConcept{PrefUUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand", SourceRepresentations: []Concept{{UUID: basicConceptUUID, PrefLabel: "The Best Label", Type: "Brand", AuthorityValue: "123456-UPP"}}}

	testAggregateConceptNoPrefLabel := testStruct{testName: "testAggregateConceptNoPrefLabel", aggConcept: aggregateConceptNoPrefLabel, returnedError: "Invalid request, no prefLabel has been supplied"}
	testAggregateConceptNoType := testStruct{testName: "testAggregateConceptNoType", aggConcept: aggregateConceptNoType, returnedError: "Invalid request, no type has been supplied"}
	testAggregateConceptNoSourceReps := testStruct{testName: "testAggregateConceptNoSourceReps", aggConcept: aggregateConceptNoSourceReps, returnedError: "Invalid request, no sourceRepresentation has been supplied"}
	testSourceRepNoPrefLabel := testStruct{testName: "testSourceRepNoPrefLabel", aggConcept: sourceRepNoPrefLabel, returnedError: "Invalid request, no sourceRepresentation.prefLabel has been supplied"}
	testSourceRepNoType := testStruct{testName: "testSourceRepNoType", aggConcept: sourceRepNoType, returnedError: "Invalid request, no sourceRepresentation.type has been supplied"}
	testSourceRepNoAuthorityValue := testStruct{testName: "testSourceRepNoAuthorityValue", aggConcept: sourceRepNoAuthorityValue, returnedError: "Invalid request, no sourceRepresentation.authorityValue has been supplied"}
	returnNoErrorTest := testStruct{testName: "returnNoErrorTest", aggConcept: returnNoError, returnedError: ""}

	scenarios := []testStruct{testAggregateConceptNoPrefLabel, testAggregateConceptNoType, testAggregateConceptNoSourceReps, testSourceRepNoPrefLabel, testSourceRepNoType, testSourceRepNoAuthorityValue, returnNoErrorTest}

	for _, scenario := range scenarios {
		err := validateObject(scenario.aggConcept, "trans_id")
		if err != nil {
			assert.Contains(t, err.Error(), scenario.returnedError, scenario.testName)
		} else {
			assert.NoError(t, err, scenario.testName)
		}
	}
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	defer cleanDB(t)

	basicAggregatedConcept := getFullLoneAggregatedConcept()
	assert.NoError(conceptsDriver.Write(basicAggregatedConcept, ""), "Failed to write concept")

	nr, err := conceptsDriver.Count()
	assert.Equal(2, nr, "Should be 2 concepts in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	assert.NoError(conceptsDriver.Write(getAnotherFullLoneAggregatedConcept(), ""), "Failed to write concept")

	nr, err = conceptsDriver.Count()
	assert.Equal(4, nr, "Should be 4 subjects in Neo4j - count differs")
	assert.NoError(err, "An unexpected error occurred during count")
}

func readConceptAndCompare(t *testing.T, expected AggregatedConcept, testName string) {
	actual, found, err := conceptsDriver.Read(expected.PrefUUID, "")
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

			sort.Slice(concept.ParentUUIDs, func(i, j int) bool {
				return concept.ParentUUIDs[i] < concept.ParentUUIDs[j]
			})

			if expected.SourceRepresentations[i].ParentUUIDs != nil || len(expected.SourceRepresentations[i].ParentUUIDs) > 0 {

				sort.Slice(expected.SourceRepresentations[i].ParentUUIDs, func(i, j int) bool {
					return expected.SourceRepresentations[i].ParentUUIDs[i] < expected.SourceRepresentations[i].ParentUUIDs[j]
				})
			}
			assert.Equal(t, expected.SourceRepresentations[i].PrefLabel, concept.PrefLabel, fmt.Sprintf("Actual concept pref label differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].Type, concept.Type, fmt.Sprintf("Actual concept type differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].UUID, concept.UUID, fmt.Sprintf("Actual concept uuid differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].DescriptionXML, concept.DescriptionXML, fmt.Sprintf("Actual concept descriptionXML differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].ImageURL, concept.ImageURL, fmt.Sprintf("Actual concept image url differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].Strapline, concept.Strapline, fmt.Sprintf("Actual concept strapline differs from expected: ConceptId: %s", concept.UUID))
			assert.True(t, reflect.DeepEqual(expected.SourceRepresentations[i], concept), fmt.Sprintf("Actual concept differs from expected: ConceptId: %s", concept.UUID))
			assert.Equal(t, expected.SourceRepresentations[i].ParentUUIDs, concept.ParentUUIDs, fmt.Sprintf("Actual concept parent uuids differs from expected: ConceptId: %s", concept.UUID))
		}
		actualConcept.SourceRepresentations = concepts
	}
	assert.True(t, reflect.DeepEqual(expected, actualConcept), "Actual aggregated concept differs from expected")
}

func neoUrl() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
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

func cleanDB(t *testing.T) {
	cleanSourceNodes(t, parentUuid, anotherBasicConceptUUID, basicConceptUUID)
	deleteSourceNodes(t, parentUuid, anotherBasicConceptUUID, basicConceptUUID)
	deleteConcordedNodes(t, parentUuid, basicConceptUUID, anotherBasicConceptUUID)
}

func deleteSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel]-(i)
			DETACH DELETE rel, i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func cleanSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (a)-[hp:HAS_PARENT]-(p)
			DELETE rel, hp, i`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
}

func deleteConcordedNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {prefUUID: "%s"})
			OPTIONAL MATCH (a)-[rel]-(i)
			DELETE rel, i, a`, uuid)}
	}
	err := db.CypherBatch(qs)
	assert.NoError(t, err, "Error executing clean up cypher")
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