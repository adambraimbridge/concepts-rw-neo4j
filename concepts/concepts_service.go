package concepts

import (
	"encoding/json"
	"fmt"

	"github.com/Financial-Times/neo-model-utils-go/mapper"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

// CypherDriver - CypherDriver
type service struct {
	conn neoutils.NeoConnection
}

//NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) service {
	return service{cypherRunner}
}

// Would this be better as an extension in Neo4j? i.e. that any Thing has this
// constraint added on creation
func (s service) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
	})

	if err != nil {
		return err
	}
	return s.conn.EnsureConstraints(constraintMap)
}

type neoConcept struct {
	UUID           string   `json:"uuid"`
	PrefLabel      string   `json:"prefLabel"`
	Types          []string `json:"types"`
	Authority      string   `json:"authority"`
	AuthorityValue string   `json:"authorityValue"`
}

func (s service) Read(uuid string) (interface{}, bool, error) {
	results := []neoConcept{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Concept {uuid:{uuid}})
			return distinct n.uuid as uuid, n.prefLabel as prefLabel,
			labels(n) as types, n.authority as authority,
			n.authorityValue as authorityValue `,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		return AggregatedConcept{}, false, nil
	}

	typeName, error := mapper.MostSpecificType(results[0].Types)
	if error != nil {
		return AggregatedConcept{}, false, err
	}

	concept := Concept{UUID: results[0].UUID, PrefLabel: results[0].PrefLabel,
		Authority: results[0].Authority, AuthorityValue: results[0].AuthorityValue,
		Type: typeName}

	// currently there is no concordance for only TME concepts and needs to be
	// updated when there are rules involved to ascertain the canonical information
	// e.g for people or orgs
	aggregatedConcept := AggregatedConcept{UUID: results[0].UUID, Type: typeName,
		PrefLabel: results[0].PrefLabel, SourceRepresentations: []Concept{concept}}

	return aggregatedConcept, true, nil
}

func (s service) Write(thing interface{}) error {

	aggregatedConcept := thing.(AggregatedConcept)

	error := validateObject(aggregatedConcept)
	if (error != nil) {
		return error
	}

	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}})
			OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i)
			DELETE iden, i`,
		Parameters: map[string]interface{}{
			"uuid": aggregatedConcept.UUID,
		},
	}

	queryBatch := []*neoism.CypherQuery{deletePreviousIdentifiersQuery}

	var uppIdentifierQuery *neoism.CypherQuery
	var authorityIdentifierQuery *neoism.CypherQuery
	var createConceptQuery *neoism.CypherQuery

	// create-update node for CONCEPT
	// This needs to be enhanced for when concordance is done because it is only using the source representation rather
	// than the canonical information as it is the same for this scenario
	// ADD all the IDENTIFIER nodes and IDENTIFIES relationships

	for _, concept := range aggregatedConcept.SourceRepresentations {

		createConceptQuery = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
								set n={allprops}
								set n :%s`, getAllLabels(aggregatedConcept.Type)),
			Parameters: map[string]interface{}{
				"uuid": aggregatedConcept.UUID,
				"allprops": map[string]interface{}{
					"uuid":           concept.UUID,
					"prefLabel":      concept.PrefLabel,
					"authority":      concept.Authority,
					"authorityValue": concept.AuthorityValue,
				},
			},
		}

		queryBatch = append(queryBatch, createConceptQuery)

		// Add UPPIdentififer
		uppIdentifierQuery = createNewIdentifierQuery(concept.UUID, authorityToIdentifierLabelMap["UPP"], concept.UUID)
		queryBatch = append(queryBatch, uppIdentifierQuery)

		// Is Authority recognised
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			return requestError{(fmt.Sprintf("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority))}
		}

		// Add Authority identifier
		authorityIdentifierQuery = createNewIdentifierQuery(concept.UUID, authorityToIdentifierLabelMap[concept.Authority], concept.AuthorityValue)
		queryBatch = append(queryBatch, authorityIdentifierQuery)
	}

	return s.conn.CypherBatch(queryBatch)

}

func validateObject(aggConcept AggregatedConcept) error {
	if (aggConcept.PrefLabel == "") {
		return requestError{formatErrorString("prefLabel", aggConcept.UUID)}
	}
	if (aggConcept.Type == "") {
		return requestError{formatErrorString("type", aggConcept.UUID)}
	}
	if (aggConcept.SourceRepresentations == nil) {
		return requestError{formatErrorString("sourceRepresentation", aggConcept.UUID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		if (concept.PrefLabel == "") {
			return requestError{formatErrorString("sourceRepresentation.prefLabel", concept.UUID)}
		}
		if (concept.Type == "") {
			return requestError{formatErrorString("sourceRepresentation.type", concept.UUID)}
		}
		if (concept.AuthorityValue == "") {
			return requestError{formatErrorString("sourceRepresentation.authorityValue", concept.UUID)}
		}
	}
	return nil
}

func formatErrorString (field string, uuid string) string {
	return fmt.Sprintf("Invalid request, no %s has been supplied for: %s", field, uuid)
}

func getAllLabels(conceptType string) string {
	labels := conceptType
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		labels += ":" + parentType
		parentType = mapper.ParentType(parentType)
	}
	return labels
}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

func (s service) Delete(uuid string) (bool, error) {
	// We don't know what labels there are so we need to loop through all the types
	// so we can try and remove all the possibilities for this taxonomy
	var labelsToRemove string
	for i, conceptType := range conceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(conceptLabels) {
			labelsToRemove += ":"
		}
	}

	clearNode := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
			MATCH (t:Thing {uuid: {uuid}})
			REMOVE t:%s
			SET t:Thing
			SET t = {uuid:{uuid}}`, labelsToRemove),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	removeNodeIfUnused := &neoism.CypherQuery{
		Statement: `
			MATCH (thing:Thing {uuid: {uuid}})
 			OPTIONAL MATCH (thing)-[ir:IDENTIFIES]-(id:Identifier)
 			OPTIONAL MATCH (thing)-[a]-(x:Thing)
 			WITH ir, id, thing, count(a) AS relCount
  			WHERE relCount = 0
 			DELETE ir, id, thing
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{clearNode, removeNodeIfUnused})

	if err != nil {
		return false, err
	}

	s1, err := clearNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		deleted = true
	}

	return deleted, err
}

func (s service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.UUID, err
}

func (s service) Check() error {
	return neoutils.Check(s.conn)
}

// TODO: This needs to change of course to be taxonomy specific but will involve
// a breaking change to the base app so vendoring needs to be applied - Do we care about this count?
func (s service) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Concept) return count(n) as c`,
		Result:    &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}

type requestError struct {
	details string
}

func (re requestError) Error() string {
	return "Invalid Request"
}

func (re requestError) InvalidRequestDetails() string {
	return re.details
}