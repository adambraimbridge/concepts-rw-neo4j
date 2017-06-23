package concepts

import (
	"encoding/json"

	"github.com/Financial-Times/neo-model-utils-go/mapper"

	"fmt"
	"time"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

//Service - CypherDriver - CypherDriver
type Service struct {
	conn neoutils.NeoConnection
}

//NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) Service {
	return Service{cypherRunner}
}

//Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s Service) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
		"Thing": "prefUUID",
	})

	if err != nil {
		return err
	}
	return s.conn.EnsureConstraints(constraintMap)
}

type neoAggregatedConcept struct {
	PrefUUID              string       `json:"prefUUID,omitempty"`
	PrefLabel             string       `json:"prefLabel"`
	Types                 []string     `json:"types"`
	Aliases               []string     `json:"aliases,omitempty"`
	Strapline             string       `json:"strapline,omitempty"`
	DescriptionXML        string       `json:"descriptionXML,omitempty"`
	ImageURL              string       `json:"imageUrl,omitempty"`
	SourceRepresentations []neoConcept `json:"sourceRepresentations"`
	Authority             string       `json:"authority,omitempty"`
	AuthorityValue        string       `json:"authorityValue,omitempty"`
	LastModifiedEpoch     int          `json:"lastModifiedEpoch,omitempty"`
}

type neoConcept struct {
	UUID              string   `json:"uuid,omitempty"`
	PrefUUID          string   `json:"prefUUID,omitempty"`
	Types             []string `json:"types"`
	PrefLabel         string   `json:"prefLabel"`
	Authority         string   `json:"authority"`
	AuthorityValue    string   `json:"authorityValue"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	Aliases           []string `json:"aliases,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	Strapline         string   `json:"strapline,omitempty"`
	ImageURL          string   `json:"imageUrl,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
}

//Read - read service
func (s Service) Read(uuid string) (interface{}, bool, error) {
	// TODO should we allow to come in from any UUID not only the canonical one
	results := []neoAggregatedConcept{}

	// Is the UUID a concorded concept
	isConcordedNode, errs := s.isConcordedConcept(uuid)
	if errs != nil {
		return AggregatedConcept{}, false, errs
	}

	var query *neoism.CypherQuery

	if isConcordedNode {
		query = &neoism.CypherQuery{
			Statement: `	MATCH (canonical:Concept {prefUUID:{prefUUID}})<-[:EQUIVALENT_TO]-(node:Concept)
					OPTIONAL MATCH (node)-[HAS_PARENT]->(parent:Thing)
					WITH canonical.prefUUID as prefUUID, canonical.prefLabel as prefLabel, labels(canonical) as types, canonical.aliases as aliases,
					canonical.descriptionXML as descriptionXML, canonical.strapline as strapline, canonical.imageUrl as imageUrl,
					{uuid:node.uuid, prefLabel:node.prefLabel, authority:node.authority, authorityValue: node.authorityValue,
					types: labels(node), lastModifiedEpoch: node.lastModifiedEpoch, aliases: node.aliases, descriptionXML: node.descriptionXML,
					imageUrl: node.imageUrl, strapline: node.strapline, parentUUIDs:collect(parent.uuid)} as sources
					RETURN prefUUID, prefLabel, types, aliases, descriptionXML, strapline, imageUrl, collect(sources) as sourceRepresentations `,
			Parameters: map[string]interface{}{
				"prefUUID": uuid,
			},
			Result: &results,
		}
	} else {
		query = &neoism.CypherQuery{
			Statement: `MATCH (n:Concept {uuid:{uuid}})
			OPTIONAL MATCH (n)-[HAS_PARENT]->(parent:Thing)
			return distinct n.uuid as uuid, n.prefUUID as prefUUID, n.prefLabel as prefLabel, labels(n) as types, n.authority as authority,
			n.authorityValue as authorityValue, n.lastModifiedEpoch as lastModifiedEpoch, n.aliases as aliases, n.imageUrl as imageUrl,
			n.strapline as strapline, n.descriptionXML as descriptionXML, collect(parent.uuid) as parentUUIDs`,
			Parameters: map[string]interface{}{
				"uuid": uuid,
			},
			Result: &results,
		}
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

	var sourceConcepts []Concept
	var concept Concept

	aggregatedConcept := AggregatedConcept{
		PrefUUID:       results[0].PrefUUID,
		PrefLabel:      results[0].PrefLabel,
		Type:           typeName,
		ImageURL:       results[0].ImageURL,
		DescriptionXML: results[0].DescriptionXML,
		Strapline:      results[0].Strapline,
		Aliases:        results[0].Aliases,
	}

	if isConcordedNode {

		for _, srcConcept := range results[0].SourceRepresentations {

			conceptType, error := mapper.MostSpecificType(srcConcept.Types)
			if error != nil {
				return AggregatedConcept{}, false, err
			}
			if len(srcConcept.Aliases) > 0 {
				concept.Aliases = srcConcept.Aliases
			}

			uuids := []string{}
			if len(srcConcept.ParentUUIDs) > 0 {
				//TODO do this differently but I get a "" back from the cypher!
				for _, uuid := range srcConcept.ParentUUIDs {
					if (uuid != "") {
						uuids = append(uuids, uuid)
					}
				}
			}
			if len(uuids) > 0 {
				concept.ParentUUIDs = uuids
			}
			concept.UUID = srcConcept.UUID
			concept.PrefLabel = srcConcept.PrefLabel
			concept.Authority = srcConcept.Authority
			concept.AuthorityValue = srcConcept.AuthorityValue
			concept.Type = conceptType
			concept.LastModifiedEpoch = srcConcept.LastModifiedEpoch
			concept.ImageURL = srcConcept.ImageURL
			concept.Strapline = srcConcept.Strapline
			concept.DescriptionXML = srcConcept.DescriptionXML

			sourceConcepts = append(sourceConcepts, concept)
		}
	} else {
		concept.UUID = aggregatedConcept.PrefUUID
		concept.PrefLabel = aggregatedConcept.PrefLabel
		concept.Authority = results[0].Authority
		concept.AuthorityValue = results[0].AuthorityValue
		concept.Type = typeName
		concept.LastModifiedEpoch = results[0].LastModifiedEpoch
		concept.Aliases = results[0].Aliases
		concept.ImageURL = results[0].ImageURL
		concept.Strapline = results[0].Strapline
		concept.DescriptionXML = results[0].DescriptionXML
		if len(results[0].Aliases) > 0 {
			concept.Aliases = results[0].Aliases
		}

		uuids := []string{}
		if len(results[0].SourceRepresentations[0].ParentUUIDs) > 0 {
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range results[0].SourceRepresentations[0].ParentUUIDs {
				if (uuid != "") {
					uuids = append(uuids, uuid)
				}
			}

			concept.ParentUUIDs = uuids
		}

		if len(uuids) > 0 {
			concept.ParentUUIDs = uuids
		}
		sourceConcepts = append(sourceConcepts, concept)
	}
	aggregatedConcept.SourceRepresentations = sourceConcepts

	return aggregatedConcept, true, nil
}

func (s Service) isConcordedConcept(uuid string) (bool, error) {
	results := []struct {
		uuid string
	}{}
	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Thing {uuid:{uuid}})-[:EQUIVALENT_TO]-(c) return n.uuid`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return false, err
	}

	if len(results) > 0 {
		return true, nil
	} else {
		results = []struct {
			uuid string
		}{}
		query := &neoism.CypherQuery{
			Statement: `MATCH (n:Thing {prefUUID:{uuid}})-[:EQUIVALENT_TO]-(c) return n.uuid`,
			Parameters: map[string]interface{}{
				"uuid": uuid,
			},
			Result: &results,
		}

		err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
		if err != nil {
			return false, err
		}

		if len(results) > 0 {
			return true, nil
		}
	}

	return false, nil
}

//Write - write method
func (s Service) Write(thing interface{}) error {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lode node if it has been removed from the concordance listed against a smart logic concept
	aggregatedConcept := thing.(AggregatedConcept)

	// TODO: Compare: existingNeoRepresentation, _, _ := s.Read(aggregatedConcept.PrefUUID)

	error := validateObject(aggregatedConcept)
	if error != nil {
		return error
	}

	var queryBatch []*neoism.CypherQuery

	// If canonical node is needed create it i.e more than one source and link each node to it
	if len(aggregatedConcept.SourceRepresentations) > 1 {
		// Does the canonical node exist already?
		// Clear down and delete any equivilent-to relationship
		err := s.clearDownExistingNodes(aggregatedConcept)

		if err != nil {
			return err
		}

		// Create a concept from the canonical information - WITH NO UUID
		concept := Concept{
			PrefLabel:      aggregatedConcept.PrefLabel,
			Aliases:        aggregatedConcept.Aliases,
			Strapline:      aggregatedConcept.Strapline,
			DescriptionXML: aggregatedConcept.DescriptionXML,
			ImageURL:       aggregatedConcept.ImageURL,
			Type:           aggregatedConcept.Type,
		}

		// Create the canonical node
		queryBatch = append(queryBatch, createNodeQueries(concept, aggregatedConcept.PrefUUID, "")...)

		for _, concept := range aggregatedConcept.SourceRepresentations {
			queryBatch = append(queryBatch, createNodeQueries(concept, "", concept.UUID)...)

			if len(aggregatedConcept.SourceRepresentations) > 1 {
				equivQuery := &neoism.CypherQuery{
					Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
					MERGE (t)-[:EQUIVALENT_TO]->(c)`,
					Parameters: map[string]interface{}{
						"uuid":     concept.UUID,
						"prefUUID": aggregatedConcept.PrefUUID,
					},
				}
				queryBatch = append(queryBatch, equivQuery)
			}
		}
	} else {
		// TODO Check if there is already a canonical node with this prefUUID - This might be unconcord operation

		// Lone node with no concordance created first
		// Assuming that there is 1 source system. Also assuming that if there is only one source system then the
		// preflabel and uuid should be the same as the aggregated level. However TODO add validation of this
		queryBatch = append(queryBatch, createNodeQueries(aggregatedConcept.SourceRepresentations[0], aggregatedConcept.SourceRepresentations[0].UUID, aggregatedConcept.SourceRepresentations[0].UUID)...)
	}

	// TODO Compare original neo model and set prefUUID if needed

	// TODO: Handle Constraint error properly but having difficulties with *neoutils.ConstraintViolationError
	return s.conn.CypherBatch(queryBatch)
}

func returnNewlyOrphanedLoneNodes(originalLeaves []Concept, futureLeaves []Concept) []Concept {
	conceptMap := make(map[string]Concept)
	for _, originalLeaf := range originalLeaves {
		conceptMap[originalLeaf.UUID] = originalLeaf
		for _, futureLeaf := range futureLeaves {
			if futureLeaf.UUID == originalLeaf.UUID {
				delete(conceptMap, originalLeaf.UUID)
			}
		}
	}

	orpahanNodes := make([]Concept, 0, len(conceptMap))

	for _, value := range conceptMap {
		orpahanNodes = append(orpahanNodes, value)
	}
	return orpahanNodes
}

func validateObject(aggConcept AggregatedConcept) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatErrorString("prefLabel", aggConcept.PrefUUID)}
	}
	if aggConcept.Type == "" {
		return requestError{formatErrorString("type", aggConcept.PrefUUID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatErrorString("sourceRepresentation", aggConcept.PrefUUID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			return requestError{(fmt.Sprintf("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority))}
		}
		if concept.PrefLabel == "" {
			return requestError{formatErrorString("sourceRepresentation.prefLabel", concept.UUID)}
		}
		if concept.Type == "" {
			return requestError{formatErrorString("sourceRepresentation.type", concept.UUID)}
		}
		if concept.AuthorityValue == "" {
			return requestError{formatErrorString("sourceRepresentation.authorityValue", concept.UUID)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{fmt.Sprintf("The source representation of uuid: %s has an unknown type of: %s", concept.UUID, concept.Type)}
		}
	}
	return nil
}

func formatErrorString(field string, uuid string) string {
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

func getLabelsToRemove() string {
	var labelsToRemove string
	for i, conceptType := range conceptLabels {
		labelsToRemove += conceptType
		if i+1 < len(conceptLabels) {
			labelsToRemove += ":"
		}
	}
	return labelsToRemove
}

func (s Service) clearDownExistingNodes(ac AggregatedConcept) error {
	acUUID := ac.PrefUUID
	sourceUuids := getSourceIds(ac.SourceRepresentations)


	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing {prefUUID:{acUUID}})
			OPTIONAL MATCH (t)<-[rel:EQUIVALENT_TO]-(s)
			REMOVE t:%s
			SET t={prefUUID:{acUUID}}
			DELETE rel`, getLabelsToRemove()),
		Parameters: map[string]interface{}{
			"acUUID": acUUID,
		},
	}

	queryBatch := []*neoism.CypherQuery{deletePreviousIdentifiersLabelsAndPropertiesQuery}

	for _, id := range sourceUuids {
		deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[rel:IDENTIFIES]-(i)
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE rel, i`, getLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": id,
			},
		}
		queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)
	}

	err := s.conn.CypherBatch(queryBatch)
	if err != nil {
		return err
	}

	return nil
}

func getSourceIds(sourceConcepts []Concept) []string {
	var idList []string
	for _, concept := range sourceConcepts {
		idList = append(idList, concept.UUID)
	}
	return idList
}

func createNodeQueries(concept Concept, prefUUID string, uuid string) []*neoism.CypherQuery {
	queryBatch := []*neoism.CypherQuery{}
	var createConceptQuery *neoism.CypherQuery

	// Leaf or Lone Node
	if uuid != "" {
		allProps := setProps(concept, uuid, true)
		createConceptQuery = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
								set n={allprops}
								set n :%s`, getAllLabels(concept.Type)),
			Parameters: map[string]interface{}{
				"uuid":     uuid,
				"allprops": allProps,
			},
		}
	} else {
		// Canonical node that doesn't have UUID
		allProps := setProps(concept, prefUUID, false)
		createConceptQuery = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MERGE (n:Thing {prefUUID: {prefUUID}})
								set n={allprops}
								set n :%s`, getAllLabels(concept.Type)),
			Parameters: map[string]interface{}{
				"prefUUID": prefUUID,
				"allprops": allProps,
			},
		}

	}

	if len(concept.ParentUUIDs) > 0 {
		for _, parentUUID := range concept.ParentUUIDs {
			writeParent := &neoism.CypherQuery{
				Statement: `
                                MERGE (o:Thing {uuid: {uuid}})
		  	   	MERGE (parentupp:Identifier:UPPIdentifier{value:{paUuid}})
                            	MERGE (parentupp)-[:IDENTIFIES]->(p:Thing) ON CREATE SET p.uuid = {paUuid}
		            	MERGE (o)-[:HAS_PARENT]->(p)	`,
				Parameters: neoism.Props{
					"paUuid": parentUUID,
					"uuid":   concept.UUID,
				},
			}
			queryBatch = append(queryBatch, writeParent)
		}
	}

	queryBatch = append(queryBatch, createConceptQuery)

	// If no UUID then it is the canonical node and will not have identifier nodes
	if uuid != "" {
		queryBatch = append(queryBatch, addIdentifierNodes(uuid, concept.Authority, concept.AuthorityValue)...)
	}

	return queryBatch

}

func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	nodeProps["prefLabel"] = concept.PrefLabel
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if len(concept.Aliases) > 0 {
		nodeProps["aliases"] = concept.Aliases
	}

	if concept.DescriptionXML != "" {
		nodeProps["descriptionXML"] = concept.DescriptionXML
	}
	if concept.ImageURL != "" {
		nodeProps["imageUrl"] = concept.ImageURL
	}
	if concept.Strapline != "" {
		nodeProps["strapline"] = concept.Strapline
	}

	if isSource {
		nodeProps["uuid"] = id
		nodeProps["authority"] = concept.Authority
		nodeProps["authorityValue"] = concept.AuthorityValue

	} else {
		nodeProps["prefUUID"] = id
	}

	return nodeProps
}

func addIdentifierNodes(UUID string, authority string, authorityValue string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	//Add Alternative Identifier
	for k, v := range authorityToIdentifierLabelMap {
		if k == authority {
			alternativeIdentifierQuery := createNewIdentifierQuery(UUID, v, authorityValue)
			queryBatch = append(queryBatch, alternativeIdentifierQuery)
		}
	}

	if authority != "UPP" {
		// Add UPPIdentififer
		uppIdentifierQuery := createNewIdentifierQuery(UUID,
			authorityToIdentifierLabelMap["UPP"], UUID)

		queryBatch = append(queryBatch, uppIdentifierQuery)
	}
	return queryBatch
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

//Delete - Delete method
func (s Service) Delete(uuid string) (bool, error) {
	// TODO: We need to establish what are the bounds of this service, how much of a concorded concept should be deleted

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

//DecodeJSON - decode json
func (s Service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s Service) Check() error {
	return neoutils.Check(s.conn)
}

//Count - Count of concepts
// TODO: This needs to change of course to be taxonomy specific but will involve
// a breaking change to the base app so vendoring needs to be applied - Do we care about this count?
func (s Service) Count() (int, error) {

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

//Error - Error
func (re requestError) Error() string {
	return "Invalid Request"
}

//InvalidRequestDetails - Specific error for providing bad request (400) back
func (re requestError) InvalidRequestDetails() string {
	return re.details
}
