package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	log "github.com/sirupsen/logrus"
)

//Service - CypherDriver - CypherDriver
type ConceptService struct {
	conn neoutils.NeoConnection
}

// Service defines the functions any read-write application needs to implement
type ConceptServicer interface {
	Write(thing interface{}, transId string) (updatedIds interface{}, err error)
	Read(uuid string, transId string) (thing interface{}, found bool, err error)
	DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error)
	Check() error
	Initialise() error
}

//NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) ConceptService {
	return ConceptService{cypherRunner}
}

//Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s ConceptService) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
	})
	if err != nil {
		log.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.conn.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		log.WithError(err).Error("Could not run DB constraints")
		return err
	}

	err = s.conn.EnsureConstraints(map[string]string{
		"Thing":   "prefUUID",
		"Concept": "prefUUID",
	})
	if err != nil {
		log.WithError(err).Error("Could not run db constraints")
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
	EmailAddress          string       `json:"emailAddress,omitempty"`
	FacebookPage          string       `json:"facebookPage,omitempty"`
	TwitterHandle         string       `json:"twitterHandle,omitempty"`
	ScopeNote             string       `json:"scopeNote,omitempty"`
	ShortLabel            string       `json:"shortLabel,omitempty"`
	OrganisationUUID      string       `json:"organisationUUID,omitempty"`
	PersonUUID            string       `json:"personUUID,omitempty"`
	MembershipRoles       []string     `json:"membershipRoles,omitempty"`
}

type neoConcept struct {
	UUID              string   `json:"uuid,omitempty"`
	PrefUUID          string   `json:"prefUUID,omitempty"`
	Types             []string `json:"types,omitempty"`
	PrefLabel         string   `json:"prefLabel,omitempty"`
	Authority         string   `json:"authority,omitempty"`
	AuthorityValue    string   `json:"authorityValue,omitempty"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	Aliases           []string `json:"aliases,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	Strapline         string   `json:"strapline,omitempty"`
	ImageURL          string   `json:"imageUrl,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
	EmailAddress      string   `json:"emailAddress,omitempty"`
	FacebookPage      string   `json:"facebookPage,omitempty"`
	TwitterHandle     string   `json:"twitterHandle,omitempty"`
	ScopeNote         string   `json:"scopeNote,omitempty"`
	ShortLabel        string   `json:"shortLabel,omitempty"`
	RelatedUUIDs      []string `json:"relatedUUIDs,omitempty"`
	BroaderUUIDs      []string `json:"broaderUUIDs,omitempty"`
	OrganisationUUID  string   `json:"organisationUUID,omitempty"`
	PersonUUID        string   `json:"personUUID,omitempty"`
	MembershipRoles   []string `json:"membershipRoles,omitempty"`
}

type equivalenceResult struct {
	SourceUuid  string `json:"sourceUuid"`
	PrefUuid    string `json:"prefUuid"`
	Equivalence int    `json:"count"`
}

//Read - read service
func (s ConceptService) Read(uuid string, transId string) (interface{}, bool, error) {
	results := []neoAggregatedConcept{}

	query := &neoism.CypherQuery{
		Statement: `
				MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(node:Thing)
				OPTIONAL MATCH (node)-[:HAS_ORGANISATION]->(org:Thing)
				WITH canonical, node, org.uuid as organisationUUID
				OPTIONAL MATCH (node)-[:HAS_MEMBER]->(person:Thing)
				WITH canonical, node, organisationUUID, person.uuid as personUUID
				OPTIONAL MATCH (node)-[:IS_RELATED_TO]->(related:Thing)
				WITH canonical, node, collect(related.uuid) as relUUIDS, organisationUUID, personUUID
				OPTIONAL MATCH (node)-[:HAS_BROADER]->(broader:Thing)
				WITH canonical, node, relUUIDS, organisationUUID, personUUID, collect(broader.uuid) as broaderUUIDs
				OPTIONAL MATCH (node)-[:HAS_ROLE]->(role:Thing)
				WITH canonical, node, relUUIDS, organisationUUID, personUUID, broaderUUIDs, collect(role.uuid) as membershipRoles
				OPTIONAL MATCH (node)-[:HAS_PARENT]->(parent:Thing)
				WITH canonical.prefUUID as prefUUID, canonical.prefLabel as prefLabel, labels(canonical) as types, canonical.aliases as aliases,
				canonical.descriptionXML as descriptionXML, canonical.strapline as strapline, canonical.imageUrl as imageUrl,
				canonical.emailAddress as emailAddress, canonical.facebookPage as facebookPage, canonical.twitterHandle as twitterHandle,
				canonical.scopeNote as scopeNote, canonical.shortLabel as shortLabel, organisationUUID, personUUID, membershipRoles,
				{uuid:node.uuid, prefLabel:node.prefLabel, authority:node.authority, authorityValue: node.authorityValue,
				types: labels(node), lastModifiedEpoch: node.lastModifiedEpoch, emailAddress: node.emailAddress,
				facebookPage: node.facebookPage,twitterHandle: node.twitterHandle, scopeNote: node.scopeNote, shortLabel: node.shortLabel,
				aliases: node.aliases,descriptionXML: node.descriptionXML, imageUrl: node.imageUrl, strapline: node.strapline, parentUUIDs:collect(parent.uuid),
				relatedUUIDs:relUUIDS, broaderUUIDs:broaderUUIDs, organisationUUID: organisationUUID, personUUID: personUUID, membershipRoles: membershipRoles} as sources
				RETURN prefUUID, prefLabel, types, aliases, descriptionXML, strapline, imageUrl, emailAddress,
				facebookPage, twitterHandle, scopeNote, shortLabel, organisationUUID, personUUID, membershipRoles, collect(sources) as sourceRepresentations `,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Error executing neo4j read query")
		return AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		log.WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Info("Concept not found in db")
		return AggregatedConcept{}, false, nil
	}
	typeName, err := mapper.MostSpecificType(results[0].Types)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Returned concept had no recognized type")
		return AggregatedConcept{}, false, err
	}

	var sourceConcepts []Concept
	aggregatedConcept := AggregatedConcept{
		PrefUUID:         results[0].PrefUUID,
		PrefLabel:        results[0].PrefLabel,
		Type:             typeName,
		ImageURL:         results[0].ImageURL,
		DescriptionXML:   results[0].DescriptionXML,
		Strapline:        results[0].Strapline,
		Aliases:          results[0].Aliases,
		EmailAddress:     results[0].EmailAddress,
		FacebookPage:     results[0].FacebookPage,
		TwitterHandle:    results[0].TwitterHandle,
		ScopeNote:        results[0].ScopeNote,
		ShortLabel:       results[0].ShortLabel,
		PersonUUID:       results[0].PersonUUID,
		OrganisationUUID: results[0].OrganisationUUID,
	}

	if len(results[0].MembershipRoles) > 0 {
		var uuids = []string{}
		//TODO do this differently but I get a "" back from the cypher!
		for _, uuid := range results[0].MembershipRoles {
			if uuid != "" {
				uuids = append(uuids, uuid)
			}
		}
		if len(uuids) > 0 {
			aggregatedConcept.MembershipRoles = uuids
		}
	}

	for _, srcConcept := range results[0].SourceRepresentations {
		var concept Concept
		conceptType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"UUID": srcConcept.UUID, "transaction_id": transId}).Error("Returned source concept had no recognized type")
			return AggregatedConcept{}, false, err
		}
		if len(srcConcept.Aliases) > 0 {
			concept.Aliases = srcConcept.Aliases
		}

		uuids := []string{}

		if len(srcConcept.ParentUUIDs) > 0 {
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.ParentUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.ParentUUIDs = uuids
			}
		}

		if len(srcConcept.RelatedUUIDs) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.RelatedUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.RelatedUUIDs = uuids
			}
		}

		if len(srcConcept.BroaderUUIDs) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.BroaderUUIDs {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.BroaderUUIDs = uuids
			}
		}

		if len(srcConcept.MembershipRoles) > 0 {
			uuids = []string{}
			//TODO do this differently but I get a "" back from the cypher!
			for _, uuid := range srcConcept.MembershipRoles {
				if uuid != "" {
					uuids = append(uuids, uuid)
				}
			}
			if len(uuids) > 0 {
				concept.MembershipRoles = uuids
			}
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
		concept.FacebookPage = srcConcept.FacebookPage
		concept.EmailAddress = srcConcept.EmailAddress
		concept.TwitterHandle = srcConcept.TwitterHandle
		concept.ShortLabel = srcConcept.ShortLabel
		concept.ScopeNote = srcConcept.ScopeNote
		concept.PersonUUID = srcConcept.PersonUUID
		concept.OrganisationUUID = srcConcept.OrganisationUUID
		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts

	log.WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Debugf("Returned concept is %v", aggregatedConcept)

	return aggregatedConcept, true, nil
}

func (s ConceptService) Write(thing interface{}, transId string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	aggregatedConceptToWrite := thing.(AggregatedConcept)
	uuidsToUpdate := UpdatedConcepts{}

	var updatedUuidList []string
	updatedUuidList = append(updatedUuidList, aggregatedConceptToWrite.PrefUUID)

	existingConcept, exists, err := s.Read(aggregatedConceptToWrite.PrefUUID, transId)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Error("Read request for existing concordance resulted in error")
		return uuidsToUpdate, err
	}

	err = validateObject(aggregatedConceptToWrite, transId)
	if err != nil {
		return uuidsToUpdate, err
	}

	var updatedSourceIds []string
	for _, updatedSource := range aggregatedConceptToWrite.SourceRepresentations {
		if updatedSource.UUID != aggregatedConceptToWrite.PrefUUID {
			updatedSourceIds = append(updatedSourceIds, updatedSource.UUID)
			//We will need to send a notification of updates of all incoming source ids
			updatedUuidList = append(updatedUuidList, updatedSource.UUID)
		}
	}

	existingAggregateConcept := existingConcept.(AggregatedConcept)
	var existingSourceIds []string
	for _, existingSource := range existingAggregateConcept.SourceRepresentations {
		if existingSource.UUID != existingAggregateConcept.PrefUUID {
			existingSourceIds = append(existingSourceIds, existingSource.UUID)
		}
	}

	var listToUnconcord []string
	if exists {
		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		listToUnconcord = filterIdsThatAreUniqueToFirstList(existingSourceIds, updatedSourceIds)
	}

	//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
	listToTransferConcordance := filterIdsThatAreUniqueToFirstList(updatedSourceIds, existingSourceIds)

	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	//Handle scenarios for transferring source id from an existing concordance to this concordance
	if len(listToTransferConcordance) > 0 {
		prefUUIDsToBeDeletedQueryBatch, updatedUuidList, err = s.handleTransferConcordance(listToTransferConcordance, aggregatedConceptToWrite.PrefUUID, transId, updatedUuidList)
		if err != nil {
			return uuidsToUpdate, err
		}
	}

	var queryBatch []*neoism.CypherQuery

	clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)

	for _, query := range clearDownQuery {
		queryBatch = append(queryBatch, query)
	}

	// Create a concept from the canonical information - WITH NO UUID
	concept := Concept{
		PrefLabel:      aggregatedConceptToWrite.PrefLabel,
		Aliases:        aggregatedConceptToWrite.Aliases,
		Strapline:      aggregatedConceptToWrite.Strapline,
		DescriptionXML: aggregatedConceptToWrite.DescriptionXML,
		ImageURL:       aggregatedConceptToWrite.ImageURL,
		Type:           aggregatedConceptToWrite.Type,
		EmailAddress:   aggregatedConceptToWrite.EmailAddress,
		FacebookPage:   aggregatedConceptToWrite.FacebookPage,
		TwitterHandle:  aggregatedConceptToWrite.TwitterHandle,
		ScopeNote:      aggregatedConceptToWrite.ScopeNote,
		ShortLabel:     aggregatedConceptToWrite.ShortLabel,
	}

	// Create the canonical node
	queryBatch = append(queryBatch, createNodeQueries(concept, aggregatedConceptToWrite.PrefUUID, "")...)

	// Repopulate
	for _, concept := range aggregatedConceptToWrite.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(concept, "", concept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     concept.UUID,
				"prefUUID": aggregatedConceptToWrite.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

		if len(concept.RelatedUUIDs) > 0 {
			for _, relatedUUID := range concept.RelatedUUIDs {
				relatedToQuery := &neoism.CypherQuery{
					Statement: `
						MATCH (o:Concept {uuid: {uuid}})
						MERGE (p:Thing {uuid: {relUUID}})
		            	MERGE (o)-[:IS_RELATED_TO]->(p)
						MERGE (relatedUPP:Identifier:UPPIdentifier{value:{relUUID}})
                        MERGE (relatedUPP)-[:IDENTIFIES]->(p)`,
					Parameters: map[string]interface{}{
						"uuid":    concept.UUID,
						"relUUID": relatedUUID,
					},
				}
				queryBatch = append(queryBatch, relatedToQuery)
			}
		}

		if len(concept.BroaderUUIDs) > 0 {
			for _, broaderThanUUID := range concept.BroaderUUIDs {
				broaderThanQuery := &neoism.CypherQuery{
					Statement: `
						MATCH (o:Concept {uuid: {uuid}})
						MERGE (p:Thing {uuid: {brUUID}})
		            	MERGE (o)-[:HAS_BROADER]->(p)
		            	MERGE (brUPP:Identifier:UPPIdentifier{value:{brUUID}})
                        MERGE (brUPP)-[:IDENTIFIES]->(p)`,
					Parameters: map[string]interface{}{
						"uuid":   concept.UUID,
						"brUUID": broaderThanUUID,
					},
				}
				queryBatch = append(queryBatch, broaderThanQuery)
			}
		}
	}

	if len(listToUnconcord) > 0 {
		for _, idToUnconcord := range listToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					unconcordQuery := s.writeConcordedNodeForUnconcordedConcepts(concept)
					queryBatch = append(queryBatch, unconcordQuery)

					//We will need to send a notification of updates to unconcorded ids
					updatedUuidList = append(updatedUuidList, idToUnconcord)
				}
			}
		}
	}

	if len(prefUUIDsToBeDeletedQueryBatch) > 0 {
		for _, query := range prefUUIDsToBeDeletedQueryBatch {
			queryBatch = append(queryBatch, query)
		}
	}

	uuidsToUpdate.UpdatedIds = updatedUuidList

	log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Debug(fmt.Sprintf("Query: %s", query))
	}

	err = s.conn.CypherBatch(queryBatch)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Error("Error executing neo4j write query. Concept NOT written.")
		return uuidsToUpdate, err
	} else {
		log.WithFields(log.Fields{"UUID": aggregatedConceptToWrite.PrefUUID, "transaction_id": transId}).Info("Concept written to db")
		return uuidsToUpdate, nil
	}

	return uuidsToUpdate, nil
}

func validateObject(aggConcept AggregatedConcept, transId string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transId)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transId)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transId)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			log.WithField("UUID", aggConcept.PrefUUID).Debug("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority)
		}
		if concept.PrefLabel == "" {
			return requestError{formatError("sourceRepresentation.prefLabel", concept.UUID, transId)}
		}
		if concept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", concept.UUID, transId)}
		}
		if concept.AuthorityValue == "" {
			return requestError{formatError("sourceRepresentation.authorityValue", concept.UUID, transId)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{formatError("type", aggConcept.PrefUUID, transId)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transId string) string {
	err := errors.New("Invalid request, no " + field + " has been supplied")
	log.WithError(err).WithFields(log.Fields{"UUID": uuid, "transaction_id": transId}).Error("Validation of payload failed")
	return err.Error()
}

func filterIdsThatAreUniqueToFirstList(firstListIds []string, secondListIds []string) []string {
	//Loop through both lists to find id which is present in first list but not in the second
	var idIsUniqueToFirstList = true
	needToBeHandled := []string{}
	for _, firstId := range firstListIds {
		for _, secondId := range secondListIds {
			if firstId == secondId {
				//Id is present in both lists
				idIsUniqueToFirstList = false
			}
		}
		if idIsUniqueToFirstList == true {
			needToBeHandled = append(needToBeHandled, firstId)
		}
		idIsUniqueToFirstList = true
	}
	return needToBeHandled
}

func (s ConceptService) handleTransferConcordance(updatedSourceIds []string, prefUUID string, transId string, uuidsToUpdate []string) ([]*neoism.CypherQuery, []string, error) {
	result := []equivalenceResult{}

	deleteLonePrefUuidQueries := []*neoism.CypherQuery{}

	for _, updatedSourceId := range updatedSourceIds {
		equivQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (t:Thing {uuid:{uuid}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, c.prefUUID as prefUuid, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"uuid": updatedSourceId,
			},
			Result: &result,
		}

		err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId}).Error("Requests for source nodes canonical information resulted in error")
			return deleteLonePrefUuidQueries, uuidsToUpdate, err
		}

		if len(result) == 0 {
			log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId}).Debug("No existing concordance found")
			break
		} else if len(result) > 1 {
			err = errors.New("Multiple concepts found with matching uuid!")
			log.WithError(err).WithField("UUID", updatedSourceId)
			return deleteLonePrefUuidQueries, uuidsToUpdate, err
		}

		log.WithField("UUID", result[0].SourceUuid).Debug("Existing prefUUID is " + result[0].PrefUuid + " equivalence count is " + strconv.Itoa(result[0].Equivalence))
		// Source has no existing concordance and will be handled by clearDownExistingNodes function
		if result[0].Equivalence == 0 {
			break
		} else if result[0].Equivalence == 1 {
			// Source has existing concordance to itself, after transfer old pref uuid node will need to be cleaned up
			if result[0].SourceUuid == result[0].PrefUuid {
				log.WithField("UUID", result[0].SourceUuid).Debug("Pref uuid node will need to be deleted")
				deleteLonePrefUuidQueries = append(deleteLonePrefUuidQueries, deleteLonePrefUuid(result[0].PrefUuid))
				break
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := errors.New("This source id: " + result[0].SourceUuid + " the only concordance to a non-matching node with prefUuid: " + result[0].PrefUuid)
				log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingDodgyData"}).Error(err)
				return deleteLonePrefUuidQueries, uuidsToUpdate, err
			}
		} else {
			if result[0].SourceUuid == result[0].PrefUuid {
				if result[0].SourceUuid != prefUUID {
					// Source is prefUUID for a different concordance
					err := errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: " + result[0].SourceUuid)
					log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingInvalidConcordance"}).Error(err)
					return deleteLonePrefUuidQueries, uuidsToUpdate, err
				} else {
					// Source is prefUUID for a current concordance
					break
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of data
				log.WithFields(log.Fields{"UUID": prefUUID, "transaction_id": transId, "alert_tag": "ConceptLoadingStaleData"}).Info("Need to re-ingest concordance record for prefUuid: " + result[0].PrefUuid + " as source: " + result[0].SourceUuid + " has been removed.")
				//We will need to send a notification of updates to existing concordances who have had source nodes removed
				uuidsToUpdate = append(uuidsToUpdate, result[0].PrefUuid)
				break
			}
		}
	}
	return deleteLonePrefUuidQueries, uuidsToUpdate, nil
}

func deleteLonePrefUuid(prefUUID string) *neoism.CypherQuery {
	log.WithField("UUID", prefUUID).Debug("Deleting orphaned prefUUID node")
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {prefUUID:{id}}) DELETE t`,
		Parameters: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

func (s ConceptService) clearDownExistingNodes(ac AggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID
	sourceUuids := getSourceIds(ac.SourceRepresentations)

	queryBatch := []*neoism.CypherQuery{}

	for _, id := range sourceUuids {
		// TODO: We should be consistent in using a method to add identifiers: addIdentifierNodes
		deletePreviousIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
			OPTIONAL MATCH (t)-[x:HAS_PARENT]->(p)
			OPTIONAL MATCH (t)-[relatedTo:IS_RELATED_TO]->(relNode)
			OPTIONAL MATCH (t)-[broader:HAS_BROADER]->(brNode)
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE x, rel, i, eq, relatedTo, broader`, getLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": id,
			},
		}
		queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)
	}

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
	queryBatch = append(queryBatch, deletePreviousIdentifiersLabelsAndPropertiesQuery)

	return queryBatch
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
				Statement: `MERGE (o:Thing {uuid: {uuid}})
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

	if concept.OrganisationUUID != "" {
		writeOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (orgupp:Identifier:UPPIdentifier{value:{orgUuid}})
                            MERGE (orgupp)-[:IDENTIFIES]->(org:Thing) ON CREATE SET org.uuid = {orgUuid}
		            		MERGE (membership)-[:HAS_ORGANISATION]->(org)`,
			Parameters: neoism.Props{
				"orgUuid": concept.OrganisationUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeOrganisation)
	}

	if concept.PersonUUID != "" {
		writePerson := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (personupp:Identifier:UPPIdentifier{value:{personUuid}})
                            MERGE (personupp)-[:IDENTIFIES]->(person:Thing) ON CREATE SET person.uuid = {personUuid}
		            		MERGE (membership)-[:HAS_MEMBER]->(person)`,
			Parameters: neoism.Props{
				"personUuid": concept.PersonUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writePerson)
	}

	if len(concept.MembershipRoles) > 0 {
		for _, membershipRoleUUID := range concept.MembershipRoles {
			writeParent := &neoism.CypherQuery{
				Statement: `MERGE (membership:Thing {uuid: {uuid}})
		  	   				MERGE (roleupp:Identifier:UPPIdentifier{value:{mmbUuid}})
                            MERGE (roleupp)-[:IDENTIFIES]->(role:Thing) ON CREATE SET role.uuid = {mmbUuid}
		            		MERGE (membership)-[:HAS_ROLE]->(role)	`,
				Parameters: neoism.Props{
					"mmbUuid": membershipRoleUUID,
					"uuid":    concept.UUID,
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

func (s ConceptService) writeConcordedNodeForUnconcordedConcepts(concept Concept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	log.WithField("UUID", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`	MATCH (t:Thing{uuid:{prefUUID}})
										MERGE (n:Thing {prefUUID: {prefUUID}})<-[:EQUIVALENT_TO]-(t)
										set n={allprops}
										set n :%s`, getAllLabels(concept.Type)),
		Parameters: map[string]interface{}{
			"prefUUID": concept.UUID,
			"allprops": allProps,
		},
	}
	return createCanonicalNodeQuery
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

func getSourceIds(sourceConcepts []Concept) []string {
	var idList []string
	for _, concept := range sourceConcepts {
		idList = append(idList, concept.UUID)
	}
	return idList
}

func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}

	nodeProps["prefLabel"] = concept.PrefLabel
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()

	if len(concept.Aliases) > 0 {
		nodeProps["aliases"] = concept.Aliases
	}
	if concept.EmailAddress != "" {
		nodeProps["emailAddress"] = concept.EmailAddress
	}
	if concept.FacebookPage != "" {
		nodeProps["facebookPage"] = concept.FacebookPage
	}
	if concept.TwitterHandle != "" {
		nodeProps["twitterHandle"] = concept.TwitterHandle
	}
	if concept.ScopeNote != "" {
		nodeProps["scopeNote"] = concept.ScopeNote
	}
	if concept.ShortLabel != "" {
		nodeProps["shortLabel"] = concept.ShortLabel
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
		//TODO Remove this when things no longer matches on UppIdentifier
		// Add UPPIdentififer
		uppIdentifierQuery := createNewIdentifierQuery(UUID,
			authorityToIdentifierLabelMap["UPP"], UUID)

		queryBatch = append(queryBatch, uppIdentifierQuery)
	}
	return queryBatch
}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					MERGE (i:Identifier {value:{value}})
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

//DecodeJSON - decode json
func (s ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s ConceptService) Check() error {
	return neoutils.Check(s.conn)
}

type requestError struct {
	details string
}

//Error - Error
func (re requestError) Error() string {
	return re.details
}

//InvalidRequestDetails - Specific error for providing bad request (400) back
func (re requestError) InvalidRequestDetails() string {
	return re.details
}
