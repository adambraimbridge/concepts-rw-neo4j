package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	logger "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
)

const (
	iso8601DateOnly = "2006-01-02"
	//Event types
	UpdatedEvent = "CONCEPT_UPDATED"
	AddedEvent   = "CONCORDANCE_ADDED"
	RemovedEvent = "CONCORDANCE_REMOVED"
)

var concordancesSources = []string{"ManagedLocation", "Smartlogic"}

// ConceptService - CypherDriver - CypherDriver
type ConceptService struct {
	conn neoutils.NeoConnection
}

// ConceptServicer defines the functions any read-write application needs to implement
type ConceptServicer interface {
	Write(thing interface{}, transID string) (updatedIds interface{}, err error)
	Read(uuid string, transID string) (thing interface{}, found bool, err error)
	DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error)
	Check() error
	Initialise() error
}

// NewConceptService instantiate driver
func NewConceptService(cypherRunner neoutils.NeoConnection) ConceptService {
	return ConceptService{cypherRunner}
}

// Initialise - Would this be better as an extension in Neo4j? i.e. that any Thing has this constraint added on creation
func (s *ConceptService) Initialise() error {
	err := s.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
		"Concept":    "leiCode",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db index")
		return err
	}

	err = s.conn.EnsureIndexes(map[string]string{
		"Thing":   "authorityValue",
		"Concept": "authorityValue",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run DB constraints")
		return err
	}

	err = s.conn.EnsureConstraints(map[string]string{
		"Thing":    "prefUUID",
		"Concept":  "prefUUID",
		"Location": "iso31661",
	})
	if err != nil {
		logger.WithError(err).Error("Could not run db constraints")
		return err
	}
	return s.conn.EnsureConstraints(constraintMap)
}

type neoAggregatedConcept struct {
	AggregateHash         string           `json:"aggregateHash,omitempty"`
	Aliases               []string         `json:"aliases,omitempty"`
	Authority             string           `json:"authority,omitempty"`
	AuthorityValue        string           `json:"authorityValue,omitempty"`
	DescriptionXML        string           `json:"descriptionXML,omitempty"`
	EmailAddress          string           `json:"emailAddress,omitempty"`
	FacebookPage          string           `json:"facebookPage,omitempty"`
	FigiCode              string           `json:"figiCode,omitempty"`
	ImageURL              string           `json:"imageUrl,omitempty"`
	InceptionDate         string           `json:"inceptionDate,omitempty"`
	InceptionDateEpoch    int64            `json:"inceptionDateEpoch,omitempty"`
	IssuedBy              string           `json:"issuedBy,omitempty"`
	LastModifiedEpoch     int              `json:"lastModifiedEpoch,omitempty"`
	MembershipRoles       []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID      string           `json:"organisationUUID,omitempty"`
	PersonUUID            string           `json:"personUUID,omitempty"`
	PrefLabel             string           `json:"prefLabel"`
	PrefUUID              string           `json:"prefUUID,omitempty"`
	ScopeNote             string           `json:"scopeNote,omitempty"`
	ShortLabel            string           `json:"shortLabel,omitempty"`
	SourceRepresentations []neoConcept     `json:"sourceRepresentations"`
	Strapline             string           `json:"strapline,omitempty"`
	TerminationDate       string           `json:"terminationDate,omitempty"`
	TerminationDateEpoch  int64            `json:"terminationDateEpoch,omitempty"`
	TwitterHandle         string           `json:"twitterHandle,omitempty"`
	Types                 []string         `json:"types"`
	IsDeprecated          bool             `json:"isDeprecated,omitempty"`
	// Organisations
	ProperName                 string   `json:"properName,omitempty"`
	ShortName                  string   `json:"shortName,omitempty"`
	TradeNames                 []string `json:"tradeNames,omitempty"`
	FormerNames                []string `json:"formerNames,omitempty"`
	CountryCode                string   `json:"countryCode,omitempty"`
	CountryOfRisk              string   `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation     string   `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations        string   `json:"countryOfOperations,omitempty"`
	CountryOfRiskUUID          string   `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID string   `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID    string   `json:"countryOfOperationsUUID,omitempty"`
	PostalCode                 string   `json:"postalCode,omitempty"`
	YearFounded                int      `json:"yearFounded,omitempty"`
	LeiCode                    string   `json:"leiCode,omitempty"`
	ParentOrganisation         string   `json:"parentOrganisation,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
}

type neoConcept struct {
	Aliases              []string         `json:"aliases,omitempty"`
	Authority            string           `json:"authority,omitempty"`
	AuthorityValue       string           `json:"authorityValue,omitempty"`
	BroaderUUIDs         []string         `json:"broaderUUIDs,omitempty"`
	DescriptionXML       string           `json:"descriptionXML,omitempty"`
	EmailAddress         string           `json:"emailAddress,omitempty"`
	FacebookPage         string           `json:"facebookPage,omitempty"`
	FigiCode             string           `json:"figiCode,omitempty"`
	ImageURL             string           `json:"imageUrl,omitempty"`
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	IssuedBy             string           `json:"issuedBy,omitempty"`
	LastModifiedEpoch    int              `json:"lastModifiedEpoch,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	ParentUUIDs          []string         `json:"parentUUIDs,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	PrefLabel            string           `json:"prefLabel,omitempty"`
	PrefUUID             string           `json:"prefUUID,omitempty"`
	RelatedUUIDs         []string         `json:"relatedUUIDs,omitempty"`
	SupersededByUUIDs    []string         `json:"supersededByUUIDs,omitempty"`
	ScopeNote            string           `json:"scopeNote,omitempty"`
	ShortLabel           string           `json:"shortLabel,omitempty"`
	Strapline            string           `json:"strapline,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	TwitterHandle        string           `json:"twitterHandle,omitempty"`
	Types                []string         `json:"types,omitempty"`
	UUID                 string           `json:"uuid,omitempty"`
	IsDeprecated         bool             `json:"isDeprecated,omitempty"`
	// Organisations
	ProperName                 string   `json:"properName,omitempty"`
	ShortName                  string   `json:"shortName,omitempty"`
	TradeNames                 []string `json:"tradeNames,omitempty"`
	FormerNames                []string `json:"formerNames,omitempty"`
	CountryCode                string   `json:"countryCode,omitempty"`
	CountryOfRisk              string   `json:"countryOfRisk,omitempty"`
	CountryOfIncorporation     string   `json:"countryOfIncorporation,omitempty"`
	CountryOfOperations        string   `json:"countryOfOperations,omitempty"`
	CountryOfRiskUUID          string   `json:"countryOfRiskUUID,omitempty"`
	CountryOfIncorporationUUID string   `json:"countryOfIncorporationUUID,omitempty"`
	CountryOfOperationsUUID    string   `json:"countryOfOperationsUUID,omitempty"`
	PostalCode                 string   `json:"postalCode,omitempty"`
	YearFounded                int      `json:"yearFounded,omitempty"`
	LeiCode                    string   `json:"leiCode,omitempty"`
	ParentOrganisation         string   `json:"parentOrganisation,omitempty"`
	// Location
	ISO31661 string `json:"iso31661,omitempty"`
	// Person
	Salutation string `json:"salutation,omitempty"`
	BirthYear  int    `json:"birthYear,omitempty"`
}

type equivalenceResult struct {
	SourceUUID  string   `json:"sourceUuid"`
	PrefUUID    string   `json:"prefUuid"`
	Types       []string `json:"types"`
	Equivalence int      `json:"count"`
	Authority   string   `json:"authority"`
}

//Read - read service
func (s *ConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	var results []neoAggregatedConcept

	query := &neoism.CypherQuery{
		Statement: `
			MATCH (canonical:Thing {prefUUID:{uuid}})<-[:EQUIVALENT_TO]-(source:Thing)
			OPTIONAL MATCH (source)-[:HAS_BROADER]->(broader:Thing)
			OPTIONAL MATCH (source)-[:HAS_MEMBER]->(person:Thing)
			OPTIONAL MATCH (source)-[:HAS_ORGANISATION]->(org:Thing)
			OPTIONAL MATCH (source)-[:HAS_PARENT]->(parent:Thing)
			OPTIONAL MATCH (source)-[:IS_RELATED_TO]->(related:Thing)
			OPTIONAL MATCH (source)-[:SUPERSEDED_BY]->(supersededBy:Thing)
			OPTIONAL MATCH (source)-[:ISSUED_BY]->(issuer:Thing)
			OPTIONAL MATCH (source)-[roleRel:HAS_ROLE]->(role:Thing)
			OPTIONAL MATCH (source)-[:SUB_ORGANISATION_OF]->(parentOrg:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_OPERATIONS]->(coo:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_RISK]->(cor:Thing)
			OPTIONAL MATCH (source)-[:COUNTRY_OF_INCORPORATION]->(coi:Thing)
			WITH
				broader,
				canonical,
				issuer,
				org,
				parent,
				person,
				related,
				supersededBy,
				role,
				roleRel,
				parentOrg,
				coo,
				cor,
				coi,
				source
				ORDER BY
					source.uuid,
					role.uuid
			WITH
				broader,
				canonical,
				issuer,
				org,
				parent,
				person,
				supersededBy,
				related,
				coo,
				cor,
				coi,
				{
					authority: source.authority,
					authorityValue: source.authorityValue,
					broaderUUIDs: collect(broader.uuid),
					supersededByUUIDs: collect(supersededBy.uuid),
					figiCode: source.figiCode,
					issuedBy: issuer.uuid,
					lastModifiedEpoch: source.lastModifiedEpoch,
					membershipRoles: collect({
						membershipRoleUUID: role.uuid,
						inceptionDate: roleRel.inceptionDate,
						terminationDate: roleRel.terminationDate,
						inceptionDateEpoch: roleRel.inceptionDateEpoch,
						terminationDateEpoch: roleRel.terminationDateEpoch
					}),
					organisationUUID: org.uuid,
					parentUUIDs: collect(parent.uuid),
					personUUID: person.uuid,
					parentOrganisation: parentOrg.uuid,
					prefLabel: source.prefLabel,
					relatedUUIDs: collect(related.uuid),
					types: labels(source),
					uuid: source.uuid,
					isDeprecated: source.isDeprecated,
					countryOfIncorporationUUID: coi.uuid,
					countryOfOperationsUUID: coo.uuid,
					countryOfRiskUUID: cor.uuid
				} as sources,
				collect({
					inceptionDate: roleRel.inceptionDate,
					inceptionDateEpoch: roleRel.inceptionDateEpoch,
					membershipRoleUUID: role.uuid,
					terminationDate: roleRel.terminationDate,
					terminationDateEpoch: roleRel.terminationDateEpoch
				}) as membershipRoles
			RETURN
				canonical.aggregateHash as aggregateHash,
				canonical.aliases as aliases,
				canonical.descriptionXML as descriptionXML,
				canonical.emailAddress as emailAddress,
				canonical.facebookPage as facebookPage,
				canonical.figiCode as figiCode,
				canonical.imageUrl as imageUrl,
				canonical.inceptionDate as inceptionDate,
				canonical.inceptionDateEpoch as inceptionDateEpoch,
				canonical.prefLabel as prefLabel,
				canonical.prefUUID as prefUUID,
				canonical.scopeNote as scopeNote,
				canonical.shortLabel as shortLabel,
				canonical.strapline as strapline,
				canonical.terminationDate as terminationDate,
				canonical.terminationDateEpoch as terminationDateEpoch,
				canonical.twitterHandle as twitterHandle,
				collect(sources) as sourceRepresentations,
				issuer.uuid as issuedBy,
				labels(canonical) as types,
				membershipRoles,
				org.uuid as organisationUUID,
				person.uuid as personUUID,
				canonical.properName as properName,
				canonical.shortName as shortName,
				canonical.tradeNames as tradeNames,
				canonical.formerNames as formerNames,
				canonical.countryCode as countryCode,
				canonical.countryOfIncorporation as countryOfIncorporation,
				canonical.countryOfOperations as countryOfOperations,
				canonical.countryOfRisk as countryOfRisk,
				coi.uuid as countryOfIncorporationUUID,
				coo.uuid as countryOfOperationsUUID,
				cor.uuid as countryOfRiskUUID,
				canonical.postalCode as postalCode,
				canonical.yearFounded as yearFounded,
				canonical.leiCode as leiCode,
				canonical.isDeprecated as isDeprecated,
				canonical.salutation as salutation,
				canonical.birthYear as birthYear,
				canonical.iso31661 as iso31661
			`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Error executing neo4j read query")
		return AggregatedConcept{}, false, err
	}

	if len(results) == 0 {
		logger.WithTransactionID(transID).WithUUID(uuid).Info("Concept not found in db")
		return AggregatedConcept{}, false, nil
	}
	typeName, err := mapper.MostSpecificType(results[0].Types)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Returned concept had no recognized type")
		return AggregatedConcept{}, false, err
	}

	aggregatedConcept := AggregatedConcept{
		AggregatedHash:   results[0].AggregateHash,
		Aliases:          results[0].Aliases,
		DescriptionXML:   results[0].DescriptionXML,
		EmailAddress:     results[0].EmailAddress,
		FacebookPage:     results[0].FacebookPage,
		FigiCode:         results[0].FigiCode,
		ImageURL:         results[0].ImageURL,
		InceptionDate:    results[0].InceptionDate,
		IssuedBy:         results[0].IssuedBy,
		MembershipRoles:  cleanMembershipRoles(results[0].MembershipRoles),
		OrganisationUUID: results[0].OrganisationUUID,
		PersonUUID:       results[0].PersonUUID,
		PrefLabel:        results[0].PrefLabel,
		PrefUUID:         results[0].PrefUUID,
		ScopeNote:        results[0].ScopeNote,
		ShortLabel:       results[0].ShortLabel,
		Strapline:        results[0].Strapline,
		TerminationDate:  results[0].TerminationDate,
		TwitterHandle:    results[0].TwitterHandle,
		Type:             typeName,
		IsDeprecated:     results[0].IsDeprecated,
		// Organisations
		ProperName:                 results[0].ProperName,
		ShortName:                  results[0].ShortName,
		TradeNames:                 results[0].TradeNames,
		FormerNames:                results[0].FormerNames,
		CountryCode:                results[0].CountryCode,
		CountryOfIncorporation:     results[0].CountryOfIncorporation,
		CountryOfRisk:              results[0].CountryOfRisk,
		CountryOfOperations:        results[0].CountryOfOperations,
		CountryOfIncorporationUUID: results[0].CountryOfIncorporationUUID,
		CountryOfRiskUUID:          results[0].CountryOfRiskUUID,
		CountryOfOperationsUUID:    results[0].CountryOfOperationsUUID,
		PostalCode:                 results[0].PostalCode,
		YearFounded:                results[0].YearFounded,
		LeiCode:                    results[0].LeiCode,
		// Person
		Salutation: results[0].Salutation,
		BirthYear:  results[0].BirthYear,
		// Location
		ISO31661: results[0].ISO31661,
	}

	var sourceConcepts []Concept
	for _, srcConcept := range results[0].SourceRepresentations {
		conceptType, err := mapper.MostSpecificType(srcConcept.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Returned source concept had no recognized type")
			return AggregatedConcept{}, false, err
		}

		concept := Concept{
			Authority:                  srcConcept.Authority,
			AuthorityValue:             srcConcept.AuthorityValue,
			BroaderUUIDs:               filterSlice(srcConcept.BroaderUUIDs),
			SupersededByUUIDs:          filterSlice(srcConcept.SupersededByUUIDs),
			FigiCode:                   srcConcept.FigiCode,
			IssuedBy:                   srcConcept.IssuedBy,
			LastModifiedEpoch:          srcConcept.LastModifiedEpoch,
			MembershipRoles:            cleanMembershipRoles(srcConcept.MembershipRoles),
			OrganisationUUID:           srcConcept.OrganisationUUID,
			CountryOfIncorporationUUID: srcConcept.CountryOfIncorporationUUID,
			CountryOfRiskUUID:          srcConcept.CountryOfRiskUUID,
			CountryOfOperationsUUID:    srcConcept.CountryOfOperationsUUID,
			ParentUUIDs:                filterSlice(srcConcept.ParentUUIDs),
			PersonUUID:                 srcConcept.PersonUUID,
			PrefLabel:                  srcConcept.PrefLabel,
			RelatedUUIDs:               filterSlice(srcConcept.RelatedUUIDs),
			Type:                       conceptType,
			UUID:                       srcConcept.UUID,
			IsDeprecated:               srcConcept.IsDeprecated,
			// Organisations
			ParentOrganisation: srcConcept.ParentOrganisation,
		}
		sourceConcepts = append(sourceConcepts, concept)
	}

	aggregatedConcept.SourceRepresentations = sourceConcepts
	logger.WithTransactionID(transID).WithUUID(uuid).Debugf("Returned concept is %v", aggregatedConcept)
	return cleanConcept(aggregatedConcept), true, nil
}

func (s *ConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	// Read the aggregated concept - We need read the entire model first. This is because if we unconcord a TME concept
	// then we need to add prefUUID to the lone node if it has been removed from the concordance listed against a Smartlogic concept
	updateRecord := ConceptChanges{}
	var updatedUUIDList []string
	aggregatedConceptToWrite := thing.(AggregatedConcept)
	aggregatedConceptToWrite = cleanSourceProperties(aggregatedConceptToWrite)
	requestSourceData := getSourceData(aggregatedConceptToWrite.SourceRepresentations)

	requestHash, err := hashstructure.Hash(aggregatedConceptToWrite, nil)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error hashing json from request")
		return updateRecord, err
	}

	hashAsString := strconv.FormatUint(requestHash, 10)

	if err = validateObject(aggregatedConceptToWrite, transID); err != nil {
		return updateRecord, err
	}

	existingConcept, exists, err := s.Read(aggregatedConceptToWrite.PrefUUID, transID)
	if err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Read request for existing concordance resulted in error")
		return updateRecord, err
	}

	aggregatedConceptToWrite = processMembershipRoles(aggregatedConceptToWrite).(AggregatedConcept)

	var queryBatch []*neoism.CypherQuery
	var prefUUIDsToBeDeletedQueryBatch []*neoism.CypherQuery
	if exists {
		existingAggregateConcept := existingConcept.(AggregatedConcept)
		if existingAggregateConcept.AggregatedHash == "" {
			existingAggregateConcept.AggregatedHash = "0"
		}
		currentHash, err := strconv.ParseUint(existingAggregateConcept.AggregatedHash, 10, 64)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Error whilst parsing existing concept hash")
			return updateRecord, nil
		}
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Currently stored concept has hash of %d", currentHash)
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debugf("Aggregated concept has hash of %d", requestHash)
		if currentHash == requestHash {
			logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept has not changed since most recent update")
			return updateRecord, nil
		}
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("This concept is different to record stored in db, updating...")

		existingSourceData := getSourceData(existingAggregateConcept.SourceRepresentations)

		//Concept has been updated since last write, so need to send notification of all affected ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}

		//This filter will leave us with ids that were members of existing concordance but are NOT members of current concordance
		//They will need a new prefUUID node written
		conceptsToUnconcord := filterIdsThatAreUniqueToFirstMap(existingSourceData, requestSourceData)

		//This filter will leave us with ids that are members of current concordance payload but were not previously concorded to this concordance
		conceptsToTransferConcordance := filterIdsThatAreUniqueToFirstMap(requestSourceData, existingSourceData)

		//Handle scenarios for transferring source id from an existing concordance to this concordance
		if len(conceptsToTransferConcordance) > 0 {
			prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(conceptsToTransferConcordance, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
			if err != nil {
				return updateRecord, err
			}

		}

		clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		for idToUnconcord := range conceptsToUnconcord {
			for _, concept := range existingAggregateConcept.SourceRepresentations {
				if idToUnconcord == concept.UUID {
					//aggConcept := buildAggregateConcept(concept)
					//set this to 0 as otherwise it is empty
					//TODO fix this up at some point to do it properly?
					concept.Hash = "0"
					unconcordQuery := s.writeCanonicalNodeForUnconcordedConcepts(concept)
					queryBatch = append(queryBatch, unconcordQuery)

					//We will need to send a notification of ids that have been removed from current concordance
					updatedUUIDList = append(updatedUUIDList, idToUnconcord)

					//Unconcordance event for new concept notifications
					updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
						ConceptType:   conceptsToUnconcord[idToUnconcord],
						ConceptUUID:   idToUnconcord,
						AggregateHash: hashAsString,
						TransactionID: transID,
						EventDetails: ConcordanceEvent{
							Type:  RemovedEvent,
							OldID: aggregatedConceptToWrite.PrefUUID,
							NewID: idToUnconcord,
						},
					})
				}
			}
		}
	} else {
		var conceptsToCheckForExistingConcordance []string
		for _, sr := range aggregatedConceptToWrite.SourceRepresentations {
			conceptsToCheckForExistingConcordance = append(conceptsToCheckForExistingConcordance, sr.UUID)
		}

		prefUUIDsToBeDeletedQueryBatch, err = s.handleTransferConcordance(requestSourceData, &updateRecord, hashAsString, aggregatedConceptToWrite, transID)
		if err != nil {
			return updateRecord, err
		}

		clearDownQuery := s.clearDownExistingNodes(aggregatedConceptToWrite)
		for _, query := range clearDownQuery {
			queryBatch = append(queryBatch, query)
		}

		//Concept is new, send notification of all source ids
		for _, source := range aggregatedConceptToWrite.SourceRepresentations {
			updatedUUIDList = append(updatedUUIDList, source.UUID)
		}
	}

	aggregatedConceptToWrite.AggregatedHash = hashAsString
	queryBatch = populateConceptQueries(queryBatch, aggregatedConceptToWrite)
	for _, query := range prefUUIDsToBeDeletedQueryBatch {
		queryBatch = append(queryBatch, query)
	}

	updateRecord.UpdatedIds = updatedUUIDList
	updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
		ConceptType:   aggregatedConceptToWrite.Type,
		ConceptUUID:   aggregatedConceptToWrite.PrefUUID,
		AggregateHash: hashAsString,
		TransactionID: transID,
		EventDetails: ConceptEvent{
			Type: UpdatedEvent,
		},
	})

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug("Executing " + strconv.Itoa(len(queryBatch)) + " queries")
	for _, query := range queryBatch {
		logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Debug(fmt.Sprintf("Query: %v", query))
	}

	// check that the issuer is not already related to a different org
	if aggregatedConceptToWrite.IssuedBy != "" {
		var fiRes []map[string]string
		issuerQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (issuer:Thing {uuid: {issuerUUID}})<-[:ISSUED_BY]-(fi)
					RETURN fi.uuid AS fiUUID
				`,
			Parameters: map[string]interface{}{
				"issuerUUID": aggregatedConceptToWrite.IssuedBy,
			},
			Result: &fiRes,
		}
		if err := s.conn.CypherBatch([]*neoism.CypherQuery{issuerQuery}); err != nil {
			logger.WithError(err).
				WithTransactionID(transID).
				WithUUID(aggregatedConceptToWrite.PrefUUID).
				Error("Could not get existing issuer.")
			return updateRecord, err
		}

		if len(fiRes) > 0 {
			for _, fi := range fiRes {
				fiUUID, ok := fi["fiUUID"]
				if !ok {
					continue
				}

				if fiUUID == aggregatedConceptToWrite.PrefUUID {
					continue
				}

				msg := fmt.Sprintf(
					"Issuer for %s was changed from %s to %s",
					aggregatedConceptToWrite.IssuedBy,
					fiUUID,
					aggregatedConceptToWrite.PrefUUID,
				)
				logger.WithTransactionID(transID).
					WithUUID(aggregatedConceptToWrite.PrefUUID).
					WithField("alert_tag", "ConceptLoadingLedToDifferentIssuer").Info(msg)

				deleteIssuerRelations := &neoism.CypherQuery{
					Statement: `
					MATCH (issuer:Thing {uuid: {issuerUUID}})
					MATCH (fi:Thing {uuid: {fiUUID}})
					MATCH (issuer)<-[issuerRel:ISSUED_BY]-(fi)
					DELETE issuerRel
				`,
					Parameters: map[string]interface{}{
						"issuerUUID": aggregatedConceptToWrite.IssuedBy,
						"fiUUID":     fiUUID,
					},
				}
				queryBatch = append(queryBatch, deleteIssuerRelations)
			}
		}
	}

	if err = s.conn.CypherBatch(queryBatch); err != nil {
		logger.WithError(err).WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Error("Error executing neo4j write queries. Concept NOT written.")
		return updateRecord, err
	}

	logger.WithTransactionID(transID).WithUUID(aggregatedConceptToWrite.PrefUUID).Info("Concept written to db")
	return updateRecord, nil
}

func validateObject(aggConcept AggregatedConcept, transID string) error {
	if aggConcept.PrefLabel == "" {
		return requestError{formatError("prefLabel", aggConcept.PrefUUID, transID)}
	}
	if _, ok := constraintMap[aggConcept.Type]; !ok {
		return requestError{formatError("type", aggConcept.PrefUUID, transID)}
	}
	if aggConcept.SourceRepresentations == nil {
		return requestError{formatError("sourceRepresentation", aggConcept.PrefUUID, transID)}
	}
	for _, concept := range aggConcept.SourceRepresentations {
		// Is Authority recognised?
		if _, ok := authorityToIdentifierLabelMap[concept.Authority]; !ok {
			logger.WithTransactionID(transID).WithUUID(aggConcept.PrefUUID).Debugf("Unknown authority, therefore unable to add the relevant Identifier node: %s", concept.Authority)
		}
		if concept.Type == "" {
			return requestError{formatError("sourceRepresentation.type", concept.UUID, transID)}
		}
		if concept.AuthorityValue == "" {
			return requestError{formatError("sourceRepresentation.authorityValue", concept.UUID, transID)}
		}
		if _, ok := constraintMap[concept.Type]; !ok {
			return requestError{formatError("type", aggConcept.PrefUUID, transID)}
		}
	}
	return nil
}

func formatError(field string, uuid string, transID string) string {
	err := errors.New("Invalid request, no " + field + " has been supplied")
	logger.WithError(err).WithTransactionID(transID).WithUUID(uuid).Error("Validation of payload failed")
	return err.Error()
}

func filterIdsThatAreUniqueToFirstMap(firstMapConcepts map[string]string, secondMapConcepts map[string]string) map[string]string {
	//Loop through both lists to find id which is present in first list but not in the second
	filteredMap := make(map[string]string)

	for conceptID := range firstMapConcepts {
		if _, ok := secondMapConcepts[conceptID]; !ok {
			filteredMap[conceptID] = firstMapConcepts[conceptID]
		}
	}
	return filteredMap
}

//Handle new source nodes that have been added to current concordance
func (s *ConceptService) handleTransferConcordance(conceptData map[string]string, updateRecord *ConceptChanges, aggregateHash string, newAggregatedConcept AggregatedConcept, transID string) ([]*neoism.CypherQuery, error) {
	var result []equivalenceResult
	var deleteLonePrefUUIDQueries []*neoism.CypherQuery

	for updatedSourceID := range conceptData {
		equivQuery := &neoism.CypherQuery{
			Statement: `
					MATCH (t:Thing {uuid:{id}})
					OPTIONAL MATCH (t)-[:EQUIVALENT_TO]->(c)
					OPTIONAL MATCH (c)<-[eq:EQUIVALENT_TO]-(x:Thing)
					RETURN t.uuid as sourceUuid, labels(t) as types, c.prefUUID as prefUuid, t.authority as authority, COUNT(DISTINCT eq) as count`,
			Parameters: map[string]interface{}{
				"id": updatedSourceID,
			},
			Result: &result,
		}
		err := s.conn.CypherBatch([]*neoism.CypherQuery{equivQuery})
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error("Requests for source nodes canonical information resulted in error")
			return deleteLonePrefUUIDQueries, err
		}

		//source node does not currently exist in neo4j, nothing to tidy up
		if len(result) == 0 {
			logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Info("No existing concordance record found")
			if updatedSourceID != newAggregatedConcept.PrefUUID {
				//concept does not exist, need update event
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptData[updatedSourceID],
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				})

				//create concordance event for non concorded concept
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptData[updatedSourceID],
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
			}
			continue
		} else if len(result) > 1 {
			//this scenario should never happen
			err = fmt.Errorf("Multiple source concepts found with matching uuid: %s", updatedSourceID)
			logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Error(err.Error())
			return deleteLonePrefUUIDQueries, err
		}

		entityEquivalence := result[0]
		conceptType, err := mapper.MostSpecificType(entityEquivalence.Types)
		if err != nil {
			logger.WithError(err).WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Errorf("could not return most specific type from source node: %v", entityEquivalence.Types)
			return deleteLonePrefUUIDQueries, err
		}

		logger.WithField("UUID", updatedSourceID).Debug("Existing prefUUID is " + entityEquivalence.PrefUUID + " equivalence count is " + strconv.Itoa(entityEquivalence.Equivalence))
		if entityEquivalence.Equivalence == 0 {
			// Source is old as exists in Neo4j without a prefNode. It can be transferred without issue
			continue
		} else if entityEquivalence.Equivalence == 1 {
			// Source exists in neo4j but is not concorded. It can be transferred without issue but its prefNode should be deleted
			if updatedSourceID == entityEquivalence.PrefUUID {
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Pref uuid node for source %s will need to be deleted as its source will be removed", updatedSourceID)
				deleteLonePrefUUIDQueries = append(deleteLonePrefUUIDQueries, deleteLonePrefUUID(entityEquivalence.PrefUUID))
				//concordance added
				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
				continue
			} else {
				// Source is only source concorded to non-matching prefUUID; scenario should NEVER happen
				err := fmt.Errorf("This source id: %s the only concordance to a non-matching node with prefUuid: %s", updatedSourceID, entityEquivalence.PrefUUID)
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingDodgyData").Error(err)
				return deleteLonePrefUUIDQueries, err
			}
		} else {
			if updatedSourceID == entityEquivalence.PrefUUID {
				if updatedSourceID != newAggregatedConcept.PrefUUID {
					authority := getCanonicalAuthority(newAggregatedConcept)
					if entityEquivalence.Authority != authority && stringInArr(entityEquivalence.Authority, concordancesSources) {
						logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).Debugf("Canonical node for main source %s will need to be deleted and all concordances will be transfered to the new concordance", updatedSourceID)
						// just delete the lone prefUUID node because the other concordances to
						// this node should already be in the new sourceRepresentations (aggregate-concept-transformer responsability)
						deleteLonePrefUUIDQueries = append(deleteLonePrefUUIDQueries, deleteLonePrefUUID(entityEquivalence.PrefUUID))
						updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
							ConceptType:   conceptType,
							ConceptUUID:   updatedSourceID,
							AggregateHash: aggregateHash,
							TransactionID: transID,
							EventDetails: ConcordanceEvent{
								Type:  AddedEvent,
								OldID: updatedSourceID,
								NewID: newAggregatedConcept.PrefUUID,
							},
						})
						continue
					}
					// Source is prefUUID for a different concordance
					err := fmt.Errorf("Cannot currently process this record as it will break an existing concordance with prefUuid: %s", updatedSourceID)
					logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingInvalidConcordance").Error(err)
					return deleteLonePrefUUIDQueries, err
				}
			} else {
				// Source was concorded to different concordance. Data on existing concordance is now out of date
				logger.WithTransactionID(transID).WithUUID(newAggregatedConcept.PrefUUID).WithField("alert_tag", "ConceptLoadingStaleData").Infof("Need to re-ingest concordance record for prefUuid: %s as source: %s has been removed.", entityEquivalence.PrefUUID, updatedSourceID)

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: entityEquivalence.PrefUUID,
						NewID: updatedSourceID,
					},
				})

				updateRecord.ChangedRecords = append(updateRecord.ChangedRecords, Event{
					ConceptType:   conceptType,
					ConceptUUID:   updatedSourceID,
					AggregateHash: aggregateHash,
					TransactionID: transID,
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: updatedSourceID,
						NewID: newAggregatedConcept.PrefUUID,
					},
				})
				continue
			}
		}
	}
	return deleteLonePrefUUIDQueries, nil
}

//Clean up canonical nodes of a concept that has become a source of current concept
func deleteLonePrefUUID(prefUUID string) *neoism.CypherQuery {
	logger.WithField("UUID", prefUUID).Debug("Deleting orphaned prefUUID node")
	equivQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {prefUUID:{id}}) DETACH DELETE t`,
		Parameters: map[string]interface{}{
			"id": prefUUID,
		},
	}
	return equivQuery
}

//Clear down current concept node
func (s *ConceptService) clearDownExistingNodes(ac AggregatedConcept) []*neoism.CypherQuery {
	acUUID := ac.PrefUUID

	var queryBatch []*neoism.CypherQuery

	for _, sr := range ac.SourceRepresentations {
		deletePreviousSourceIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (t:Thing {uuid:{id}})
			OPTIONAL MATCH (t)<-[rel:IDENTIFIES]-(i)
			OPTIONAL MATCH (t)-[eq:EQUIVALENT_TO]->(a:Thing)
			OPTIONAL MATCH (t)-[x:HAS_PARENT]->(p)
			OPTIONAL MATCH (t)-[relatedTo:IS_RELATED_TO]->(relNode)
			OPTIONAL MATCH (t)-[supersededBy:SUPERSEDED_BY]->(supersedesNode)
			OPTIONAL MATCH (t)-[broader:HAS_BROADER]->(brNode)
			OPTIONAL MATCH (t)-[ho:HAS_ORGANISATION]->(org)
			OPTIONAL MATCH (t)-[hm:HAS_MEMBER]->(memb)
			OPTIONAL MATCH (t)-[hr:HAS_ROLE]->(mr)
			OPTIONAL MATCH (t)-[issuerRel:ISSUED_BY]->(issuer)
			OPTIONAL MATCH (t)-[parentOrgRel:SUB_ORGANISATION_OF]->(parentOrg)
			OPTIONAL MATCH (t)-[coo:COUNTRY_OF_OPERATIONS]->(managedLocation) 
			OPTIONAL MATCH (t)-[coi:COUNTRY_OF_INCORPORATION]->(managedLocation) 
			OPTIONAL MATCH (t)-[cor:COUNTRY_OF_RISK]->(managedLocation) 
			REMOVE t:%s
			SET t={uuid:{id}}
			DELETE x, rel, i, eq, relatedTo, broader, ho, hm, hr, issuerRel, parentOrgRel, supersededBy, coo, coi, cor`, getLabelsToRemove()),
			Parameters: map[string]interface{}{
				"id": sr.UUID,
			},
		}
		queryBatch = append(queryBatch, deletePreviousSourceIdentifiersLabelsAndPropertiesQuery)
	}

	//cleanUP all the previous Equivalent to relationships
	deletePreviousCanonicalIdentifiersLabelsAndPropertiesQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (t:Thing {prefUUID:{acUUID}})
			OPTIONAL MATCH (t)<-[rel:EQUIVALENT_TO]-(s)
			REMOVE t:%s
			SET t={prefUUID:{acUUID}}
			DELETE rel`, getLabelsToRemove()),
		Parameters: map[string]interface{}{
			"acUUID": acUUID,
		},
	}
	queryBatch = append(queryBatch, deletePreviousCanonicalIdentifiersLabelsAndPropertiesQuery)

	return queryBatch
}

//Curate all queries to populate concept nodes
func populateConceptQueries(queryBatch []*neoism.CypherQuery, aggregatedConcept AggregatedConcept) []*neoism.CypherQuery {
	// Create a sourceConcept from the canonical information - WITH NO UUID
	concept := Concept{
		Aliases:              aggregatedConcept.Aliases,
		DescriptionXML:       aggregatedConcept.DescriptionXML,
		EmailAddress:         aggregatedConcept.EmailAddress,
		FacebookPage:         aggregatedConcept.FacebookPage,
		FigiCode:             aggregatedConcept.FigiCode,
		Hash:                 aggregatedConcept.AggregatedHash,
		ImageURL:             aggregatedConcept.ImageURL,
		InceptionDate:        aggregatedConcept.InceptionDate,
		InceptionDateEpoch:   aggregatedConcept.InceptionDateEpoch,
		IssuedBy:             aggregatedConcept.IssuedBy,
		PrefLabel:            aggregatedConcept.PrefLabel,
		ScopeNote:            aggregatedConcept.ScopeNote,
		ShortLabel:           aggregatedConcept.ShortLabel,
		Strapline:            aggregatedConcept.Strapline,
		TerminationDate:      aggregatedConcept.TerminationDate,
		TerminationDateEpoch: aggregatedConcept.TerminationDateEpoch,
		TwitterHandle:        aggregatedConcept.TwitterHandle,
		Type:                 aggregatedConcept.Type,
		//TODO deprecated event?
		IsDeprecated: aggregatedConcept.IsDeprecated,
		// Organisations
		ProperName:                 aggregatedConcept.ProperName,
		ShortName:                  aggregatedConcept.ShortName,
		TradeNames:                 aggregatedConcept.TradeNames,
		FormerNames:                aggregatedConcept.FormerNames,
		CountryCode:                aggregatedConcept.CountryCode,
		CountryOfIncorporation:     aggregatedConcept.CountryOfIncorporation,
		CountryOfRisk:              aggregatedConcept.CountryOfRisk,
		CountryOfOperations:        aggregatedConcept.CountryOfOperations,
		CountryOfIncorporationUUID: aggregatedConcept.CountryOfIncorporationUUID,
		CountryOfRiskUUID:          aggregatedConcept.CountryOfRiskUUID,
		CountryOfOperationsUUID:    aggregatedConcept.CountryOfOperationsUUID,
		PostalCode:                 aggregatedConcept.PostalCode,
		YearFounded:                aggregatedConcept.YearFounded,
		LeiCode:                    aggregatedConcept.LeiCode,
		// Person
		Salutation: aggregatedConcept.Salutation,
		BirthYear:  aggregatedConcept.BirthYear,
		// Location
		ISO31661: aggregatedConcept.ISO31661,
	}

	queryBatch = append(queryBatch, createNodeQueries(concept, aggregatedConcept.PrefUUID, "")...)

	// Repopulate
	for _, sourceConcept := range aggregatedConcept.SourceRepresentations {
		queryBatch = append(queryBatch, createNodeQueries(sourceConcept, "", sourceConcept.UUID)...)

		equivQuery := &neoism.CypherQuery{
			Statement: `MATCH (t:Thing {uuid:{uuid}}), (c:Thing {prefUUID:{prefUUID}})
						MERGE (t)-[:EQUIVALENT_TO]->(c)`,
			Parameters: map[string]interface{}{
				"uuid":     sourceConcept.UUID,
				"prefUUID": aggregatedConcept.PrefUUID,
			},
		}
		queryBatch = append(queryBatch, equivQuery)

		if len(sourceConcept.RelatedUUIDs) > 0 {
			queryBatch = addRelationship(sourceConcept.UUID, sourceConcept.RelatedUUIDs, "IS_RELATED_TO", queryBatch)
		}

		if len(sourceConcept.BroaderUUIDs) > 0 {
			queryBatch = addRelationship(sourceConcept.UUID, sourceConcept.BroaderUUIDs, "HAS_BROADER", queryBatch)
		}

		if len(sourceConcept.SupersededByUUIDs) > 0 {
			queryBatch = addRelationship(sourceConcept.UUID, sourceConcept.SupersededByUUIDs, "SUPERSEDED_BY", queryBatch)
		}
	}
	return queryBatch
}

//Create concept nodes
func createNodeQueries(concept Concept, prefUUID string, uuid string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
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

	for _, parentUUID := range concept.ParentUUIDs {
		writeParent := &neoism.CypherQuery{
			Statement: `MERGE (o:Thing {uuid: {uuid}})
						MERGE (parentupp:Identifier:UPPIdentifier {value: {parentUUID}})
						MERGE (parent:Thing {uuid: {parentUUID}})
						MERGE (parentupp)-[:IDENTIFIES]->(parent)
						MERGE (o)-[:HAS_PARENT]->(parent)	`,
			Parameters: neoism.Props{
				"parentUUID": parentUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeParent)
	}

	if concept.OrganisationUUID != "" {
		writeOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
						MERGE (orgupp:Identifier:UPPIdentifier {value: {orgUUID}})
						MERGE (org:Thing {uuid: {orgUUID}})
						MERGE (orgupp)-[:IDENTIFIES]->(org)
						MERGE (membership)-[:HAS_ORGANISATION]->(org)`,
			Parameters: neoism.Props{
				"orgUUID": concept.OrganisationUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeOrganisation)
	}

	if concept.PersonUUID != "" {
		writePerson := &neoism.CypherQuery{
			Statement: `MERGE (membership:Thing {uuid: {uuid}})
						MERGE (personupp:Identifier:UPPIdentifier {value: {personUUID}})
						MERGE (person:Thing {uuid: {personUUID}})
						MERGE (personupp)-[:IDENTIFIES]->(person)
						MERGE (membership)-[:HAS_MEMBER]->(person)`,
			Parameters: neoism.Props{
				"personUUID": concept.PersonUUID,
				"uuid":       concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writePerson)
	}

	if uuid != "" && concept.IssuedBy != "" {
		writeFinIns := &neoism.CypherQuery{
			Statement: `MERGE (fi:Thing {uuid: {fiUUID}})
						MERGE (org:Thing {uuid: {orgUUID}})
						MERGE (fi)-[:ISSUED_BY]->(org)
						MERGE (fiupp:Identifier:FIGIIdentifier {value: {fiCode}})
						MERGE (fiupp)-[:IDENTIFIES]->(fi)
						`,
			Parameters: neoism.Props{
				"fiUUID":  concept.UUID,
				"fiCode":  concept.FigiCode,
				"orgUUID": concept.IssuedBy,
			},
		}
		queryBatch = append(queryBatch, writeFinIns)
	}

	if uuid != "" && concept.ParentOrganisation != "" {
		writeParentOrganisation := &neoism.CypherQuery{
			Statement: `MERGE (org:Thing {uuid: {uuid}})
							MERGE (orgUPP:Identifier:UPPIdentifier {value: {orgUUID}})
							MERGE (parentOrg:Thing {uuid: {orgUUID}})
							MERGE (orgUPP)-[:IDENTIFIES]->(parentOrg)
							MERGE (org)-[:SUB_ORGANISATION_OF]->(parentOrg)`,
			Parameters: neoism.Props{
				"orgUUID": concept.ParentOrganisation,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeParentOrganisation)
	}

	if uuid != "" && concept.CountryOfRiskUUID != "" {
		writeCountryOfRisk := &neoism.CypherQuery{
			Statement: `MERGE (org:Thing {uuid: {uuid}})
							MERGE (orgUPP:Identifier:UPPIdentifier {value: {locUUID}})
							MERGE (location:Thing {uuid: {locUUID}})
							MERGE (orgUPP)-[:IDENTIFIES]->(location)
							MERGE (org)-[:COUNTRY_OF_RISK]->(location)`,
			Parameters: neoism.Props{
				"locUUID": concept.CountryOfRiskUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeCountryOfRisk)
	}
	if uuid != "" && concept.CountryOfIncorporationUUID != "" {
		writeCountryOfIncorporation := &neoism.CypherQuery{
			Statement: `MERGE (org:Thing {uuid: {uuid}})
							MERGE (orgUPP:Identifier:UPPIdentifier {value: {locUUID}})
							MERGE (location:Thing {uuid: {locUUID}})
							MERGE (orgUPP)-[:IDENTIFIES]->(location)
							MERGE (org)-[:COUNTRY_OF_INCORPORATION]->(location)`,
			Parameters: neoism.Props{
				"locUUID": concept.CountryOfIncorporationUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeCountryOfIncorporation)
	}
	if uuid != "" && concept.CountryOfOperationsUUID != "" {
		writeCountryOfOperations := &neoism.CypherQuery{
			Statement: `MERGE (org:Thing {uuid: {uuid}})
							MERGE (orgUPP:Identifier:UPPIdentifier {value: {locUUID}})
							MERGE (location:Thing {uuid: {locUUID}})
							MERGE (orgUPP)-[:IDENTIFIES]->(location)
							MERGE (org)-[:COUNTRY_OF_OPERATIONS]->(location)`,
			Parameters: neoism.Props{
				"locUUID": concept.CountryOfOperationsUUID,
				"uuid":    concept.UUID,
			},
		}
		queryBatch = append(queryBatch, writeCountryOfOperations)
	}

	if uuid != "" && len(concept.MembershipRoles) > 0 {
		for _, membershipRole := range concept.MembershipRoles {
			params := neoism.Props{
				"inceptionDate":        nil,
				"inceptionDateEpoch":   nil,
				"terminationDate":      nil,
				"terminationDateEpoch": nil,
				"roleUUID":             membershipRole.RoleUUID,
				"nodeUUID":             concept.UUID,
			}
			if membershipRole.InceptionDate != "" {
				params["inceptionDate"] = membershipRole.InceptionDate
			}
			if membershipRole.InceptionDateEpoch > 0 {
				params["inceptionDateEpoch"] = membershipRole.InceptionDateEpoch
			}
			if membershipRole.TerminationDate != "" {
				params["terminationDate"] = membershipRole.TerminationDate
			}
			if membershipRole.TerminationDateEpoch > 0 {
				params["terminationDateEpoch"] = membershipRole.TerminationDateEpoch
			}
			writeParent := &neoism.CypherQuery{
				Statement: `MERGE (node:Thing{uuid: {nodeUUID}})
							MERGE (role:Thing{uuid: {roleUUID}})
								ON CREATE SET
									role.uuid = {roleUUID}
							MERGE (node)-[rel:HAS_ROLE]->(role)
								ON CREATE SET
									rel.inceptionDate = {inceptionDate},
									rel.inceptionDateEpoch = {inceptionDateEpoch},
									rel.terminationDate = {terminationDate},
									rel.terminationDateEpoch = {terminationDateEpoch}
							`,
				Parameters: params,
			}
			queryBatch = append(queryBatch, writeParent)
		}
	}

	queryBatch = append(queryBatch, createConceptQuery)

	// If no UUID then it is the canonical node and will not have identifier nodes
	if uuid != "" && concept.Type != "Membership" {
		queryBatch = append(queryBatch, addIdentifierNodes(uuid, concept.Authority, concept.AuthorityValue)...)
	}

	return queryBatch

}

//Add relationships to concepts
func addRelationship(conceptID string, relationshipIDs []string, relationshipType string, queryBatch []*neoism.CypherQuery) []*neoism.CypherQuery {
	for _, id := range relationshipIDs {
		addRelationshipQuery := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
						MATCH (o:Concept {uuid: {uuid}})
						MERGE (p:Thing {uuid: {id}})
		            	MERGE (o)-[:%s]->(p)
						MERGE (x:Identifier:UPPIdentifier{value:{id}})
                        MERGE (x)-[:IDENTIFIES]->(p)`, relationshipType),
			Parameters: map[string]interface{}{
				"uuid":         conceptID,
				"id":           id,
				"relationship": relationshipType,
			},
		}
		queryBatch = append(queryBatch, addRelationshipQuery)
	}
	return queryBatch
}

//Create canonical node for any concepts that were removed from a concordance and thus would become lone
func (s *ConceptService) writeCanonicalNodeForUnconcordedConcepts(concept Concept) *neoism.CypherQuery {
	allProps := setProps(concept, concept.UUID, false)
	logger.WithField("UUID", concept.UUID).Debug("Creating prefUUID node for unconcorded concept")
	createCanonicalNodeQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`
					MATCH (t:Thing{uuid:{prefUUID}})
					MERGE (n:Thing {prefUUID: {prefUUID}})
					MERGE (n)<-[:EQUIVALENT_TO]-(t)
					set n={allprops}
					set n :%s`, getAllLabels(concept.Type)),
		Parameters: map[string]interface{}{
			"prefUUID": concept.UUID,
			"allprops": allProps,
		},
	}
	return createCanonicalNodeQuery
}

//return all concept labels
func getAllLabels(conceptType string) string {
	labels := conceptType
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		labels += ":" + parentType
		parentType = mapper.ParentType(parentType)
	}
	return labels
}

//return existing labels
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

//extract uuids of the source concepts
func getSourceData(sourceConcepts []Concept) map[string]string {
	conceptData := make(map[string]string)
	for _, concept := range sourceConcepts {
		conceptData[concept.UUID] = concept.Type
	}
	return conceptData
}

//This function dictates which properties will be actually
//written in neo for both canonical and source nodes.
func setProps(concept Concept, id string, isSource bool) map[string]interface{} {
	nodeProps := map[string]interface{}{}
	//common props
	if concept.PrefLabel != "" {
		nodeProps["prefLabel"] = concept.PrefLabel
	}
	nodeProps["lastModifiedEpoch"] = time.Now().Unix()
	if concept.FigiCode != "" {
		nodeProps["figiCode"] = concept.FigiCode
	}

	if concept.IsDeprecated {
		nodeProps["isDeprecated"] = true
	}
	//source specific props
	if isSource {
		nodeProps["uuid"] = id
		nodeProps["authority"] = concept.Authority
		nodeProps["authorityValue"] = concept.AuthorityValue

		return nodeProps
	}
	//canonical specific props
	nodeProps["prefUUID"] = id
	nodeProps["aggregateHash"] = concept.Hash

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
	if concept.FigiCode != "" {
		nodeProps["figiCode"] = concept.FigiCode
	}
	if concept.ProperName != "" {
		nodeProps["properName"] = concept.ProperName
	}
	if concept.ShortName != "" {
		nodeProps["shortName"] = concept.ShortName
	}
	if len(concept.FormerNames) > 0 {
		nodeProps["formerNames"] = concept.FormerNames
	}
	if len(concept.TradeNames) > 0 {
		nodeProps["tradeNames"] = concept.TradeNames
	}
	if concept.CountryCode != "" {
		nodeProps["countryCode"] = concept.CountryCode
	}
	if concept.CountryOfIncorporation != "" {
		nodeProps["countryOfIncorporation"] = concept.CountryOfIncorporation
	}
	if concept.CountryOfRisk != "" {
		nodeProps["countryOfRisk"] = concept.CountryOfRisk
	}
	if concept.CountryOfOperations != "" {
		nodeProps["countryOfOperations"] = concept.CountryOfOperations
	}
	if concept.PostalCode != "" {
		nodeProps["postalCode"] = concept.PostalCode
	}
	if concept.YearFounded > 0 {
		nodeProps["yearFounded"] = concept.YearFounded
	}
	if concept.LeiCode != "" {
		nodeProps["leiCode"] = concept.LeiCode
	}
	if concept.InceptionDate != "" {
		nodeProps["inceptionDate"] = concept.InceptionDate
	}
	if concept.TerminationDate != "" {
		nodeProps["terminationDate"] = concept.TerminationDate
	}
	if concept.InceptionDateEpoch > 0 {
		nodeProps["inceptionDateEpoch"] = concept.InceptionDateEpoch
	}
	if concept.TerminationDateEpoch > 0 {
		nodeProps["terminationDateEpoch"] = concept.TerminationDateEpoch
	}
	if concept.Salutation != "" {
		nodeProps["salutation"] = concept.Salutation
	}
	if concept.BirthYear > 0 {
		nodeProps["birthYear"] = concept.BirthYear
	}
	if concept.ISO31661 != "" {
		nodeProps["iso31661"] = concept.ISO31661
	}

	return nodeProps
}

//Add identifiers to node
func addIdentifierNodes(UUID string, authority string, authorityValue string) []*neoism.CypherQuery {
	var queryBatch []*neoism.CypherQuery
	//Add Alternative Identifier

	if label, ok := authorityToIdentifierLabelMap[authority]; ok {
		alternativeIdentifierQuery := createNewIdentifierQuery(UUID, label, authorityValue)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)

		uppIdentifierQuery := createNewIdentifierQuery(UUID, authorityToIdentifierLabelMap["UPP"], UUID)
		queryBatch = append(queryBatch, uppIdentifierQuery)
	}

	return queryBatch
}

//Create identifier
func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					MERGE (i:Identifier:%s {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)`, identifierLabel)
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
func (s *ConceptService) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := AggregatedConcept{}
	err := dec.Decode(&sub)
	return sub, sub.PrefUUID, err
}

//Check - checker
func (s *ConceptService) Check() error {
	if err := neoutils.CheckWritable(s.conn); err != nil {
		return err
	}
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

func processMembershipRoles(v interface{}) interface{} {
	switch c := v.(type) {
	case AggregatedConcept:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
		c.MembershipRoles = cleanMembershipRoles(c.MembershipRoles)
		for _, s := range c.SourceRepresentations {
			processMembershipRoles(s)
		}
	case Concept:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
		c.MembershipRoles = cleanMembershipRoles(c.MembershipRoles)
	case MembershipRole:
		c.InceptionDateEpoch = getEpoch(c.InceptionDate)
		c.TerminationDateEpoch = getEpoch(c.TerminationDate)
	}
	return v
}

func cleanMembershipRoles(m []MembershipRole) []MembershipRole {
	deleted := 0
	for i := range m {
		j := i - deleted
		if m[j].RoleUUID == "" {
			m = m[:j+copy(m[j:], m[j+1:])]
			deleted++
			continue
		}
		m[j].InceptionDateEpoch = getEpoch(m[j].InceptionDate)
		m[j].TerminationDateEpoch = getEpoch(m[j].TerminationDate)
	}

	if len(m) == 0 {
		return nil
	}

	return m
}

func getEpoch(t string) int64 {
	if t == "" {
		return 0
	}

	tt, _ := time.Parse(iso8601DateOnly, t)
	return tt.Unix()
}

func filterSlice(a []string) []string {
	r := []string{}
	for _, str := range a {
		if str != "" {
			r = append(r, str)
		}
	}

	if len(r) == 0 {
		return nil
	}

	return a
}

func cleanConcept(c AggregatedConcept) AggregatedConcept {
	for j := range c.SourceRepresentations {
		c.SourceRepresentations[j].LastModifiedEpoch = 0
		for i := range c.SourceRepresentations[j].MembershipRoles {
			c.SourceRepresentations[j].MembershipRoles[i].InceptionDateEpoch = 0
			c.SourceRepresentations[j].MembershipRoles[i].TerminationDateEpoch = 0
		}
		sort.SliceStable(c.SourceRepresentations[j].MembershipRoles[:], func(k, l int) bool {
			return c.SourceRepresentations[j].MembershipRoles[k].RoleUUID < c.SourceRepresentations[j].MembershipRoles[l].RoleUUID
		})
	}
	for i := range c.MembershipRoles {
		c.MembershipRoles[i].InceptionDateEpoch = 0
		c.MembershipRoles[i].TerminationDateEpoch = 0
	}
	sort.SliceStable(c.SourceRepresentations[:], func(k, l int) bool {
		return c.SourceRepresentations[k].UUID < c.SourceRepresentations[l].UUID
	})
	return c
}

func cleanHash(c AggregatedConcept) AggregatedConcept {
	c.AggregatedHash = ""
	return c
}

func cleanSourceProperties(c AggregatedConcept) AggregatedConcept {
	var cleanSources []Concept
	for _, source := range c.SourceRepresentations {
		cleanConcept := Concept{
			UUID:              source.UUID,
			PrefLabel:         source.PrefLabel,
			Type:              source.Type,
			Authority:         source.Authority,
			AuthorityValue:    source.AuthorityValue,
			ParentUUIDs:       source.ParentUUIDs,
			OrganisationUUID:  source.OrganisationUUID,
			PersonUUID:        source.PersonUUID,
			RelatedUUIDs:      source.RelatedUUIDs,
			BroaderUUIDs:      source.BroaderUUIDs,
			SupersededByUUIDs: source.SupersededByUUIDs,
			MembershipRoles:   source.MembershipRoles,
			IssuedBy:          source.IssuedBy,
			FigiCode:          source.FigiCode,
			IsDeprecated:      source.IsDeprecated,
			// Organisations
			ParentOrganisation:         source.ParentOrganisation,
			CountryOfOperationsUUID:    source.CountryOfOperationsUUID,
			CountryOfIncorporationUUID: source.CountryOfIncorporationUUID,
			CountryOfRiskUUID:          source.CountryOfRiskUUID,
		}
		cleanSources = append(cleanSources, cleanConcept)
	}
	c.SourceRepresentations = cleanSources
	return c
}

func getCanonicalAuthority(aggregate AggregatedConcept) string {
	for _, source := range aggregate.SourceRepresentations {
		if source.UUID == aggregate.PrefUUID {
			return source.Authority
		}
	}
	return ""
}

func stringInArr(searchFor string, values []string) bool {
	for _, val := range values {
		if searchFor == val {
			return true
		}
	}
	return false
}
