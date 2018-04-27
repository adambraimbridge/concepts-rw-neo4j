package concepts

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	_ "github.com/joho/godotenv/autoload"
	"github.com/mitchellh/hashstructure"
	"github.com/stretchr/testify/assert"
)

//all uuids to be cleaned from DB
const (
	basicConceptUUID           = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID    = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	yetAnotherBasicConceptUUID = "f7e3fe2d-7496-4d42-b19f-378094efd263"
	simpleSmartlogicTopicUUID  = "abd38d90-2152-11e8-9ac1-da24cd01f044"
	parentUuid                 = "2ef39c2a-da9c-4263-8209-ebfd490d3101"

	boardRoleUUID                     = "aa9ef631-c025-43b2-b0ce-d78d394cc6e6"
	membershipRoleUUID                = "f807193d-337b-412f-b32c-afa14b385819"
	organisationUUID                  = "7f40d291-b3cb-47c4-9bce-18413e9350cf"
	personUUID                        = "35946807-0205-4fc1-8516-bb1ae141659b"
	financialInstrumentUUID           = "475b7b59-66d5-47e2-a273-adc3d1ba8286"
	financialInstrumentSameIssuerUUID = "08c6066c-9356-4e96-abd5-9a4f3726724a"
	financialOrgUUID                  = "4290f028-05e9-4c2d-9f11-61ec59ba081a"
	anotherFinancialOrgUUID           = "230e3a74-694a-4d94-8294-6a45ec1ced26"
	membershipUUID                    = "cbadd9a7-5da9-407a-a5ec-e379460991f2"
	anotherMembershipRoleUUID         = "fe94adc6-ca44-438f-ad8f-0188d4a74987"
	anotherOrganisationUUID           = "7ccf2673-2ec0-4b42-b69e-9a2460b945c6"
	anotherPersonUUID                 = "69a8e241-2bfb-4aed-a441-8489d813c5f7"

	sourceId_1 = "74c94c35-e16b-4527-8ef1-c8bcdcc8f05b"
	sourceId_2 = "de3bcb30-992c-424e-8891-73f5bd9a7d3a"
	sourceId_3 = "5b1d8c31-dfe4-4326-b6a9-6227cb59af1f"

	unknownThingUUID = "b5d7c6b5-db7d-4bce-9d6a-f62195571f92"
)

var (
	membershipRole = MembershipRole{
		RoleUUID:        "f807193d-337b-412f-b32c-afa14b385819",
		InceptionDate:   "2016-01-01",
		TerminationDate: "2017-02-02",
	}
	anotherMembershipRole = MembershipRole{
		RoleUUID:      "fe94adc6-ca44-438f-ad8f-0188d4a74987",
		InceptionDate: "2011-06-27",
	}
	anotherMembershipRole2 = MembershipRole{
		RoleUUID:        "83102635-e6d5-3c48-9d5f-ab34c1401c22",
		InceptionDate:   "2009-09-10",
		TerminationDate: "2013-02-20",
	}
)

//Reusable Neo4J connection
var db neoutils.NeoConnection

//Concept Service under test
var conceptsDriver ConceptService

var emptyList []string

func getSingleConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "The Best Label",
		Type:           "Brand",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "The Best Label",
			Type:           "Brand",
			Strapline:      "Keeping it simple",
			DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
			ImageURL:       "http://media.ft.com/brand.png",
			EmailAddress:   "simple@ft.com",
			FacebookPage:   "#facebookFTComment",
			TwitterHandle:  "@ftComment",
			ScopeNote:      "Comments about stuff",
			ShortLabel:     "Label",
			Authority:      "TME",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getDualConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "The Best Label",
		Type:           "Brand",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				EmailAddress:   "simple@ft.com",
				FacebookPage:   "#facebookFTComment",
				TwitterHandle:  "@ftComment",
				ScopeNote:      "Comments about stuff",
				ShortLabel:     "Label",
				Authority:      "TME",
				AuthorityValue: "1234",
				Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			},
			{
				UUID:           sourceId_1,
				PrefLabel:      "Not as good Label",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<p>Some stuff</p>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "987as3dza654-TME",
			},
		},
	}
}

func getUpdatedDualConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "The Biggest, Bestest, Brandiest Brand",
		Type:           "Brand",
		Strapline:      "Much more complicated",
		DescriptionXML: "<body>One brand to rule them all, one brand to find them; one brand to bring them all and in the darkness bind them</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel"},
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				Strapline:      "Much more complicated",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				EmailAddress:   "simple@ft.com",
				FacebookPage:   "#facebookFTComment",
				TwitterHandle:  "@ftComment",
				ScopeNote:      "Comments about stuff",
				ShortLabel:     "Label",
				Authority:      "TME",
				AuthorityValue: "1234",
				Aliases:        []string{"oneLabel", "secondLabel"},
			},
			{
				UUID:           sourceId_1,
				PrefLabel:      "The Biggest, Bestest, Brandiest Brand",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<body>One brand to rule them all, one brand to find them; one brand to bring them all and in the darkness bind them</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "987as3dza654-TME",
			},
		},
	}
}

func getTriConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       basicConceptUUID,
		PrefLabel:      "The Best Label",
		Type:           "Brand",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				EmailAddress:   "simple@ft.com",
				FacebookPage:   "#facebookFTComment",
				TwitterHandle:  "@ftComment",
				ScopeNote:      "Comments about stuff",
				ShortLabel:     "Label",
				Authority:      "TME",
				AuthorityValue: "1234",
				Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			},
			{
				UUID:           sourceId_1,
				PrefLabel:      "Not as good Label",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<p>Some stuff</p>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "987as3dza654-TME",
			},
			{
				UUID:           sourceId_2,
				PrefLabel:      "Even worse Label",
				Type:           "Brand",
				Strapline:      "Bad strapline",
				DescriptionXML: "<p>More stuff</p>",
				Authority:      "TME",
				AuthorityValue: "123bc3xwa456-TME",
			},
		},
	}
}

func getPrefUUIDAsASource() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       anotherBasicConceptUUID,
		PrefLabel:      "The Best Label",
		Type:           "Brand",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{
			{

				UUID:           anotherBasicConceptUUID,
				PrefLabel:      "Not as good Label",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<p>Some stuff</p>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "987as3dz344-TME",
			},
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				EmailAddress:   "simple@ft.com",
				FacebookPage:   "#facebookFTComment",
				TwitterHandle:  "@ftComment",
				ScopeNote:      "Comments about stuff",
				ShortLabel:     "Label",
				Authority:      "TME",
				AuthorityValue: "1234",
				Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			},
			{
				UUID:           sourceId_2,
				PrefLabel:      "Even worse Label",
				Type:           "Brand",
				Strapline:      "Bad strapline",
				DescriptionXML: "<p>More stuff</p>",
				Authority:      "TME",
				AuthorityValue: "123bc3xwa456-TME",
			},
		},
	}
}

func getTransferSourceConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       anotherBasicConceptUUID,
		PrefLabel:      "A decent label",
		Type:           "Brand",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Short",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{
			{

				UUID:           anotherBasicConceptUUID,
				PrefLabel:      "A decent label",
				Type:           "Brand",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "123abc456-TME",
			},
			{

				UUID:           sourceId_1,
				PrefLabel:      "Not as good Label",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<p>Some stuff</p>",
				ImageURL:       "http://media.ft.com/brand2.png",
				Authority:      "TME",
				AuthorityValue: "987as3dza654-TME",
			},
		},
	}
}

func getTransferMultipleSourceConcordance() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:       simpleSmartlogicTopicUUID,
		PrefLabel:      "The Best Label",
		Type:           "Topic",
		Strapline:      "Keeping it simple",
		DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
		ImageURL:       "http://media.ft.com/brand.png",
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
		Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		SourceRepresentations: []Concept{
			{
				UUID:           simpleSmartlogicTopicUUID,
				PrefLabel:      "A decent label",
				Type:           "Topic",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "Smartlogic",
				AuthorityValue: simpleSmartlogicTopicUUID,
			},
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Section",
				Strapline:      "Keeping it simple",
				DescriptionXML: "<body>This <i>brand</i> has no parent but otherwise has valid values for all fields</body>",
				ImageURL:       "http://media.ft.com/brand.png",
				EmailAddress:   "simple@ft.com",
				FacebookPage:   "#facebookFTComment",
				TwitterHandle:  "@ftComment",
				ScopeNote:      "Comments about stuff",
				ShortLabel:     "Label",
				Authority:      "TME",
				AuthorityValue: "1234",
				Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			},
			{
				UUID:           yetAnotherBasicConceptUUID,
				PrefLabel:      "Concept PrefLabel",
				Type:           "Section",
				Authority:      "TME",
				AuthorityValue: "randomTmeID",
				Aliases:        []string{"oneLabel", "secondLabel"},
			},
		},
	}

}

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
		EmailAddress:   "simple@ft.com",
		FacebookPage:   "#facebookFTComment",
		TwitterHandle:  "@ftComment",
		ScopeNote:      "Comments about stuff",
		ShortLabel:     "Label",
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
			EmailAddress:   "simple@ft.com",
			FacebookPage:   "#facebookFTComment",
			TwitterHandle:  "@ftComment",
			ScopeNote:      "Comments about stuff",
			ShortLabel:     "Label",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getYetAnotherFullLoneAggregatedConcept() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  yetAnotherBasicConceptUUID,
		PrefLabel: "Concept PrefLabel",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           yetAnotherBasicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: yetAnotherBasicConceptUUID,
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
		}},
	}
}

func getLoneTmeSection() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  yetAnotherBasicConceptUUID,
		PrefLabel: "Concept PrefLabel",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           yetAnotherBasicConceptUUID,
			PrefLabel:      "Concept PrefLabel",
			Type:           "Section",
			Authority:      "TME",
			AuthorityValue: "randomTmeID",
			Aliases:        []string{"oneLabel", "secondLabel"},
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

func getConceptWithRelatedTo() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Pref Label",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: basicConceptUUID,
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			RelatedUUIDs:   []string{yetAnotherBasicConceptUUID},
		}}}
}

func getConceptWithRelatedToUnknownThing() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Pref Label",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			RelatedUUIDs:   []string{unknownThingUUID},
		}}}
}

func getConceptWithHasBroaderToUnknownThing() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Pref Label",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			BroaderUUIDs:   []string{unknownThingUUID},
		}}}
}

func getConceptWithHasBroader() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:           basicConceptUUID,
			PrefLabel:      "Pref Label",
			Type:           "Section",
			Authority:      "Smartlogic",
			AuthorityValue: "1234",
			Aliases:        []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			BroaderUUIDs:   []string{yetAnotherBasicConceptUUID},
		}}}
}

func getMembershipRole() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  membershipRole.RoleUUID,
		PrefLabel: "MembershipRole Pref Label",
		Type:      "MembershipRole",
		SourceRepresentations: []Concept{{
			UUID:           membershipRole.RoleUUID,
			PrefLabel:      "MembershipRole Pref Label",
			Type:           "MembershipRole",
			Authority:      "Smartlogic",
			AuthorityValue: "987654321",
		}}}
}

func getBoardRole() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  boardRoleUUID,
		PrefLabel: "BoardRole Pref Label",
		Type:      "BoardRole",
		SourceRepresentations: []Concept{{
			UUID:           boardRoleUUID,
			PrefLabel:      "BoardRole Pref Label",
			Type:           "BoardRole",
			Authority:      "Smartlogic",
			AuthorityValue: "987654321",
		}}}
}

func getMembership() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:         membershipUUID,
		PrefLabel:        "Membership Pref Label",
		Type:             "Membership",
		OrganisationUUID: organisationUUID,
		PersonUUID:       personUUID,
		InceptionDate:    membershipRole.InceptionDate,
		TerminationDate:  membershipRole.TerminationDate,
		MembershipRoles: []MembershipRole{
			membershipRole,
			anotherMembershipRole,
		},
		SourceRepresentations: []Concept{
			{
				UUID:             membershipUUID,
				PrefLabel:        "Membership Pref Label",
				Type:             "Membership",
				Authority:        "Smartlogic",
				AuthorityValue:   "746464",
				OrganisationUUID: organisationUUID,
				PersonUUID:       personUUID,
				InceptionDate:    membershipRole.InceptionDate,
				TerminationDate:  membershipRole.TerminationDate,
				MembershipRoles: []MembershipRole{
					membershipRole,
					anotherMembershipRole,
				},
			},
		},
	}
}

func getOldMembership() Concept {
	return Concept{
		UUID:             membershipUUID,
		PrefLabel:        "Membership Pref Label",
		Type:             "Membership",
		Authority:        "Smartlogic",
		AuthorityValue:   "746464",
		OrganisationUUID: organisationUUID,
		PersonUUID:       personUUID,
		InceptionDate:    "2016-01-01T00:00:00Z",
		TerminationDate:  "2017-02-02T00:00:00Z",
		MembershipRoles: []MembershipRole{
			MembershipRole{
				RoleUUID:        "f807193d-337b-412f-b32c-afa14b385819",
				InceptionDate:   "2016-01-01T00:00:00Z",
				TerminationDate: "2017-02-02T00:00:00Z",
			}, MembershipRole{
				RoleUUID:        "f807193d-337b-412f-b32c-afa14b385819",
				InceptionDate:   "2016-01-01T00:00:00Z",
				TerminationDate: "2017-02-02T00:00:00Z",
			},
		},
	}
}

func getFinancialInstrument() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  financialInstrumentUUID,
		PrefLabel: "FinancialInstrument Pref Label",
		Type:      "FinancialInstrument",
		FigiCode:  "12345",
		IssuedBy:  financialOrgUUID,
		SourceRepresentations: []Concept{{
			UUID:           financialInstrumentUUID,
			PrefLabel:      "FinancialInstrument Pref Label",
			Type:           "FinancialInstrument",
			Authority:      "FACTSET",
			AuthorityValue: "746464",
			FigiCode:       "12345",
			IssuedBy:       financialOrgUUID,
		}},
	}
}

func getFinancialInstrumentWithSameIssuer() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  financialInstrumentSameIssuerUUID,
		PrefLabel: "FinancialInstrument Pref Label 2",
		Type:      "FinancialInstrument",
		FigiCode:  "12345678",
		IssuedBy:  financialOrgUUID,
		SourceRepresentations: []Concept{{
			UUID:           financialInstrumentSameIssuerUUID,
			PrefLabel:      "FinancialInstrument Pref Label 2",
			Type:           "FinancialInstrument",
			Authority:      "FACTSET",
			AuthorityValue: "19283671",
			FigiCode:       "12345678",
			IssuedBy:       financialOrgUUID,
		}},
	}
}

func getUpdatedFinancialInstrument() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  financialInstrumentUUID,
		PrefLabel: "FinancialInstrument Pref Label",
		Type:      "FinancialInstrument",
		FigiCode:  "123457",
		IssuedBy:  anotherFinancialOrgUUID,
		SourceRepresentations: []Concept{{
			UUID:           financialInstrumentUUID,
			PrefLabel:      "FinancialInstrument Pref Label",
			Type:           "FinancialInstrument",
			Authority:      "FACTSET",
			AuthorityValue: "746464",
			FigiCode:       "123457",
			IssuedBy:       anotherFinancialOrgUUID,
		}},
	}
}

func getOrganisation() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:    "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
		Type:        "PublicCompany",
		ProperName:  "Strix Group Plc",
		PrefLabel:   "Strix Group Plc",
		ShortName:   "Strix Group",
		HiddenLabel: "STRIX GROUP PLC",
		FormerNames: []string{
			"Castletown Thermostats",
			"Steam Plc",
		},
		Aliases: []string{
			"Strix Group Plc",
			"STRIX GROUP PLC",
			"Strix Group",
			"Castletown Thermostats",
			"Steam Plc",
		},
		CountryCode:            "GB",
		CountryOfIncorporation: "IM",
		PostalCode:             "IM9 2RG",
		YearFounded:            1951,
		EmailAddress:           "info@strix.com",
		LeiCode:                "213800KZEW5W6BZMNT62",
		SourceRepresentations: []Concept{
			{
				UUID:           "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
				Type:           "PublicCompany",
				Authority:      "FACTSET",
				AuthorityValue: "B000BB-S",
				ProperName:     "Strix Group Plc",
				PrefLabel:      "Strix Group Plc",
				ShortName:      "Strix Group",
				HiddenLabel:    "STRIX GROUP PLC",
				FormerNames: []string{
					"Castletown Thermostats",
					"Steam Plc",
				},
				Aliases: []string{
					"Strix Group Plc",
					"STRIX GROUP PLC",
					"Strix Group",
					"Castletown Thermostats",
					"Steam Plc",
				},
				CountryCode:            "GB",
				CountryOfIncorporation: "IM",
				PostalCode:             "IM9 2RG",
				YearFounded:            1951,
				EmailAddress:           "info@strix.com",
				LeiCode:                "213800KZEW5W6BZMNT62",
			},
		},
	}
}

func getUpdatedMembership() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:         membershipUUID,
		PrefLabel:        "Membership Pref Label",
		Type:             "Membership",
		OrganisationUUID: anotherOrganisationUUID,
		PersonUUID:       anotherPersonUUID,
		InceptionDate:    anotherMembershipRole.InceptionDate,
		TerminationDate:  anotherMembershipRole.TerminationDate,
		MembershipRoles: []MembershipRole{
			anotherMembershipRole,
		},
		SourceRepresentations: []Concept{
			{
				UUID:             membershipUUID,
				PrefLabel:        "Membership Pref Label",
				Type:             "Membership",
				Authority:        "Smartlogic",
				AuthorityValue:   "746464",
				OrganisationUUID: anotherOrganisationUUID,
				PersonUUID:       anotherPersonUUID,
				InceptionDate:    anotherMembershipRole.InceptionDate,
				TerminationDate:  anotherMembershipRole.TerminationDate,
				MembershipRoles: []MembershipRole{
					anotherMembershipRole,
				},
			},
		},
	}
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first
	logger.InitLogger("test-concepts-rw-neo4j", "info")

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
		testName             string
		aggregatedConcept    AggregatedConcept
		otherRelatedConcepts []AggregatedConcept
		errStr               string
		updatedConcepts      UpdatedConcepts
	}{
		{
			testName:             "Throws validation error for invalid concept",
			aggregatedConcept:    AggregatedConcept{PrefUUID: basicConceptUUID},
			otherRelatedConcepts: nil,
			errStr:               "Invalid request, no prefLabel has been supplied",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{},
			},
		},
		{
			testName:             "Creates All Values Present for a Lone Concept",
			aggregatedConcept:    getFullLoneAggregatedConcept(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a MembershipRole",
			aggregatedConcept:    getMembershipRole(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					membershipRoleUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a BoardRole",
			aggregatedConcept:    getBoardRole(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					boardRoleUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a Membership",
			aggregatedConcept:    getMembership(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					membershipUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a FinancialInstrument",
			aggregatedConcept:    getFinancialInstrument(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					financialInstrumentUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a RELATED_TO relationship",
			aggregatedConcept: getConceptWithRelatedTo(),
			otherRelatedConcepts: []AggregatedConcept{
				getYetAnotherFullLoneAggregatedConcept(),
			},
			errStr: "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a Concept with a RELATED_TO relationship to an unknown thing",
			aggregatedConcept:    getConceptWithRelatedToUnknownThing(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:          "Creates All Values Present for a Concept with a HAS_BROADER relationship",
			aggregatedConcept: getConceptWithHasBroader(),
			otherRelatedConcepts: []AggregatedConcept{
				getYetAnotherFullLoneAggregatedConcept(),
			},
			errStr: "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a Concept with a HAS_BROADER relationship to an unknown thing",
			aggregatedConcept:    getConceptWithHasBroaderToUnknownThing(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Creates All Values Present for a Concorded Concept",
			aggregatedConcept:    getFullConcordedAggregatedConcept(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					anotherBasicConceptUUID,
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Creates Handles Special Characters",
			aggregatedConcept:    updateLoneSourceSystemPrefLabel("Herr Ümlaut und Frau Groß"),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{
					basicConceptUUID,
				},
			},
		},
		{
			testName:             "Adding Concept with existing Identifiers fails",
			aggregatedConcept:    getConcordedConceptWithConflictedIdentifier(),
			otherRelatedConcepts: nil,
			errStr:               "already exists with label `TMEIdentifier` and property `value` = '1234'",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{},
			},
		},
		{
			testName:             "Unknown Authority Should Fail",
			aggregatedConcept:    getUnknownAuthority(),
			otherRelatedConcepts: nil,
			errStr:               "Invalid Request",
			updatedConcepts: UpdatedConcepts{
				UpdatedIds: []string{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			defer cleanDB(t)
			// Create the related and broader than concepts
			for _, relatedConcept := range test.otherRelatedConcepts {
				_, err := conceptsDriver.Write(relatedConcept, "")
				assert.NoError(t, err, "Failed to write related/broader concept")
			}

			updatedConcepts, err := conceptsDriver.Write(test.aggregatedConcept, "")
			if test.errStr == "" {
				assert.NoError(t, err, "Failed to write concept")
				readConceptAndCompare(t, test.aggregatedConcept, test.testName)
				assert.Equal(t, test.updatedConcepts, updatedConcepts, "Test "+test.testName+" failed: Updated uuid list differs from expected")

				// Check lone nodes and leaf nodes for identifiers nodes
				// lone node
				if len(test.aggregatedConcept.SourceRepresentations) != 1 {
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

func TestWriteMemberships_Organisation(t *testing.T) {
	defer cleanDB(t)

	org := getOrganisation()
	_, err := conceptsDriver.Write(org, "test_tid")
	assert.NoError(t, err, "Failed to write concept")

	readConceptAndCompare(t, org, "TestWriteMemberships_Organisation")
}

func TestWriteMemberships_CleansUpExisting(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getMembership(), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	ab, err := json.Marshal(cleanHash(result.(AggregatedConcept)))

	originalMembership := AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
	assert.Equal(t, organisationUUID, originalMembership.OrganisationUUID)
	assert.Equal(t, personUUID, originalMembership.PersonUUID)

	_, err = conceptsDriver.Write(getUpdatedMembership(), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	updatedResult, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	cd, err := json.Marshal(cleanHash(updatedResult.(AggregatedConcept)))

	updatedMemebership := AggregatedConcept{}
	json.Unmarshal(cd, &updatedMemebership)

	assert.Equal(t, len(updatedMemebership.MembershipRoles), 1)
	assert.Equal(t, []MembershipRole{anotherMembershipRole}, updatedMemebership.MembershipRoles)
	assert.Equal(t, anotherOrganisationUUID, updatedMemebership.OrganisationUUID)
	assert.Equal(t, anotherPersonUUID, updatedMemebership.PersonUUID)
}

func TestWriteMemberships_FixOldData(t *testing.T) {
	defer cleanDB(t)

	queries := createNodeQueries(getOldMembership(), "", membershipUUID)
	err := db.CypherBatch(queries)
	assert.NoError(t, err, "Failed to write source")

	_, err = conceptsDriver.Write(getMembership(), "test_tid")
	assert.NoError(t, err, "Failed to write membership")

	result, _, err := conceptsDriver.Read(membershipUUID, "test_tid")
	assert.NoError(t, err, "Failed to read membership")
	ab, err := json.Marshal(cleanHash(result.(AggregatedConcept)))

	originalMembership := AggregatedConcept{}
	json.Unmarshal(ab, &originalMembership)

	originalMembership = cleanConcept(originalMembership)

	assert.Equal(t, len(originalMembership.MembershipRoles), 2)
	assert.True(t, reflect.DeepEqual([]MembershipRole{membershipRole, anotherMembershipRole}, originalMembership.MembershipRoles))
	assert.Equal(t, organisationUUID, originalMembership.OrganisationUUID)
	assert.Equal(t, personUUID, originalMembership.PersonUUID)
}

func TestFinancialInstrumentExistingIssuedByRemoved(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getFinancialInstrument(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	_, err = conceptsDriver.Write(getFinancialInstrument(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getFinancialInstrument(), "TestFinancialInstrumentExistingIssuedByRemoved")

	_, err = conceptsDriver.Write(getUpdatedFinancialInstrument(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	_, err = conceptsDriver.Write(getFinancialInstrument(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getFinancialInstrument(), "TestFinancialInstrumentExistingIssuedByRemoved")
}

func TestFinancialInstrumentIssuerOrgRelationRemoved(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getFinancialInstrument(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getFinancialInstrument(), "TestFinancialInstrumentExistingIssuedByRemoved")

	_, err = conceptsDriver.Write(getFinancialInstrumentWithSameIssuer(), "test_tid")
	assert.NoError(t, err, "Failed to write financial instrument")

	readConceptAndCompare(t, getFinancialInstrumentWithSameIssuer(), "TestFinancialInstrumentExistingIssuedByRemoved")
}

func TestWriteService_HandlingConcordance(t *testing.T) {
	tid := "test_tid"
	type testStruct struct {
		testName        string
		setUpConcept    AggregatedConcept
		testConcept     AggregatedConcept
		uuidsToCheck    []string
		returnedError   string
		updatedConcepts UpdatedConcepts
	}
	singleConcordanceNoChangesNoUpdates := testStruct{
		testName:     "singleConcordanceNoChangesNoUpdates",
		setUpConcept: getSingleConcordance(),
		testConcept:  getSingleConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: emptyList,
		},
	}
	dualConcordanceNoChangesNoUpdates := testStruct{
		testName:     "dualConcordanceNoChangesNoUpdates",
		setUpConcept: getDualConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: emptyList,
		},
	}
	singleConcordanceToDualConcordanceUpdatesBoth := testStruct{
		testName:     "singleConcordanceToDualConcordanceUpdatesBoth",
		setUpConcept: getSingleConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				basicConceptUUID,
				sourceId_1,
			},
		},
	}
	dualConcordanceToSingleConcordanceUpdatesBoth := testStruct{
		testName:     "dualConcordanceToSingleConcordanceUpdatesBoth",
		setUpConcept: getDualConcordance(),
		testConcept:  getSingleConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				basicConceptUUID,
				sourceId_1,
			},
		},
	}
	errorsOnAddingConcordanceOfCanonicalNode := testStruct{
		testName:      "errorsOnAddingConcordanceOfCanonicalNode",
		setUpConcept:  getDualConcordance(),
		testConcept:   getPrefUUIDAsASource(),
		returnedError: "Cannot currently process this record as it will break an existing concordance with prefUuid: bbc4f575-edb3-4f51-92f0-5ce6c708d1ea",
	}
	oldCanonicalRemovedWhenSingleConcordancebecomesSource := testStruct{
		testName:     "oldCanonicalRemovedWhenSingleConcordancebecomesSource",
		setUpConcept: getSingleConcordance(),
		testConcept:  getPrefUUIDAsASource(),
		uuidsToCheck: []string{
			anotherBasicConceptUUID,
			basicConceptUUID,
			sourceId_2,
		},
		returnedError: "",
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				basicConceptUUID,
				sourceId_2,
			},
		},
	}
	transferSourceFromOtherConcordanceToAnother := testStruct{
		testName:     "transferSourceFromOtherConcordanceToAnother",
		setUpConcept: getDualConcordance(),
		testConcept:  getTransferSourceConcordance(),
		uuidsToCheck: []string{
			anotherBasicConceptUUID,
			sourceId_1,
			basicConceptUUID,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				sourceId_1,
			},
		},
	}
	addThirdSourceToDualConcordanceUpdateAll := testStruct{
		testName:     "addThirdSourceToDualConcordanceUpdateAll",
		setUpConcept: getDualConcordance(),
		testConcept:  getTriConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
			sourceId_2,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				basicConceptUUID,
				sourceId_1,
				sourceId_2,
			},
		},
	}
	triConcordanceToDualConcordanceUpdatesAll := testStruct{
		testName:     "triConcordanceToDualConcordanceUpdatesAll",
		setUpConcept: getTriConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
			sourceId_2,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				basicConceptUUID,
				sourceId_1,
				sourceId_2,
			},
		},
	}
	dataChangesOnCanonicalUpdateBoth := testStruct{
		testName:     "dataChangesOnCanonicalUpdateBoth",
		setUpConcept: getDualConcordance(),
		testConcept:  getUpdatedDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceId_1,
		},
		updatedConcepts: UpdatedConcepts{
			UpdatedIds: []string{
				basicConceptUUID,
				sourceId_1,
			},
		},
	}

	scenarios := []testStruct{
		singleConcordanceNoChangesNoUpdates,
		dualConcordanceNoChangesNoUpdates,
		singleConcordanceToDualConcordanceUpdatesBoth,
		dualConcordanceToSingleConcordanceUpdatesBoth,
		errorsOnAddingConcordanceOfCanonicalNode,
		transferSourceFromOtherConcordanceToAnother,
		addThirdSourceToDualConcordanceUpdateAll,
		triConcordanceToDualConcordanceUpdatesAll,
		dataChangesOnCanonicalUpdateBoth,
		oldCanonicalRemovedWhenSingleConcordancebecomesSource,
	}

	for _, scenario := range scenarios {
		cleanDB(t)
		//Write data into db, to set up test scenario
		_, err := conceptsDriver.Write(scenario.setUpConcept, tid)
		assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
		verifyAggregateHashIsCorrect(t, scenario.setUpConcept, scenario.testName)
		//Overwrite data with update
		updatedConcepts, err := conceptsDriver.Write(scenario.testConcept, tid)
		if err != nil {
			assert.Contains(t, err.Error(), scenario.returnedError, "Scenario "+scenario.testName+" failed; returned unexpected error")
		}

		assert.Equal(t, scenario.updatedConcepts, updatedConcepts, "Test "+scenario.testName+" failed: Updated uuid list differs from expected")

		for _, id := range scenario.uuidsToCheck {
			conceptIf, found, err := conceptsDriver.Read(id, tid)
			concept := cleanHash(conceptIf.(AggregatedConcept))
			if found {
				assert.NotNil(t, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.True(t, found, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
				verifyAggregateHashIsCorrect(t, scenario.testConcept, scenario.testName)
			} else {
				assert.Equal(t, AggregatedConcept{}, concept, "Scenario "+scenario.testName+" failed; id: "+id+" should return a valid concept")
				assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
			}
		}
		cleanDB(t)
	}

}

func TestMultipleConcordancesAreHandled(t *testing.T) {
	defer cleanDB(t)

	_, err := conceptsDriver.Write(getFullLoneAggregatedConcept(), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	_, err = conceptsDriver.Write(getLoneTmeSection(), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	_, err = conceptsDriver.Write(getTransferMultipleSourceConcordance(), "test_tid")
	assert.NoError(t, err, "Test TestMultipleConcordancesAreHandled failed; returned unexpected error")

	conceptIf, found, err := conceptsDriver.Read(simpleSmartlogicTopicUUID, "test_tid")
	concept := cleanHash(conceptIf.(AggregatedConcept))
	assert.NoError(t, err, "Should be able to read concept with no problems")
	assert.True(t, found, "Concept should exist")
	assert.NotNil(t, concept, "Concept should be populated")
	readConceptAndCompare(t, getTransferMultipleSourceConcordance(), "TestMultipleConcordancesAreHandled")
}

func TestInvalidTypesThrowError(t *testing.T) {
	invalidPrefConceptType := `MERGE (t:Thing{prefUUID:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"}) SET t={prefUUID:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea", prefLabel:"The Best Label"} SET t:Concept:Brand:Unknown MERGE (s:Thing{uuid:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"}) SET s={uuid:"bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"} SET t:Concept:Brand MERGE (t)<-[:EQUIVALENT_TO]-(s)`
	invalidSourceConceptType := `MERGE (t:Thing{prefUUID:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"}) SET t={prefUUID:"4c41f314-4548-4fb6-ac48-4618fcbfa84c", prefLabel:"The Best Label"} SET t:Concept:Brand MERGE (s:Thing{uuid:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"}) SET s={uuid:"4c41f314-4548-4fb6-ac48-4618fcbfa84c"} SET t:Concept:Brand:Unknown MERGE (t)<-[:EQUIVALENT_TO]-(s)`

	type testStruct struct {
		testName         string
		prefUuid         string
		statementToWrite string
		returnedError    error
	}

	invalidPrefConceptTypeTest := testStruct{
		testName:         "invalidPrefConceptTypeTest",
		prefUuid:         basicConceptUUID,
		statementToWrite: invalidPrefConceptType,
		returnedError:    nil,
	}
	invalidSourceConceptTypeTest := testStruct{
		testName:         "invalidSourceConceptTypeTest",
		prefUuid:         anotherBasicConceptUUID,
		statementToWrite: invalidSourceConceptType,
		returnedError:    nil,
	}

	scenarios := []testStruct{invalidPrefConceptTypeTest, invalidSourceConceptTypeTest}

	for _, scenario := range scenarios {
		db.CypherBatch([]*neoism.CypherQuery{{Statement: scenario.statementToWrite}})
		aggConcept, found, err := conceptsDriver.Read(scenario.prefUuid, "")
		assert.Equal(t, AggregatedConcept{}, aggConcept, "Scenario "+scenario.testName+" failed; aggregate concept should be empty")
		assert.Equal(t, false, found, "Scenario "+scenario.testName+" failed; aggregate concept should not be returned from read")
		assert.Error(t, err, "Scenario "+scenario.testName+" failed; read of concept should return error")
		assert.Contains(t, err.Error(), "provided types are not a consistent hierarchy", "Scenario "+scenario.testName+" failed; should throw error from mapper.MostSpecificType function")
	}

	defer cleanDB(t)
}

func TestFilteringOfUniqueIds(t *testing.T) {
	type testStruct struct {
		testName     string
		firstList    []string
		secondList   []string
		filteredList []string
	}

	emptyWhenBothListsAreEmpty := testStruct{
		testName:     "emptyWhenBothListsAreEmpty",
		firstList:    []string{},
		secondList:   []string{},
		filteredList: []string{},
	}
	emptyWhenListsAreTheIdentical := testStruct{
		testName:     "emptyWhenListsAreTheIdentical",
		firstList:    []string{"1", "2", "3"},
		secondList:   []string{"1", "2", "3"},
		filteredList: []string{},
	}
	emptyWhenListsHaveSameIdsInDifferentOrder := testStruct{
		testName:     "emptyWhenListsHaveSameIdsInDifferentOrder",
		firstList:    []string{"1", "2", "3"},
		secondList:   []string{"2", "3", "1"},
		filteredList: []string{},
	}
	hasCompleteFirstListWhenSecondListIsEmpty := testStruct{
		testName:     "hasCompleteSecondListWhenFirstListIsEmpty",
		firstList:    []string{"1", "2", "3"},
		secondList:   []string{},
		filteredList: []string{"1", "2", "3"},
	}
	properlyFiltersWhen1IdIsUnique := testStruct{
		testName:     "properlyFiltersWhen1IdIsUnique",
		firstList:    []string{"1", "2", "3"},
		secondList:   []string{"1", "2"},
		filteredList: []string{"3"},
	}
	properlyFiltersWhen2IdsAreUnique := testStruct{
		testName:     "properlyFiltersWhen2IdsAreUnique",
		firstList:    []string{"1", "2", "3"},
		secondList:   []string{"2"},
		filteredList: []string{"1", "3"},
	}

	Scenarios := []testStruct{
		emptyWhenBothListsAreEmpty,
		emptyWhenListsAreTheIdentical,
		emptyWhenListsHaveSameIdsInDifferentOrder,
		hasCompleteFirstListWhenSecondListIsEmpty,
		properlyFiltersWhen1IdIsUnique,
		properlyFiltersWhen2IdsAreUnique,
	}

	for _, scenario := range Scenarios {
		returnedList := filterIdsThatAreUniqueToFirstList(scenario.firstList, scenario.secondList)
		assert.Equal(t, scenario.filteredList, returnedList, "Scenario: "+scenario.testName+" returned unexpected results")
	}
}

func TestTransferConcordance(t *testing.T) {
	statement := `MERGE (a:Thing{prefUUID:"1"}) MERGE (b:Thing{uuid:"1"}) MERGE (c:Thing{uuid:"2"}) MERGE (d:Thing{uuid:"3"}) MERGE (w:Thing{prefUUID:"4"}) MERGE (y:Thing{uuid:"5"}) MERGE (j:Thing{prefUUID:"6"}) MERGE (k:Thing{uuid:"6"}) MERGE (c)-[:EQUIVALENT_TO]->(a)<-[:EQUIVALENT_TO]-(b) MERGE (w)<-[:EQUIVALENT_TO]-(d) MERGE (j)<-[:EQUIVALENT_TO]-(k)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	emptyQuery := []*neoism.CypherQuery{}

	type testStruct struct {
		testName         string
		updatedSourceIds []string
		returnResult     bool
		returnedError    error
	}

	nodeHasNoConconcordance := testStruct{
		testName:         "nodeHasNoConconcordance",
		updatedSourceIds: []string{"5"},
		returnedError:    nil,
	}
	nodeHasExistingConcordanceWhichWouldCauseDataIssues := testStruct{
		testName:         "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: []string{"1"},
		returnedError:    errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasExistingConcordanceWhichNeedsToBeReWritten := testStruct{
		testName:         "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: []string{"2"},
		returnedError:    nil,
	}
	nodeHasInvalidConcordance := testStruct{
		testName:         "nodeHasInvalidConcordance",
		updatedSourceIds: []string{"3"},
		returnedError:    errors.New("This source id: 3 the only concordance to a non-matching node with prefUuid: 4"),
	}
	nodeIsPrefUuidForExistingConcordance := testStruct{
		testName:         "nodeIsPrefUuidForExistingConcordance",
		updatedSourceIds: []string{"1"},
		returnedError:    errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted := testStruct{
		testName:         "nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted",
		updatedSourceIds: []string{"6"},
		returnResult:     true,
		returnedError:    nil}

	scenarios := []testStruct{
		nodeHasNoConconcordance,
		nodeHasExistingConcordanceWhichWouldCauseDataIssues,
		nodeHasExistingConcordanceWhichNeedsToBeReWritten,
		nodeHasInvalidConcordance,
		nodeIsPrefUuidForExistingConcordance,
		nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, "", "")
		assert.Equal(t, scenario.returnedError, err, "Scenario "+scenario.testName+" returned unexpected error")
		if scenario.returnResult == true {
			assert.NotEqual(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			break
		}
		assert.Equal(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5", "6")
	defer deleteConcordedNodes(t, "1", "4", "6")
}

func TestObjectFieldValidationCorrectlyWorks(t *testing.T) {
	defer cleanDB(t)

	type testStruct struct {
		testName      string
		aggConcept    AggregatedConcept
		returnedError string
	}

	aggregateConceptNoPrefLabel := AggregatedConcept{
		PrefUUID: basicConceptUUID,
	}
	aggregateConceptNoType := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
	}
	aggregateConceptNoSourceReps := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
	}
	sourceRepNoPrefLabel := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID: basicConceptUUID,
			},
		},
	}
	sourceRepNoType := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:      basicConceptUUID,
				PrefLabel: "The Best Label",
			},
		},
	}
	sourceRepNoAuthorityValue := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:      basicConceptUUID,
				PrefLabel: "The Best Label",
				Type:      "Brand",
			},
		},
	}
	returnNoError := AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "The Best Label",
		Type:      "Brand",
		SourceRepresentations: []Concept{
			{
				UUID:           basicConceptUUID,
				PrefLabel:      "The Best Label",
				Type:           "Brand",
				AuthorityValue: "123456-UPP",
			},
		},
	}
	testAggregateConceptNoPrefLabel := testStruct{
		testName:      "testAggregateConceptNoPrefLabel",
		aggConcept:    aggregateConceptNoPrefLabel,
		returnedError: "Invalid request, no prefLabel has been supplied",
	}
	testAggregateConceptNoType := testStruct{
		testName:      "testAggregateConceptNoType",
		aggConcept:    aggregateConceptNoType,
		returnedError: "Invalid request, no type has been supplied",
	}
	testAggregateConceptNoSourceReps := testStruct{
		testName:      "testAggregateConceptNoSourceReps",
		aggConcept:    aggregateConceptNoSourceReps,
		returnedError: "Invalid request, no sourceRepresentation has been supplied",
	}
	testSourceRepNoPrefLabel := testStruct{
		testName:      "testSourceRepNoPrefLabel",
		aggConcept:    sourceRepNoPrefLabel,
		returnedError: "Invalid request, no sourceRepresentation.prefLabel has been supplied",
	}
	testSourceRepNoType := testStruct{
		testName:      "testSourceRepNoType",
		aggConcept:    sourceRepNoType,
		returnedError: "Invalid request, no sourceRepresentation.type has been supplied",
	}
	testSourceRepNoAuthorityValue := testStruct{
		testName:      "testSourceRepNoAuthorityValue",
		aggConcept:    sourceRepNoAuthorityValue,
		returnedError: "Invalid request, no sourceRepresentation.authorityValue has been supplied",
	}
	returnNoErrorTest := testStruct{
		testName:      "returnNoErrorTest",
		aggConcept:    returnNoError,
		returnedError: "",
	}

	scenarios := []testStruct{
		testAggregateConceptNoPrefLabel,
		testAggregateConceptNoType,
		testAggregateConceptNoSourceReps,
		testSourceRepNoPrefLabel,
		testSourceRepNoType,
		testSourceRepNoAuthorityValue,
		returnNoErrorTest,
	}

	for _, scenario := range scenarios {
		err := validateObject(scenario.aggConcept, "transaction_id")
		if err != nil {
			assert.Contains(t, err.Error(), scenario.returnedError, scenario.testName)
		} else {
			assert.NoError(t, err, scenario.testName)
		}
	}
}

func readConceptAndCompare(t *testing.T, payload AggregatedConcept, testName string) {
	expectedIf, found, err := conceptsDriver.Read(payload.PrefUUID, "")
	expected := expectedIf.(AggregatedConcept)

	expected = cleanHash(cleanConcept(expected))
	clean := cleanSourceProperties(payload)
	actual := cleanHash(cleanConcept(clean))

	assert.Equal(t, expected, actual, fmt.Sprintf("Test %s failed: Concepts were not equal", testName))
	assert.NoError(t, err, fmt.Sprintf("Test %s failed: Unexpected Error occurred", testName))
	assert.True(t, found, fmt.Sprintf("Test %s failed: Concept has not been found", testName))
}

func neoUrl() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}
	return url
}

func getConceptService(t *testing.T) ConceptService {
	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(neoUrl(), conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewConceptService(db)
	service.Initialise()
	return service
}

func cleanDB(t *testing.T) {
	cleanSourceNodes(t,
		parentUuid,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceId_1,
		sourceId_2,
		sourceId_3,
		unknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
	)
	deleteSourceNodes(t,
		parentUuid,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceId_1,
		sourceId_2,
		sourceId_3,
		unknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
	)
	deleteConcordedNodes(t,
		parentUuid,
		basicConceptUUID,
		anotherBasicConceptUUID,
		sourceId_1,
		sourceId_2,
		sourceId_3,
		unknownThingUUID,
		yetAnotherBasicConceptUUID,
		membershipRole.RoleUUID,
		personUUID,
		organisationUUID,
		membershipUUID,
		anotherMembershipRole.RoleUUID,
		anotherOrganisationUUID,
		anotherPersonUUID,
		simpleSmartlogicTopicUUID,
		boardRoleUUID,
		financialInstrumentSameIssuerUUID,
		financialInstrumentUUID,
		financialOrgUUID,
		anotherFinancialOrgUUID,
	)
}

func deleteSourceNodes(t *testing.T, uuids ...string) {
	qs := make([]*neoism.CypherQuery, len(uuids))
	for i, uuid := range uuids {
		qs[i] = &neoism.CypherQuery{
			Statement: fmt.Sprintf(`
			MATCH (a:Thing {uuid: "%s"})
			OPTIONAL MATCH (a)-[rel:IDENTIFIES]-(i)
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
	var results []struct {
		Value string `json:"i.value"`
	}

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

func verifyAggregateHashIsCorrect(t *testing.T, concept AggregatedConcept, testName string) {
	var results []struct {
		Hash string `json:"a.aggregateHash"`
	}

	query := &neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {prefUUID: {uuid}})
			RETURN a.aggregateHash`,
		Parameters: map[string]interface{}{
			"uuid": concept.PrefUUID,
		},
		Result: &results,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{query})
	assert.NoError(t, err, fmt.Sprintf("Error while retrieving concept hash"))

	conceptHash, _ := hashstructure.Hash(cleanSourceProperties(concept), nil)
	hashAsString := strconv.FormatUint(conceptHash, 10)
	assert.Equal(t, hashAsString, results[0].Hash, fmt.Sprintf("Test %s failed: Concept hash %s and stored record %s are not equal!", testName, hashAsString, results[0].Hash))
}
