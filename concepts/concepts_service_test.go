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

	"sort"
	"strings"

	logger "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/mitchellh/hashstructure"
	"github.com/stretchr/testify/assert"
)

//all uuids to be cleaned from DB
const (
	basicConceptUUID           = "bbc4f575-edb3-4f51-92f0-5ce6c708d1ea"
	anotherBasicConceptUUID    = "4c41f314-4548-4fb6-ac48-4618fcbfa84c"
	yetAnotherBasicConceptUUID = "f7e3fe2d-7496-4d42-b19f-378094efd263"
	simpleSmartlogicTopicUUID  = "abd38d90-2152-11e8-9ac1-da24cd01f044"
	parentUUID                 = "2ef39c2a-da9c-4263-8209-ebfd490d3101"

	boardRoleUUID                     = "aa9ef631-c025-43b2-b0ce-d78d394cc6e6"
	membershipRoleUUID                = "f807193d-337b-412f-b32c-afa14b385819"
	organisationUUID                  = "7f40d291-b3cb-47c4-9bce-18413e9350cf"
	personUUID                        = "35946807-0205-4fc1-8516-bb1ae141659b"
	financialInstrumentUUID           = "475b7b59-66d5-47e2-a273-adc3d1ba8286"
	financialInstrumentSameIssuerUUID = "08c6066c-9356-4e96-abd5-9a4f3726724a"
	financialOrgUUID                  = "4290f028-05e9-4c2d-9f11-61ec59ba081a"
	anotherFinancialOrgUUID           = "230e3a74-694a-4d94-8294-6a45ec1ced26"
	membershipUUID                    = "cbadd9a7-5da9-407a-a5ec-e379460991f2"
	anotherOrganisationUUID           = "7ccf2673-2ec0-4b42-b69e-9a2460b945c6"
	anotherPersonUUID                 = "69a8e241-2bfb-4aed-a441-8489d813c5f7"
	testOrgUUID                       = "c28fa0b4-4245-11e8-842f-0ed5f89f718b"
	parentOrgUUID                     = "c001ee9c-94c5-11e8-8f42-da24cd01f044"
	locationUUID                      = "82cba3ce-329b-3010-b29d-4282a215889f"

	supersededByUUID = "1a96ee7a-a4af-3a56-852c-60420b0b8da6"

	sourceID1 = "74c94c35-e16b-4527-8ef1-c8bcdcc8f05b"
	sourceID2 = "de3bcb30-992c-424e-8891-73f5bd9a7d3a"
	sourceID3 = "5b1d8c31-dfe4-4326-b6a9-6227cb59af1f"

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
				UUID:           sourceID1,
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
				UUID:           sourceID1,
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
				UUID:           sourceID1,
				PrefLabel:      "Not as good Label",
				Type:           "Brand",
				Strapline:      "Boring strapline",
				DescriptionXML: "<p>Some stuff</p>",
				ImageURL:       "http://media.ft.com/brand.png",
				Authority:      "TME",
				AuthorityValue: "987as3dza654-TME",
			},
			{
				UUID:           sourceID2,
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
				UUID:           sourceID2,
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

				UUID:           sourceID1,
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
			ParentUUIDs:    []string{parentUUID},
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

func getConceptWithSupersededByUUIDs() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  basicConceptUUID,
		PrefLabel: "Pref Label",
		Type:      "Section",
		SourceRepresentations: []Concept{{
			UUID:              basicConceptUUID,
			PrefLabel:         "Pref Label",
			Type:              "Section",
			Authority:         "Smartlogic",
			AuthorityValue:    "1234",
			Aliases:           []string{"oneLabel", "secondLabel", "anotherOne", "whyNot"},
			SupersededByUUIDs: []string{supersededByUUID},
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
		Salutation: "Mr",
		BirthYear:  2018,
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
				Salutation: "Mr",
				BirthYear:  2018,
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
			UUID:               financialInstrumentUUID,
			PrefLabel:          "FinancialInstrument Pref Label",
			Type:               "FinancialInstrument",
			Authority:          "FACTSET",
			AuthorityValue:     "746464",
			FigiCode:           "12345",
			ParentOrganisation: parentOrgUUID,
			IssuedBy:           financialOrgUUID,
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
			UUID:               financialInstrumentSameIssuerUUID,
			PrefLabel:          "FinancialInstrument Pref Label 2",
			Type:               "FinancialInstrument",
			Authority:          "FACTSET",
			AuthorityValue:     "19283671",
			FigiCode:           "12345678",
			ParentOrganisation: parentOrgUUID,
			IssuedBy:           financialOrgUUID,
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
			UUID:               financialInstrumentUUID,
			PrefLabel:          "FinancialInstrument Pref Label",
			Type:               "FinancialInstrument",
			Authority:          "FACTSET",
			AuthorityValue:     "746464",
			FigiCode:           "123457",
			ParentOrganisation: parentOrgUUID,
			IssuedBy:           anotherFinancialOrgUUID,
		}},
	}
}

func getOrganisation() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:   testOrgUUID,
		Type:       "PublicCompany",
		ProperName: "Strix Group Plc",
		PrefLabel:  "Strix Group Plc",
		ShortName:  "Strix Group",
		TradeNames: []string{
			"STRIX GROUP PLC",
		},
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
				UUID:           testOrgUUID,
				Type:           "PublicCompany",
				Authority:      "FACTSET",
				AuthorityValue: "B000BB-S",
				ProperName:     "Strix Group Plc",
				PrefLabel:      "Strix Group Plc",
				ShortName:      "Strix Group",
				TradeNames: []string{
					"STRIX GROUP PLC",
				},
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
				ParentOrganisation:     parentOrgUUID,
			},
		},
	}
}

func getUpdatedOrganisation() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:   testOrgUUID,
		Type:       "PublicCompany",
		ProperName: "Strix Group Plc",
		PrefLabel:  "Strix Group Plc",
		ShortName:  "Strix Group",
		TradeNames: []string{
			"STRIX GROUP PLC",
		},
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
		CountryCode:            "GB 2",
		CountryOfIncorporation: "IM 2",
		PostalCode:             "IM9 2RG 2",
		YearFounded:            1951,
		EmailAddress:           "info@strix.com",
		LeiCode:                "213800KZEW5W6BZMNT62",
		SourceRepresentations: []Concept{
			{
				UUID:           testOrgUUID,
				Type:           "PublicCompany",
				Authority:      "FACTSET",
				AuthorityValue: "B000BB-S",
				ProperName:     "Strix Group Plc",
				PrefLabel:      "Strix Group Plc",
				ShortName:      "Strix Group",
				TradeNames: []string{
					"STRIX GROUP PLC",
				},
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
				CountryCode:            "GB 2",
				CountryOfIncorporation: "IM 2",
				PostalCode:             "IM9 2RG 2",
				YearFounded:            1951,
				EmailAddress:           "info@strix.com",
				LeiCode:                "213800KZEW5W6BZMNT62",
				ParentOrganisation:     parentOrgUUID,
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

func getLocation() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label",
		Type:      "Location",
		SourceRepresentations: []Concept{{
			UUID:           locationUUID,
			PrefLabel:      "Location Pref Label",
			Type:           "Location",
			Authority:      "ManagedLocation",
			AuthorityValue: locationUUID,
		}},
	}
}

func getLocationWithISO31661() AggregatedConcept {
	return AggregatedConcept{
		PrefUUID:  locationUUID,
		PrefLabel: "Location Pref Label 2",
		Type:      "Location",
		Aliases: []string{
			"Bulgaria",
			"Bulgarie",
			"Bulgarien",
		},
		ISO31661: "BG",
		SourceRepresentations: []Concept{{
			UUID:           locationUUID,
			PrefLabel:      "Location Pref Label 2",
			Type:           "Location",
			Authority:      "ManagedLocation",
			AuthorityValue: locationUUID,
			Aliases: []string{
				"Bulgaria",
				"Bulgarie",
				"Bulgarien",
			},
			ISO31661: "BG",
		}},
	}
}

func init() {
	// We are initialising a lot of constraints on an empty database therefore we need the database to be fit before
	// we run tests so initialising the service will create the constraints first
	logger.InitLogger("test-concepts-rw-neo4j", "info")

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, _ = neoutils.Connect(newURL(), conf)
	if db == nil {
		panic("Cannot connect to Neo4J")
	}
	conceptsDriver = NewConceptService(db)
	conceptsDriver.Initialise()

	duration := 5 * time.Second
	time.Sleep(duration)
}

func TestWriteService(t *testing.T) {
	defer cleanDB(t)

	tests := []struct {
		testName             string
		aggregatedConcept    AggregatedConcept
		otherRelatedConcepts []AggregatedConcept
		errStr               string
		updatedConcepts      ConceptChanges
	}{
		{
			testName:             "Throws validation error for invalid concept",
			aggregatedConcept:    AggregatedConcept{PrefUUID: basicConceptUUID},
			otherRelatedConcepts: nil,
			errStr:               "Invalid request, no prefLabel has been supplied",
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:             "Creates All Values Present for a Lone Concept",
			aggregatedConcept:    getFullLoneAggregatedConcept(),
			otherRelatedConcepts: nil,
			errStr:               "",
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "12591793372790141578",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "MembershipRole",
						ConceptUUID:   membershipRoleUUID,
						AggregateHash: "14640755948418282404",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "BoardRole",
						ConceptUUID:   boardRoleUUID,
						AggregateHash: "12533493287104314664",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Membership",
						ConceptUUID:   membershipUUID,
						AggregateHash: "12174081352968985291",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "FinancialInstrument",
						ConceptUUID:   financialInstrumentUUID,
						AggregateHash: "10908149922168548759",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "11988934631458519545",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "1866321902309950776",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "17783168711585993926",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "4347657290564856411",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   anotherBasicConceptUUID,
						AggregateHash: "8075388916260734914",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   anotherBasicConceptUUID,
						AggregateHash: "8075388916260734914",
						EventDetails: ConcordanceEvent{
							Type:  AddedEvent,
							OldID: anotherBasicConceptUUID,
							NewID: basicConceptUUID,
						},
					},
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "8075388916260734914",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				ChangedRecords: []Event{
					{
						ConceptType:   "Section",
						ConceptUUID:   basicConceptUUID,
						AggregateHash: "7950394790968879608",
						EventDetails: ConceptEvent{
							Type: UpdatedEvent,
						},
					},
				},
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
			updatedConcepts: ConceptChanges{
				UpdatedIds: []string{},
			},
		},
		{
			testName:             "Unknown Authority Should Fail",
			aggregatedConcept:    getUnknownAuthority(),
			otherRelatedConcepts: nil,
			errStr:               "Invalid Request",
			updatedConcepts: ConceptChanges{
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

	upOrg := getUpdatedOrganisation()
	_, err = conceptsDriver.Write(upOrg, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, upOrg, "TestWriteMemberships_Organisation.Updated")
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
	assert.Equal(t, "Mr", originalMembership.Salutation)
	assert.Equal(t, 2018, originalMembership.BirthYear)

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
		updatedConcepts ConceptChanges
		customAssertion func(t *testing.T, concept AggregatedConcept)
	}
	singleConcordanceNoChangesNoUpdates := testStruct{
		testName:     "singleConcordanceNoChangesNoUpdates",
		setUpConcept: getSingleConcordance(),
		testConcept:  getSingleConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			UpdatedIds: emptyList,
		},
	}
	dualConcordanceNoChangesNoUpdates := testStruct{
		testName:     "dualConcordanceNoChangesNoUpdates",
		setUpConcept: getDualConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			UpdatedIds: emptyList,
		},
	}
	singleConcordanceToDualConcordanceUpdatesBoth := testStruct{
		testName:     "singleConcordanceToDualConcordanceUpdatesBoth",
		setUpConcept: getSingleConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "8376993856177577898",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "8376993856177577898",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID1,
						NewID: basicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "8376993856177577898",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
			},
		},
	}
	dualConcordanceToSingleConcordanceUpdatesBoth := testStruct{
		testName:     "dualConcordanceToSingleConcordanceUpdatesBoth",
		setUpConcept: getDualConcordance(),
		testConcept:  getSingleConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "17376643100845455947",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID1,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "17376643100845455947",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
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
			sourceID2,
		},
		returnedError: "",
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "7024510897749121461",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: basicConceptUUID,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "7024510897749121461",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "7024510897749121461",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID2,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   anotherBasicConceptUUID,
					AggregateHash: "7024510897749121461",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				basicConceptUUID,
				sourceID2,
			},
		},
	}
	transferSourceFromOneConcordanceToAnother := testStruct{
		testName:     "transferSourceFromOneConcordanceToAnother",
		setUpConcept: getDualConcordance(),
		testConcept:  getTransferSourceConcordance(),
		uuidsToCheck: []string{
			anotherBasicConceptUUID,
			sourceID1,
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "10511029648263458857",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID1,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID1,
					AggregateHash: "10511029648263458857",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID1,
						NewID: anotherBasicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   anotherBasicConceptUUID,
					AggregateHash: "10511029648263458857",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				anotherBasicConceptUUID,
				sourceID1,
			},
		},
	}
	addThirdSourceToDualConcordanceUpdateAll := testStruct{
		testName:     "addThirdSourceToDualConcordanceUpdateAll",
		setUpConcept: getDualConcordance(),
		testConcept:  getTriConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
			sourceID2,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "8286823545264502062",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "8286823545264502062",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  AddedEvent,
						OldID: sourceID2,
						NewID: basicConceptUUID,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "8286823545264502062",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
				sourceID2,
			},
		},
	}
	triConcordanceToDualConcordanceUpdatesAll := testStruct{
		testName:     "triConcordanceToDualConcordanceUpdatesAll",
		setUpConcept: getTriConcordance(),
		testConcept:  getDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
			sourceID2,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   sourceID2,
					AggregateHash: "8376993856177577898",
					TransactionID: "test_tid",
					EventDetails: ConcordanceEvent{
						Type:  RemovedEvent,
						OldID: basicConceptUUID,
						NewID: sourceID2,
					},
				},
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "8376993856177577898",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
				sourceID2,
			},
		},
	}
	dataChangesOnCanonicalUpdateBoth := testStruct{
		testName:     "dataChangesOnCanonicalUpdateBoth",
		setUpConcept: getDualConcordance(),
		testConcept:  getUpdatedDualConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
			sourceID1,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "982906849716893457",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
				sourceID1,
			},
		},
	}
	singleConcordanceDeprecationChangesUpdates := testStruct{
		testName:     "singleConcordanceDeprecationChangesUpdates",
		setUpConcept: getSingleConcordance(),
		testConcept: func() AggregatedConcept {
			concept := getSingleConcordance()
			concept.IsDeprecated = true
			concept.SourceRepresentations[0].IsDeprecated = true
			return concept
		}(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "10525957566524362520",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
	}
	singleConcordanceSupersededByAddRelationship := testStruct{
		testName:     "singleConcordanceSupersededByAddRelationship",
		setUpConcept: getSingleConcordance(),
		testConcept: func() AggregatedConcept {
			concept := getSingleConcordance()
			concept.SourceRepresentations[0].SupersededByUUIDs = []string{supersededByUUID}
			return concept
		}(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "6290510617827722994",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
		customAssertion: func(t *testing.T, concept AggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Lenf(t, concept.SourceRepresentations[0].SupersededByUUIDs, 1, "Test %s failed. Different number of supersededByUUIDs items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Equalf(t, supersededByUUID, concept.SourceRepresentations[0].SupersededByUUIDs[0], "Test %s failed. Different supersededByUUID than expected", "singleConcordanceSupersededByRemoveRelationship")
		},
	}
	singleConcordanceSupersededByRemoveRelationship := testStruct{
		testName:     "singleConcordanceSupersededByRemoveRelationship",
		setUpConcept: getConceptWithSupersededByUUIDs(),
		testConcept:  getSingleConcordance(),
		uuidsToCheck: []string{
			basicConceptUUID,
		},
		updatedConcepts: ConceptChanges{
			ChangedRecords: []Event{
				{
					ConceptType:   "Brand",
					ConceptUUID:   basicConceptUUID,
					AggregateHash: "17376643100845455947",
					TransactionID: "test_tid",
					EventDetails: ConceptEvent{
						Type: UpdatedEvent,
					},
				},
			},
			UpdatedIds: []string{
				basicConceptUUID,
			},
		},
		customAssertion: func(t *testing.T, concept AggregatedConcept) {
			assert.Lenf(t, concept.SourceRepresentations, 1, "Test %s failed. Different number of sourceRepresentation items than expected", "singleConcordanceSupersededByRemoveRelationship")
			assert.Emptyf(t, concept.SourceRepresentations[0].SupersededByUUIDs, "Test %s failed. No supersededByUUIDs content expected", "singleConcordanceSupersededByRemoveRelationship")
		},
	}

	scenarios := []testStruct{
		singleConcordanceNoChangesNoUpdates,
		dualConcordanceNoChangesNoUpdates,
		singleConcordanceToDualConcordanceUpdatesBoth,
		dualConcordanceToSingleConcordanceUpdatesBoth,
		errorsOnAddingConcordanceOfCanonicalNode,
		oldCanonicalRemovedWhenSingleConcordancebecomesSource,
		transferSourceFromOneConcordanceToAnother,
		addThirdSourceToDualConcordanceUpdateAll,
		triConcordanceToDualConcordanceUpdatesAll,
		dataChangesOnCanonicalUpdateBoth,
		singleConcordanceDeprecationChangesUpdates,
		singleConcordanceSupersededByAddRelationship,
		singleConcordanceSupersededByRemoveRelationship,
	}

	for _, scenario := range scenarios {
		cleanDB(t)
		//Write data into db, to set up test scenario
		_, err := conceptsDriver.Write(scenario.setUpConcept, tid)
		assert.NoError(t, err, "Scenario "+scenario.testName+" failed; returned unexpected error")
		verifyAggregateHashIsCorrect(t, scenario.setUpConcept, scenario.testName)
		//Overwrite data with update
		output, err := conceptsDriver.Write(scenario.testConcept, tid)
		actualChanges := output.(ConceptChanges)
		sort.Slice(actualChanges.ChangedRecords, func(i, j int) bool {
			l, _ := json.Marshal(actualChanges.ChangedRecords[i])
			r, _ := json.Marshal(actualChanges.ChangedRecords[j])
			c := strings.Compare(string(l), string(r))
			if c >= 0 {
				return true
			}
			return false
		})
		sort.Slice(scenario.updatedConcepts.ChangedRecords, func(i, j int) bool {
			l, _ := json.Marshal(scenario.updatedConcepts.ChangedRecords[i])
			r, _ := json.Marshal(scenario.updatedConcepts.ChangedRecords[j])
			c := strings.Compare(string(l), string(r))
			if c >= 0 {
				return true
			}
			return false
		})
		if err != nil {
			assert.Contains(t, err.Error(), scenario.returnedError, "Scenario "+scenario.testName+" failed; returned unexpected error")
		}

		sort.Strings(scenario.updatedConcepts.UpdatedIds)
		sort.Strings(actualChanges.UpdatedIds)

		assert.Equal(t, scenario.updatedConcepts, actualChanges, "Test "+scenario.testName+" failed: Updated uuid list differs from expected")

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
			if scenario.customAssertion != nil {
				scenario.customAssertion(t, concept)
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
		prefUUID         string
		statementToWrite string
		returnedError    error
	}

	invalidPrefConceptTypeTest := testStruct{
		testName:         "invalidPrefConceptTypeTest",
		prefUUID:         basicConceptUUID,
		statementToWrite: invalidPrefConceptType,
		returnedError:    nil,
	}
	invalidSourceConceptTypeTest := testStruct{
		testName:         "invalidSourceConceptTypeTest",
		prefUUID:         anotherBasicConceptUUID,
		statementToWrite: invalidSourceConceptType,
		returnedError:    nil,
	}

	scenarios := []testStruct{invalidPrefConceptTypeTest, invalidSourceConceptTypeTest}

	for _, scenario := range scenarios {
		db.CypherBatch([]*neoism.CypherQuery{{Statement: scenario.statementToWrite}})
		aggConcept, found, err := conceptsDriver.Read(scenario.prefUUID, "")
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
		firstList    map[string]string
		secondList   map[string]string
		filteredList map[string]string
	}

	emptyWhenBothListsAreEmpty := testStruct{
		testName:     "emptyWhenBothListsAreEmpty",
		firstList:    make(map[string]string),
		secondList:   make(map[string]string),
		filteredList: make(map[string]string),
	}
	emptyWhenListsAreTheIdentical := testStruct{
		testName: "emptyWhenListsAreTheIdentical",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		filteredList: make(map[string]string),
	}
	emptyWhenListsHaveSameIdsInDifferentOrder := testStruct{
		testName: "emptyWhenListsHaveSameIdsInDifferentOrder",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"2": "",
			"3": "",
			"1": "",
		},
		filteredList: make(map[string]string),
	}
	hasCompleteFirstListWhenSecondListIsEmpty := testStruct{
		testName: "hasCompleteSecondListWhenFirstListIsEmpty",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: make(map[string]string),
		filteredList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
	}
	properlyFiltersWhen1IdIsUnique := testStruct{
		testName: "properlyFiltersWhen1IdIsUnique",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"1": "",
			"2": "",
		},
		filteredList: map[string]string{
			"3": "",
		},
	}
	properlyFiltersWhen2IdsAreUnique := testStruct{
		testName: "properlyFiltersWhen2IdsAreUnique",
		firstList: map[string]string{
			"1": "",
			"2": "",
			"3": "",
		},
		secondList: map[string]string{
			"2": "",
		},
		filteredList: map[string]string{
			"1": "",
			"3": "",
		},
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
		returnedList := filterIdsThatAreUniqueToFirstMap(scenario.firstList, scenario.secondList)
		assert.Equal(t, scenario.filteredList, returnedList, "Scenario: "+scenario.testName+" returned unexpected results")
	}
}

func TestTransferConcordance(t *testing.T) {
	statement := `MERGE (a:Thing{prefUUID:"1"}) MERGE (b:Thing{uuid:"1"}) MERGE (c:Thing{uuid:"2"}) MERGE (d:Thing{uuid:"3"}) MERGE (w:Thing{prefUUID:"4"}) MERGE (y:Thing{uuid:"5"}) MERGE (j:Thing{prefUUID:"6"}) MERGE (k:Thing{uuid:"6"}) MERGE (c)-[:EQUIVALENT_TO]->(a)<-[:EQUIVALENT_TO]-(b) MERGE (w)<-[:EQUIVALENT_TO]-(d) MERGE (j)<-[:EQUIVALENT_TO]-(k)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	var emptyQuery []*neoism.CypherQuery
	var updatedConcept ConceptChanges

	type testStruct struct {
		testName         string
		updatedSourceIds map[string]string
		returnResult     bool
		returnedError    error
	}

	nodeHasNoConconcordance := testStruct{
		testName: "nodeHasNoConconcordance",
		updatedSourceIds: map[string]string{
			"5": "Brand"},
		returnedError: nil,
	}
	nodeHasExistingConcordanceWhichWouldCauseDataIssues := testStruct{
		testName: "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: map[string]string{
			"1": "Brand"},
		returnedError: errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasExistingConcordanceWhichNeedsToBeReWritten := testStruct{
		testName: "nodeHasExistingConcordanceWhichNeedsToBeReWritten",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError: nil,
	}
	nodeHasInvalidConcordance := testStruct{
		testName: "nodeHasInvalidConcordance",
		updatedSourceIds: map[string]string{
			"3": "Brand"},
		returnedError: errors.New("This source id: 3 the only concordance to a non-matching node with prefUuid: 4"),
	}
	nodeIsPrefUUIDForExistingConcordance := testStruct{
		testName: "nodeIsPrefUuidForExistingConcordance",
		updatedSourceIds: map[string]string{
			"1": "Brand"},
		returnedError: errors.New("Cannot currently process this record as it will break an existing concordance with prefUuid: 1"),
	}
	nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted := testStruct{
		testName: "nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted",
		updatedSourceIds: map[string]string{
			"6": "Brand"},
		returnResult:  true,
		returnedError: nil,
	}

	scenarios := []testStruct{
		nodeHasNoConconcordance,
		nodeHasExistingConcordanceWhichWouldCauseDataIssues,
		nodeHasExistingConcordanceWhichNeedsToBeReWritten,
		nodeHasInvalidConcordance,
		nodeIsPrefUUIDForExistingConcordance,
		nodeHasConcordanceToItselfPrefNodeNeedsToBeDeleted,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", AggregatedConcept{}, "")
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

func TestTransferCanonicalMultipleConcordance(t *testing.T) {
	statement := `
	MERGE (editorialCanonical:Thing{prefUUID:"1"}) 
	MERGE (editorial:Thing{uuid:"1"}) 
	SET editorial.authority="Smartlogic"
	
	MERGE (mlCanonical:Thing{prefUUID:"2"}) 
	MERGE (ml:Thing{uuid:"2"}) 
	SET ml.authority="ManagedLocation"

	MERGE (geonames:Thing{uuid:"3"})
	SET geonames.authority="Geonames"

	MERGE (factset:Thing{uuid:"4"})
	SET factset.authority="FACTSET"

	MERGE (tme:Thing{uuid:"5"})
	SET tme.authority="TME"
	
	MERGE (editorial)-[:EQUIVALENT_TO]->(editorialCanonical)<-[:EQUIVALENT_TO]-(factset)
	MERGE (ml)-[:EQUIVALENT_TO]->(mlCanonical)<-[:EQUIVALENT_TO]-(tme)`
	db.CypherBatch([]*neoism.CypherQuery{{Statement: statement}})
	var emptyQuery []*neoism.CypherQuery
	var updatedConcept ConceptChanges

	type testStruct struct {
		testName          string
		updatedSourceIds  map[string]string
		returnResult      bool
		returnedError     error
		targetConcordance AggregatedConcept
	}
	mergeManagedLocationCanonicalWithTwoSources := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSources",
		updatedSourceIds: map[string]string{
			"2": "Brand"},
		returnedError: nil,
		returnResult:  true,
		targetConcordance: AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []Concept{
				Concept{UUID: "1", Authority: "Smartlogic"},
				Concept{UUID: "4", Authority: "FACTSET"},
				Concept{UUID: "2", Authority: "ManagedLocation"},
			},
		},
	}
	mergeManagedLocationCanonicalWithTwoSourcesAndGeonames := testStruct{
		testName: "mergeManagedLocationCanonicalWithTwoSourcesAndGeonames",
		updatedSourceIds: map[string]string{
			"3": "Brand",
			"2": "Brand"},
		returnedError: nil,
		returnResult:  true,
		targetConcordance: AggregatedConcept{
			PrefUUID: "1",
			SourceRepresentations: []Concept{
				Concept{UUID: "1", Authority: "Smartlogic"},
				Concept{UUID: "4", Authority: "FACTSET"},
				Concept{UUID: "2", Authority: "ManagedLocation"},
				Concept{UUID: "5", Authority: "TME"},
			},
		},
	}
	mergeJustASourceConcordance := testStruct{
		testName: "mergeJustASourceConcordance",
		updatedSourceIds: map[string]string{
			"4": "Brand"},
		returnedError: nil,
	}

	scenarios := []testStruct{
		mergeManagedLocationCanonicalWithTwoSources,
		mergeManagedLocationCanonicalWithTwoSourcesAndGeonames,
		mergeJustASourceConcordance,
	}

	for _, scenario := range scenarios {
		returnedQueryList, err := conceptsDriver.handleTransferConcordance(scenario.updatedSourceIds, &updatedConcept, "1234", scenario.targetConcordance, "")
		assert.Equal(t, scenario.returnedError, err, "Scenario "+scenario.testName+" returned unexpected error")
		if scenario.returnResult == true {
			assert.NotEqual(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
			continue
		}
		assert.Equal(t, emptyQuery, returnedQueryList, "Scenario "+scenario.testName+" results do not match")
	}

	defer deleteSourceNodes(t, "1", "2", "3", "5")
	defer deleteConcordedNodes(t, "1", "2")
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
				UUID:           basicConceptUUID,
				Type:           "Brand",
				AuthorityValue: "123456-UPP",
				Authority:      "UPP",
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
		testName:   "testSourceRepNoPrefLabel",
		aggConcept: sourceRepNoPrefLabel,
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

func TestWriteLocation(t *testing.T) {
	defer cleanDB(t)

	location := getLocation()
	_, err := conceptsDriver.Write(location, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, location, "TestWriteLocation")

	locationISO31661 := getLocationWithISO31661()
	_, err = conceptsDriver.Write(locationISO31661, "test_tid")
	assert.NoError(t, err, "Failed to write concept")
	readConceptAndCompare(t, locationISO31661, "TestWriteLocationISO31661")
}

func readConceptAndCompare(t *testing.T, payload AggregatedConcept, testName string) {
	actualIf, found, err := conceptsDriver.Read(payload.PrefUUID, "")
	actual := actualIf.(AggregatedConcept)

	actual = cleanHash(cleanConcept(actual))
	clean := cleanSourceProperties(payload)
	expected := cleanHash(cleanConcept(clean))

	assert.Equal(t, expected, actual, fmt.Sprintf("Test %s failed: Concepts were not equal", testName))
	assert.NoError(t, err, fmt.Sprintf("Test %s failed: Unexpected Error occurred", testName))
	assert.True(t, found, fmt.Sprintf("Test %s failed: Concept has not been found", testName))
}

func newURL() string {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}
	return url
}

func getConceptService(t *testing.T) ConceptService {
	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(newURL(), conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewConceptService(db)
	service.Initialise()
	return service
}

func cleanDB(t *testing.T) {
	cleanSourceNodes(t,
		parentUUID,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
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
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
	)
	deleteSourceNodes(t,
		parentUUID,
		anotherBasicConceptUUID,
		basicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
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
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
	)
	deleteConcordedNodes(t,
		parentUUID,
		basicConceptUUID,
		anotherBasicConceptUUID,
		sourceID1,
		sourceID2,
		sourceID3,
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
		parentOrgUUID,
		supersededByUUID,
		testOrgUUID,
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
