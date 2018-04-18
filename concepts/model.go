package concepts

type MembershipRole struct {
	RoleUUID             string `json:"membershipRoleUUID,omitempty"`
	InceptionDate        string `json:"inceptionDate,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64  `json:"terminationDateEpoch,omitempty"`
}

type AggregatedConcept struct {
	PrefUUID               string           `json:"prefUUID,omitempty"`
	PrefLabel              string           `json:"prefLabel,omitempty"`
	Type                   string           `json:"type,omitempty"`
	Aliases                []string         `json:"aliases,omitempty"`
	Strapline              string           `json:"strapline,omitempty"`
	DescriptionXML         string           `json:"descriptionXML,omitempty"`
	ImageURL               string           `json:"_imageUrl,omitempty"`
	EmailAddress           string           `json:"emailAddress,omitempty"`
	FacebookPage           string           `json:"facebookPage,omitempty"`
	TwitterHandle          string           `json:"twitterHandle,omitempty"`
	ScopeNote              string           `json:"scopeNote,omitempty"`
	ShortLabel             string           `json:"shortLabel,omitempty"`
	OrganisationUUID       string           `json:"organisationUUID,omitempty"`
	PersonUUID             string           `json:"personUUID,omitempty"`
	AggregatedHash         string           `json:"aggregateHash,omitempty"`
	SourceRepresentations  []Concept        `json:"sourceRepresentations,omitempty"`
	MembershipRoles        []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate          string           `json:"inceptionDate,omitempty"`
	TerminationDate        string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch     int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch   int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode               string           `json:"figiCode,omitempty"`
	IssuedBy               string           `json:"issuedBy,omitempty"`
	ProperName             string           `json:"properName,omitempty"`
	ShortName              string           `json:"shortName,omitempty"`
	HiddenLabel            string           `json:"hiddenLabel,omitempty"`
	FormerNames            []string         `json:"formerNames,omitempty"`
	CountryCode            string           `json:"countryCode,omitempty"`
	CountryOfIncorporation string           `json:"countryOfIncorporation,omitempty"`
	PostalCode             string           `json:"postalCode,omitempty"`
	YearFounded            int              `json:"yearFounded,omitempty"`
	LeiCode                string           `json:"leiCode,omitempty"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID                   string           `json:"uuid,omitempty"`
	PrefLabel              string           `json:"prefLabel,omitempty"`
	Type                   string           `json:"type,omitempty"`
	Authority              string           `json:"authority,omitempty"`
	AuthorityValue         string           `json:"authorityValue,omitempty"`
	LastModifiedEpoch      int              `json:"lastModifiedEpoch,omitempty"`
	Aliases                []string         `json:"aliases,omitempty"`
	ParentUUIDs            []string         `json:"parentUUIDs,omitempty"`
	Strapline              string           `json:"strapline,omitempty"`
	DescriptionXML         string           `json:"descriptionXML,omitempty"`
	ImageURL               string           `json:"_imageUrl,omitempty"`
	EmailAddress           string           `json:"emailAddress,omitempty"`
	FacebookPage           string           `json:"facebookPage,omitempty"`
	TwitterHandle          string           `json:"twitterHandle,omitempty"`
	ScopeNote              string           `json:"scopeNote,omitempty"`
	ShortLabel             string           `json:"shortLabel,omitempty"`
	BroaderUUIDs           []string         `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs           []string         `json:"relatedUUIDs,omitempty"`
	OrganisationUUID       string           `json:"organisationUUID,omitempty"`
	PersonUUID             string           `json:"personUUID,omitempty"`
	Hash                   string           `json:"hash,omitempty"`
	MembershipRoles        []MembershipRole `json:"membershipRoles,omitempty"`
	InceptionDate          string           `json:"inceptionDate,omitempty"`
	TerminationDate        string           `json:"terminationDate,omitempty"`
	InceptionDateEpoch     int64            `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch   int64            `json:"terminationDateEpoch,omitempty"`
	FigiCode               string           `json:"figiCode,omitempty"`
	IssuedBy               string           `json:"issuedBy,omitempty"`
	ProperName             string           `json:"properName,omitempty"`
	ShortName              string           `json:"shortName,omitempty"`
	HiddenLabel            string           `json:"hiddenLabel,omitempty"`
	FormerNames            []string         `json:"formerNames,omitempty"`
	CountryCode            string           `json:"countryCode,omitempty"`
	CountryOfIncorporation string           `json:"countryOfIncorporation,omitempty"`
	PostalCode             string           `json:"postalCode,omitempty"`
	YearFounded            int              `json:"yearFounded,omitempty"`
	LeiCode                string           `json:"leiCode,omitempty"`
}

type UpdatedConcepts struct {
	UpdatedIds []string `json:"updatedIDs"`
}

// Map of all the possible node types so we can ensure they all have
// constraints on the uuid
var constraintMap = map[string]string{
	"Thing":                "uuid",
	"Concept":              "uuid",
	"Classification":       "uuid",
	"Section":              "uuid",
	"Subject":              "uuid",
	"SpecialReport":        "uuid",
	"Location":             "uuid",
	"Topic":                "uuid",
	"Genre":                "uuid",
	"Brand":                "uuid",
	"AlphavilleSeries":     "uuid",
	"PublicCompany":        "uuid",
	"Person":               "uuid",
	"Organisation":         "uuid",
	"MembershipRole":       "uuid",
	"BoardRole":            "uuid",
	"Membership":           "uuid",
	"UPPIdentifier":        "value",
	"TMEIdentifier":        "value",
	"FactsetIdentifier":    "value",
	"SmartlogicIdentifier": "value",
	"FinancialInstrument":  "uuid",
}

var conceptLabels = [...]string{
	"Concept", "Classification", "Section", "Subject", "SpecialReport", "Topic",
	"Location", "Genre", "Brand", "Person", "Organisation", "MembershipRole", "Membership",
	"BoardRole",
	"FinancialInstrument",
	"PublicCompany",
}

// Map of authority and nodelabel for identifiers - we should be removing
// Identifiers after all the concepts have been migrated to the new model
var authorityToIdentifierLabelMap = map[string]string{
	"TME":        "TMEIdentifier",
	"UPP":        "UPPIdentifier",
	"Smartlogic": "SmartlogicIdentifier",
	"FACTSET":    "FactsetIdentifier",
}
