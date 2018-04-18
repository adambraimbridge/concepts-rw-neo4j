package concepts

type MembershipRole struct {
	RoleUUID             string `json:"membershipRoleUUID,omitempty"`
	InceptionDate        string `json:"inceptionDate,omitempty"`
	TerminationDate      string `json:"terminationDate,omitempty"`
	InceptionDateEpoch   int64  `json:"inceptionDateEpoch,omitempty"`
	TerminationDateEpoch int64  `json:"terminationDateEpoch,omitempty"`
}

type AggregatedConcept struct {
	AggregatedHash        string    `json:"aggregateHash,omitempty"`
	Aliases               []string  `json:"aliases,omitempty"`
	DescriptionXML        string    `json:"descriptionXML,omitempty"`
	EmailAddress          string    `json:"emailAddress,omitempty"`
	FacebookPage          string    `json:"facebookPage,omitempty"`
	ImageURL              string    `json:"_imageUrl,omitempty"`
	PrefLabel             string    `json:"prefLabel,omitempty"`
	PrefUUID              string    `json:"prefUUID,omitempty"`
	ScopeNote             string    `json:"scopeNote,omitempty"`
	ShortLabel            string    `json:"shortLabel,omitempty"`
	SourceRepresentations []Concept `json:"sourceRepresentations,omitempty"`
	TwitterHandle         string    `json:"twitterHandle,omitempty"`
	Type                  string    `json:"type,omitempty"`
	// Brand
	Strapline string `json:"strapline,omitempty"`
	// Person
	IsAuthor bool `json:"isAuthor,omitempty"`
	// Financial Instrument
	FigiCode string `json:"figiCode,omitempty"`
	IssuedBy string `json:"issuedBy,omitempty"`
	// Membership
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	// Organisation
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	HiddenLabel            string   `json:"hiddenLabel,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	Aliases           []string `json:"aliases,omitempty"`
	Authority         string   `json:"authority,omitempty"`
	AuthorityValue    string   `json:"authorityValue,omitempty"`
	BroaderUUIDs      []string `json:"broaderUUIDs,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
	EmailAddress      string   `json:"emailAddress,omitempty"`
	FacebookPage      string   `json:"facebookPage,omitempty"`
	Hash              string   `json:"hash,omitempty"`
	ImageURL          string   `json:"_imageUrl,omitempty"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	PrefLabel         string   `json:"prefLabel,omitempty"`
	RelatedUUIDs      []string `json:"relatedUUIDs,omitempty"`
	ScopeNote         string   `json:"scopeNote,omitempty"`
	ShortLabel        string   `json:"shortLabel,omitempty"`
	TwitterHandle     string   `json:"twitterHandle,omitempty"`
	Type              string   `json:"type,omitempty"`
	UUID              string   `json:"uuid,omitempty"`
	// Brand
	Strapline string `json:"strapline,omitempty"`
	// Person
	IsAuthor bool `json:"isAuthor,omitempty"`
	// Financial Instrument
	FigiCode string `json:"figiCode,omitempty"`
	IssuedBy string `json:"issuedBy,omitempty"`
	// Membership
	InceptionDate        string           `json:"inceptionDate,omitempty"`
	InceptionDateEpoch   int64            `json:"inceptionDateEpoch,omitempty"`
	MembershipRoles      []MembershipRole `json:"membershipRoles,omitempty"`
	OrganisationUUID     string           `json:"organisationUUID,omitempty"`
	PersonUUID           string           `json:"personUUID,omitempty"`
	TerminationDate      string           `json:"terminationDate,omitempty"`
	TerminationDateEpoch int64            `json:"terminationDateEpoch,omitempty"`
	// Organisation
	CountryCode            string   `json:"countryCode,omitempty"`
	CountryOfIncorporation string   `json:"countryOfIncorporation,omitempty"`
	FormerNames            []string `json:"formerNames,omitempty"`
	HiddenLabel            string   `json:"hiddenLabel,omitempty"`
	LeiCode                string   `json:"leiCode,omitempty"`
	PostalCode             string   `json:"postalCode,omitempty"`
	ProperName             string   `json:"properName,omitempty"`
	ShortName              string   `json:"shortName,omitempty"`
	YearFounded            int      `json:"yearFounded,omitempty"`
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
	"BoardRole",
	"Brand",
	"Classification",
	"Concept",
	"FinancialInstrument",
	"Genre",
	"Location",
	"Membership",
	"MembershipRole",
	"Organisation",
	"Person",
	"PublicCompany",
	"Section",
	"SpecialReport",
	"Subject",
	"Topic",
}

// Map of authority and nodelabel for identifiers - we should be removing
// Identifiers after all the concepts have been migrated to the new model
var authorityToIdentifierLabelMap = map[string]string{
	"TME":        "TMEIdentifier",
	"UPP":        "UPPIdentifier",
	"Smartlogic": "SmartlogicIdentifier",
	"FACTSET":    "FactsetIdentifier",
}
