package concepts

type AggregatedConcept struct {
	PrefUUID              string    `json:"prefUUID,omitempty"`
	PrefLabel             string    `json:"prefLabel,omitempty"`
	Type                  string    `json:"type,omitempty"`
	Aliases               []string  `json:"aliases,omitempty"`
	Strapline             string    `json:"strapline,omitempty"`
	DescriptionXML        string    `json:"descriptionXML,omitempty"`
	ImageURL              string    `json:"_imageUrl,omitempty"`
	EmailAddress          string    `json:"emailAddress,omitempty"`
	FacebookPage          string    `json:"facebookPage,omitempty"`
	TwitterHandle         string    `json:"twitterHandle,omitempty"`
	ScopeNote             string    `json:"scopeNote,omitempty"`
	ShortLabel            string    `json:"shortLabel,omitempty"`
	SourceRepresentations []Concept `json:"sourceRepresentations,omitempty"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID              string   `json:"uuid,omitempty"`
	PrefLabel         string   `json:"prefLabel,omitempty"`
	Type              string   `json:"type,omitempty"`
	Authority         string   `json:"authority,omitempty"`
	AuthorityValue    string   `json:"authorityValue,omitempty"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	Aliases           []string `json:"aliases,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	Strapline         string   `json:"strapline,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
	ImageURL          string   `json:"_imageUrl,omitempty"`
	EmailAddress      string   `json:"emailAddress,omitempty"`
	FacebookPage      string   `json:"facebookPage,omitempty"`
	TwitterHandle     string   `json:"twitterHandle,omitempty"`
	ScopeNote         string   `json:"scopeNote,omitempty"`
	ShortLabel        string   `json:"shortLabel,omitempty"`
	BroaderUUIDs      []string `json:"broaderUUIDs,omitempty"`
	RelatedUUIDs      []string `json:"relatedUUIDs,omitempty"`
}

type UpdatedConcepts struct {
	UpdatedIds []string `json: "updatedIds"`
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
	"Person":               "uuid",
	"Organisation":         "uuid",
	"UPPIdentifier":        "value",
	"TMEIdentifier":        "value",
	"FactsetIdentifier":    "value",
	"SmartlogicIdentifier": "value",
}

var conceptLabels = [...]string{
	"Concept", "Classification", "Section", "Subject", "SpecialReport", "Topic",
	"Location", "Genre", "Brand", "Person", "Organisation",
}

// Map of authority and nodelabel for identifiers - we should be removing
// Identifiers after all the concepts have been migrated to the new model
var authorityToIdentifierLabelMap = map[string]string{
	"TME":        "TMEIdentifier",
	"UPP":        "UPPIdentifier",
	"Smartlogic": "SmartlogicIdentifier",
}

var BasicTmePaths = []string{"brands", "topics", "subjects", "special-reports", "genres", "locations", "sections", "alphaville-series", "people", "organisation"}
