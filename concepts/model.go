package concepts

type AggregatedConcept struct {
	PrefUUID              string    `json:"prefUUID"`
	PrefLabel             string    `json:"prefLabel"`
	Type                  string    `json:"type"`
	Aliases               []string  `json:"aliases,omitempty"`
	Strapline             string    `json:"strapline,omitempty"`
	DescriptionXML        string    `json:"descriptionXML,omitempty"`
	ImageURL              string    `json:"_imageUrl,omitempty"`
	SourceRepresentations []Concept `json:"sourceRepresentations"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID              string   `json:"uuid,omitempty"`
	PrefLabel         string   `json:"prefLabel"`
	Type              string   `json:"type"`
	Authority         string   `json:"authority,omitempty"`
	AuthorityValue    string   `json:"authorityValue,omitempty"`
	LastModifiedEpoch int      `json:"lastModifiedEpoch,omitempty"`
	Aliases           []string `json:"aliases,omitempty"`
	ParentUUIDs       []string `json:"parentUUIDs,omitempty"`
	Strapline         string   `json:"strapline,omitempty"`
	DescriptionXML    string   `json:"descriptionXML,omitempty"`
	ImageURL          string   `json:"_imageUrl,omitempty"`
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
	"UPPIdentifier":        "value",
	"TMEIdentifier":        "value",
	"FactsetIdentifier":    "value",
	"SmartlogicIdentifier": "value",
	"Person":               "value",
}

var conceptLabels = [...]string{
	"Concept", "Classification", "Section", "Subject", "SpecialReport", "Topic",
	"Location", "Genre", "Brand", "Person",
}

// Map of authority and nodelabel for identifiers - we should be removing
// Identifiers after all the concepts have been migrated to the new model
var authorityToIdentifierLabelMap = map[string]string{
	"TME":        "TMEIdentifier",
	"UPP":        "UPPIdentifier",
	"Smartlogic": "SmartlogicIdentifier",
}

var BasicTmePaths = []string{"brands", "topics", "subjects", "special-reports", "genres", "locations", "sections", "alphaville-series", "people"}
