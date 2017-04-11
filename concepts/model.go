package concepts

type AggregatedConcept struct {
	UUID                  string    `json:"uuid"`
	PrefLabel             string    `json:"prefLabel"`
	SourceRepresentations []Concept `json:"sourceRepresentations"`
}

// Concept - could be any concept genre, subject etc
type Concept struct {
	UUID           string `json:"uuid"`
	PrefLabel      string `json:"prefLabel"`
	Type           string `json:"type"`
	Authority      string `json:"authority"`
	AuthorityValue string `json:"authorityValue"`
}

// Map of all the possible node types so we can ensure they all have
// constraints on the uuid
var constraintMap = map[string]string{
	"Thing":             "uuid",
	"Concept":           "uuid",
	"Classification":    "uuid",
	"Section":           "uuid",
	"Subject":           "uuid",
	"SpecialReport":     "uuid",
	"Location":          "uuid",
	"Topic":             "uuid",
	"Genre":             "uuid",
	"Brand":             "uuid",
	"AlphavilleSeries":  "uuid",
	"UPPIdentifier":     "value",
	"TMEIdentifier":     "value",
	"FactsetIdentifier": "value",
}

var conceptLabels = [...]string{
	"Concept", "Classification", "Section", "Subject", "SpecialReport", "Topic",
	"Location", "Genre", "Brand",
}

// Map of authority and nodelabel for identifiers - we should be removing
// Identifiers after all the concepts have been migrated to the new model
var authorityToIdentifierLabelMap = map[string]string{
	"TME":     "TMEIdentifier",
	"UPP":     "UPPIdentifier",
}

var BasicTmePaths = []string{"topics", "subjects", "special-reports", "genres", "locations", "sections", "alphaville-series"}

