package concepts

import (
	"encoding/json"
	"errors"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
)

type mockConceptService struct {
	uuid         string
	conceptType  string
	transID      string
	uuidList     []string
	failParse    bool
	failWrite    bool
	failRead     bool
	failConflict bool
	failCheck    bool

	read func(uuid string, transID string) (interface{}, bool, error)
}

func (mcs *mockConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	mockList := ConceptChanges{}
	if mcs.failWrite {
		return mockList, errors.New("TEST failing to WRITE")
	}
	if mcs.failConflict {
		return mockList, rwapi.ConstraintOrTransactionError{}
	}
	if len(mcs.uuidList) > 0 {
		mockList.UpdatedIds = mcs.uuidList
	}
	mcs.transID = transID
	return mockList, nil
}

func (mcs *mockConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	if mcs.read != nil {
		return mcs.read(uuid, transID)
	}
	return nil, false, errors.New("not implemented")
}

func (mcs *mockConceptService) DecodeJSON(*json.Decoder) (interface{}, string, error) {
	if mcs.failParse {
		return "", "", errors.New("TEST failing to DECODE")
	}
	return AggregatedConcept{
		PrefUUID: mcs.uuid,
		Type:     mcs.conceptType,
	}, mcs.uuid, nil
}

func (mcs *mockConceptService) Check() error {
	if mcs.failCheck {
		return errors.New("TEST failing to CHECK")
	}
	return nil
}

func (mcs *mockConceptService) Initialise() error {
	return nil
}
