package concepts

import (
	"encoding/json"
	"errors"
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

	write      func(thing interface{}, transID string) (interface{}, error)
	read       func(uuid string, transID string) (interface{}, bool, error)
	decodeJSON func(*json.Decoder) (interface{}, string, error)
}

func (mcs *mockConceptService) Write(thing interface{}, transID string) (interface{}, error) {
	if mcs.write != nil {
		return mcs.write(thing, transID)
	}
	return nil, errors.New("not implemented")
}

func (mcs *mockConceptService) Read(uuid string, transID string) (interface{}, bool, error) {
	if mcs.read != nil {
		return mcs.read(uuid, transID)
	}
	return nil, false, errors.New("not implemented")
}

func (mcs *mockConceptService) DecodeJSON(d *json.Decoder) (interface{}, string, error) {
	if mcs.decodeJSON != nil {
		return mcs.decodeJSON(d)
	}
	return nil, "", errors.New("not implemented")
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
