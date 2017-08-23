package concepts

import (
	"net/http"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/Financial-Times/transactionid-utils-go"
	"encoding/json"
	"github.com/gorilla/handlers"
	"io"
	"compress/gzip"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
)

type ConceptsHandler struct {
	ConceptsService ConceptService
}

func (h *ConceptsHandler) RegisterHandlers(router *mux.Router, path string) *mux.Router {
	urlPath := fmt.Sprintf("/%s/{uuid}", path)
	rwHandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetConcept),
		"PUT": http.HandlerFunc(h.PutConcept),
	}
	router.Handle(urlPath, rwHandler)
	return router
}

func (hh *ConceptsHandler) PutConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	tid := transactionidutils.GetTransactionIDFromRequest(r)
	w.Header().Add("Content-Type", "application/json")

	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		unzipped, err := gzip.NewReader(r.Body)
		if err != nil {
			writeJSONError(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer unzipped.Close()
		body = unzipped
	}

	dec := json.NewDecoder(body)
	inst, docUUID, err := hh.ConceptsService.DecodeJSON(dec)

	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if docUUID != uuid {
		writeJSONError(w, fmt.Sprintf("Uuids from payload and request, respectively, do not match: '%v' '%v'", docUUID, uuid), http.StatusBadRequest)
		return
	}

	updatedIds, err := hh.ConceptsService.Write(inst, tid)

	if err != nil {
		switch e := err.(type) {
		case rwapi.ConstraintOrTransactionError:
			writeJSONError(w, e.Error(), http.StatusConflict)
			return
		//case invalidRequestError:
		//	writeJSONError(w, e.InvalidRequestDetails(), http.StatusBadRequest)
		//	return
		default:
			writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}

	w.Header().Set("X-Request-Id", tid)
	enc := json.NewEncoder(w)
	if err := enc.Encode(updatedIds); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		w.WriteHeader(http.StatusOK)
		return
	}
	return
}

func (hh *ConceptsHandler) GetConcept(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	tid := transactionidutils.GetTransactionIDFromRequest(r)

	obj, found, err := hh.ConceptsService.Read(uuid, tid)

	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", tid)

	if err != nil {
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	if !found {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(err.Error()))
		return
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(obj); err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", errorMsg))
}
