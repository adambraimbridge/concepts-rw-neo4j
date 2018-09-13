package concepts

//
//import (
//	"fmt"
//	"github.com/Financial-Times/go-logger"
//	"net/http"
//	"net/http/httptest"
//	"testing"
//
//	"github.com/gorilla/mux"
//	"github.com/stretchr/testify/assert"
//)
//
//const knownUUID = "12345"
//
//func init() {
//	logger.InitDefaultLogger("handlers-test")
//}
//
//func TestPutHandler(t *testing.T) {
//	assert := assert.New(t)
//	tests := []struct {
//		name        string
//		req         *http.Request
//		mockService map[string]ConceptServicer
//		statusCode  int
//		contentType string // Contents of the Content-Type header
//		body        string
//	}{
//		{
//			name: "Success",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "Dummy",
//			}),
//			statusCode:  http.StatusOK,
//			contentType: "",
//			body:        "{\"events\":null,\"updatedIDs\":null}",
//		},
//		{
//			name: "RegularPathSuccess",
//			req:  newRequest("PUT", fmt.Sprintf("/financial-instruments/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "FinancialInstrument",
//			}),
//			statusCode:  http.StatusOK,
//			contentType: "",
//			body:        "{\"events\":null,\"updatedIDs\":null}",
//		},
//		{
//			name: "ParseError",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "Dummy",
//				failParse:   true,
//			}),
//			statusCode:  http.StatusBadRequest,
//			contentType: "",
//			body:        errorMessage("TEST failing to DECODE"),
//		},
//		{
//			name: "UUIDMisMatch",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", "99999")),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "Dummy",
//			}),
//			statusCode:  http.StatusBadRequest,
//			contentType: "",
//			body:        errorMessage("Uuids from payload and request, respectively, do not match: '12345' '99999'"),
//		},
//		{
//			name: "WriteFailed",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "Dummy",
//				failWrite:   true,
//			}),
//			statusCode:  http.StatusServiceUnavailable,
//			contentType: "",
//			body:        errorMessage("TEST failing to WRITE"),
//		},
//		{
//			name: "WriteFailedDueToConflict",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:         knownUUID,
//				conceptType:  "Dummy",
//				failConflict: true,
//			}),
//			statusCode:  http.StatusConflict,
//			contentType: "",
//			body:        errorMessage(""),
//		},
//		{
//			name: "BadConceptOrPath",
//			req:  newRequest("PUT", fmt.Sprintf("/dummies/%s", knownUUID)),
//			mockService: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "not-dummy",
//			}),
//			statusCode:  http.StatusBadRequest,
//			contentType: "",
//			body:        errorMessage("concept type does not match path"),
//		},
//	}
//
//	for _, test := range tests {
//		r := mux.NewRouter()
//		handler := ConceptsHandler{test.mockService}
//		handler.RegisterHandlers(r)
//		rec := httptest.NewRecorder()
//		r.ServeHTTP(rec, test.req)
//		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
//		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
//	}
//}
//
//func TestGetHandler(t *testing.T) {
//	assert := assert.New(t)
//	tests := []struct {
//		name        string
//		req         *http.Request
//		ds          map[string]ConceptServicer
//		statusCode  int
//		contentType string // Contents of the Content-Type header
//		body        string
//	}{
//		{
//			name: "Success",
//			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID)),
//			ds: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "dummies",
//			}),
//			statusCode:  http.StatusOK,
//			contentType: "",
//			body:        "{\"prefUUID\":\"12345\",\"type\":\"Dummy\"}\n",
//		},
//		{
//			name: "NotFound",
//			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", "99999")),
//			ds: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "brands",
//			}),
//			statusCode:  http.StatusNotFound,
//			contentType: "",
//			body:        "{\"message\":\"Concept with prefUUID 99999 not found in db.\"}",
//		},
//		{
//			name: "ReadError",
//			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID)),
//			ds: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "brands",
//				failRead:    true,
//			}),
//			statusCode:  http.StatusServiceUnavailable,
//			contentType: "",
//			body:        errorMessage("TEST failing to READ"),
//		},
//		{
//			name: "BadConceptOrPath",
//			req:  newRequest("GET", fmt.Sprintf("/dummies/%s", knownUUID)),
//			ds: buildServiceMap(&mockConceptService{
//				uuid:        knownUUID,
//				conceptType: "not-dummy",
//			}),
//			statusCode:  http.StatusBadRequest,
//			contentType: "",
//			body:        errorMessage("Concept type does not match path"),
//		},
//	}
//
//	for _, test := range tests {
//		r := mux.NewRouter()
//		handler := ConceptsHandler{test.ds}
//		handler.RegisterHandlers(r)
//		rec := httptest.NewRecorder()
//		r.ServeHTTP(rec, test.req)
//		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
//		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
//	}
//}
//
//func TestGtgHandler(t *testing.T) {
//	assert := assert.New(t)
//	tests := []struct {
//		name        string
//		req         *http.Request
//		ds          map[string]ConceptServicer
//		statusCode  int
//		contentType string // Contents of the Content-Type header
//		body        string
//	}{
//		{"Success", newRequest("GET", "/__gtg"), buildServiceMap(&mockConceptService{conceptType: "brands"}), http.StatusOK, "", "OK"},
//		{"GTGError", newRequest("GET", "/__gtg"), buildServiceMap(&mockConceptService{failCheck: true, conceptType: "brands"}), http.StatusServiceUnavailable, "", "TEST failing to CHECK"},
//	}
//
//	for _, test := range tests {
//		r := mux.NewRouter()
//		handler := ConceptsHandler{test.ds}
//		handler.RegisterAdminHandlers(r, "", "", "", true)
//		rec := httptest.NewRecorder()
//		r.ServeHTTP(rec, test.req)
//		assert.Equal(test.statusCode, rec.Code, fmt.Sprintf("%s: Wrong response code, was %d, should be %d", test.name, rec.Code, test.statusCode))
//		assert.Equal(test.body, rec.Body.String(), fmt.Sprintf("%s: Wrong body", test.name))
//	}
//}
//
//func newRequest(method, url string) *http.Request {
//	req, err := http.NewRequest(method, url, nil)
//	if err != nil {
//		panic(err)
//	}
//	return req
//}
//
//func errorMessage(errMsg string) string {
//	return fmt.Sprintf("{\"message\": \"%s\"}\n", errMsg)
//}
//
//func buildServiceMap(service *mockConceptService) map[string]ConceptServicer {
//	serviceMap := make(map[string]ConceptServicer)
//	serviceMap[service.conceptType] = service
//	return serviceMap
//}
