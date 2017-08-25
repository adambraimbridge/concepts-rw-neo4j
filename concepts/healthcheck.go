package concepts

import (
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	st "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func (h *ConceptsHandler) RegisterAdminHandlers(router *mux.Router, appSystemCode string, appName string, appDescription string, enableRequestLogging bool) http.Handler {
	log.Info("Registering healthcheck handlers")
	var checks []fthealth.Check = []fthealth.Check{h.makeNeo4jAvailabililtyCheck()}

	router.HandleFunc("/__health", fthealth.Handler(fthealth.HealthCheck{SystemCode: appSystemCode, Name: appName, Description: appDescription, Checks: checks}))
	router.HandleFunc(st.BuildInfoPath, st.BuildInfoHandler)
	router.HandleFunc(st.GTGPath, h.gtgCheck)

	var monitoringRouter http.Handler = router
	if enableRequestLogging {
		monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	}
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}

func (h *ConceptsHandler) gtgCheck(rw http.ResponseWriter, req *http.Request) {
	if errString, err := h.makeNeo4jAvailabililtyCheck().Checker(); err != nil {
		log.WithError(err).Errorf("Connection to Neo4j healthcheck failed [%s]", errString)
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("Connection to Neo4j healthcheck failed"))
		return
	}
	rw.WriteHeader(http.StatusOK)
}

func (h *ConceptsHandler) makeNeo4jAvailabililtyCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Cannot read/write concepts via this writer",
		Name:             "Check connectivity to Neo4j - neoUrl is a parameter in hieradata for this service",
		PanicGuide:       "https://dewey.ft.com/concepts-rw-neo4j.html",
		Severity:         2,
		TechnicalSummary: "Cannot connect to Neo4j instance with at least one concept loaded in it",
		Checker:          h.checkNeo4jAvailability,
	}
}

func (h *ConceptsHandler) checkNeo4jAvailability() (string, error) {
	err := h.ConceptsService.Check()
	if err != nil {
		return "Could not connect to database!", err
	}
	return "", nil
}
