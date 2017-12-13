package concepts

import (
	"net/http"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	st "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

func (h *ConceptsHandler) RegisterAdminHandlers(router *mux.Router, appSystemCode string, appName string, appDescription string, enableRequestLogging bool) http.Handler {
	logger.Info("Registering healthcheck handlers")
	var checks = []fthealth.Check{h.makeNeo4jAvailabilityCheck()}

	hc := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  appSystemCode,
			Name:        appName,
			Description: appDescription,
			Checks:      checks,
		},
		Timeout: 10 * time.Second,
	}

	router.HandleFunc("/__health", fthealth.Handler(hc))
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
	if errString, err := h.makeNeo4jAvailabilityCheck().Checker(); err != nil {
		logger.WithError(err).Errorf("Connection to Neo4j healthcheck failed [%s]", errString)
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("Connection to Neo4j healthcheck failed"))
		return
	}
	rw.WriteHeader(http.StatusOK)
}

func (h *ConceptsHandler) makeNeo4jAvailabilityCheck() fthealth.Check {
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
