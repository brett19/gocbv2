package gocb

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"gopkg.in/couchbase/gocbcore.v7"
)

type diagnosticsProvider interface {
	Diagnostics() (*gocbcore.DiagnosticInfo, error)
}

func diagServiceString(service ServiceType) string {
	switch service {
	case MemdService:
		return "kv"
	case CapiService:
		return "view"
	case MgmtService:
		return "mgmt"
	case N1qlService:
		return "n1ql"
	case FtsService:
		return "fts"
	case CbasService:
		return "cbas"
	}
	return "?"
}

// DiagConnState represents the state of a connection in a diagnostics report.
type DiagConnState int

const (
	// DiagStateOk indicates that the connection state is ok.
	DiagStateOk = DiagConnState(0)

	// DiagStateDisconnected indicates that the connection is disconnected.
	DiagStateDisconnected = DiagConnState(1)
)

func diagStateString(state DiagConnState) string {
	switch state {
	case DiagStateOk:
		return "ok"
	case DiagStateDisconnected:
		return "disconnected"
	}
	return "?"
}

// DiagnosticEntry represents a single entry in a diagnostics report.
type DiagnosticEntry struct {
	Service      ServiceType
	State        DiagConnState
	LocalAddr    string
	RemoteAddr   string
	LastActivity time.Time
}

// DiagnosticsReport encapsulates the results of a Diagnostics operation.
type DiagnosticsReport struct {
	ID        string
	ConfigRev int64
	Services  []DiagnosticEntry
}

type jsonDiagnosticEntry struct {
	State          string `json:"state"`
	Remote         string `json:"remote"`
	Local          string `json:"local"`
	LastActivityUs uint64 `json:"last_activity_us"`
}

type jsonDiagnosticReport struct {
	Version   int                              `json:"version"`
	ID        string                           `json:"id"`
	ConfigRev int                              `json:"config_rev"`
	Sdk       string                           `json:"sdk"`
	Services  map[string][]jsonDiagnosticEntry `json:"services"`
}

// MarshalJSON generates a JSON representation of this diagnostics report.
func (report *DiagnosticsReport) MarshalJSON() ([]byte, error) {
	jsonReport := jsonDiagnosticReport{
		Version:  1,
		ID:       report.ID,
		Services: make(map[string][]jsonDiagnosticEntry),
	}

	for _, service := range report.Services {
		serviceStr := diagServiceString(service.Service)
		stateStr := diagStateString(service.State)

		jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonDiagnosticEntry{
			State:          stateStr,
			Remote:         service.RemoteAddr,
			Local:          service.LocalAddr,
			LastActivityUs: uint64(time.Now().Sub(service.LastActivity).Nanoseconds()),
		})
	}

	return json.Marshal(&jsonReport)
}

// DiagnosticsWithID returns information about the internal state of the SDK, using reportID as the name for the report.
//
// Experimental: This API is subject to change at any time.
func (c *Cluster) DiagnosticsWithID(reportID string) (*DiagnosticsReport, error) {
	cli, err := c.randomClient()
	if err != nil {
		return nil, err
	}

	provider, err := cli.getDiagnosticsProvider()
	if err != nil {
		return nil, err
	}

	agentReport, err := provider.Diagnostics()
	if err != nil {
		return nil, err
	}

	report := &DiagnosticsReport{
		ID:        reportID,
		ConfigRev: agentReport.ConfigRev,
	}
	for _, conn := range agentReport.MemdConns {
		state := DiagStateDisconnected
		if conn.LocalAddr != "" {
			state = DiagStateOk
		}

		report.Services = append(report.Services, DiagnosticEntry{
			Service:      MemdService,
			State:        state,
			LocalAddr:    conn.LocalAddr,
			RemoteAddr:   conn.RemoteAddr,
			LastActivity: conn.LastActivity,
		})
	}

	return report, nil
}

// Diagnostics returns information about the internal state of the SDK, using uuid as the name for the report.
//
// Experimental: This API is subject to change at any time.
func (c *Cluster) Diagnostics() (*DiagnosticsReport, error) {
	return c.DiagnosticsWithID(uuid.New().String())
}
