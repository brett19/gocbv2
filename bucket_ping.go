package gocb

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"gopkg.in/couchbase/gocbcore.v7"
)

// PingServiceEntry represents a single entry in a ping report.
type PingServiceEntry struct {
	Service  ServiceType
	Endpoint string
	Success  bool
	Latency  time.Duration
}

// PingReport encapsulates the details from a executed ping operation.
type PingReport struct {
	Services []PingServiceEntry
	ID       string
}

type jsonPingServiceEntry struct {
	Remote    string `json:"remote"`
	LatencyUs uint64 `json:"latency_us"`
	Success   bool   `json:"success"`
}

type jsonPingReport struct {
	Version  int                               `json:"version"`
	ID       string                            `json:"id"`
	Sdk      string                            `json:"sdk"`
	Services map[string][]jsonPingServiceEntry `json:"services"`
}

// MarshalJSON generates a JSON representation of this ping report.
func (report *PingReport) MarshalJSON() ([]byte, error) {
	jsonReport := jsonPingReport{
		Version:  1,
		ID:       report.ID,
		Sdk:      "gocb/" + Version() + " " + "gocbcore/" + gocbcore.Version(),
		Services: make(map[string][]jsonPingServiceEntry),
	}

	for _, service := range report.Services {
		serviceStr := diagServiceString(service.Service)
		jsonReport.Services[serviceStr] = append(jsonReport.Services[serviceStr], jsonPingServiceEntry{
			Remote:    service.Endpoint,
			LatencyUs: uint64(service.Latency / time.Nanosecond),
		})
	}

	return json.Marshal(&jsonReport)
}

func (jsonReport *jsonPingReport) toReport() *PingReport {
	report := &PingReport{
		ID: jsonReport.ID,
	}

	for key, jsonServices := range jsonReport.Services {
		for _, jsonService := range jsonServices {
			report.Services = append(report.Services, PingServiceEntry{
				Service:  diagStringService(key),
				Endpoint: jsonService.Remote,
				Latency:  time.Duration(jsonService.LatencyUs) * time.Nanosecond,
				Success:  jsonService.Success,
			})
		}
	}

	return report
}

func (b *Bucket) pingKv(provider diagnosticsProvider) (pingsOut []gocbcore.PingResult, errOut error) {
	signal := make(chan bool, 1)

	op, err := provider.PingKvEx(gocbcore.PingKvOptions{}, func(services *gocbcore.PingKvResult, err error) {
		if err != nil {
			errOut = err
			signal <- true
			return
		}

		results := services.Services
		pingsOut = make([]gocbcore.PingResult, len(results))
		for pingIdx, ping := range results {
			// We rewrite the cancelled errors into timeout errors here.
			if ping.Error == gocbcore.ErrCancelled {
				ping.Error = timeoutError{}
			}
			pingsOut[pingIdx] = ping
		}
		signal <- true
	})
	if err != nil {
		return nil, err
	}

	timeoutTmr := gocbcore.AcquireTimer(b.sb.KvTimeout)
	select {
	case <-signal:
		gocbcore.ReleaseTimer(timeoutTmr, false)
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-signal
			return
		}
		return nil, timeoutError{}
	}
}

// PingOptions is the options available to the ping command.
type PingOptions struct {
	Services []ServiceType
	ReportID string
}

// Ping will ping a list of services and verify they are active and
// responding in an acceptable period of time.
//
// Experimental: This API is subject to change at any time.
func (b *Bucket) Ping(opts *PingOptions) (*PingReport, error) {
	if opts == nil {
		opts = &PingOptions{}
	}

	numServices := 0
	waitCh := make(chan error, 10)
	report := &PingReport{}
	var reportLock sync.Mutex
	services := opts.Services

	report.ID = opts.ReportID
	if report.ID == "" {
		report.ID = uuid.New().String()
	}

	if services == nil {
		services = []ServiceType{
			MemdService,
			CapiService,
			N1qlService,
			FtsService,
		}
	}

	httpReq := func(service ServiceType, url string) (time.Duration, string, error) {
		startTime := time.Now()

		cli := b.sb.getCachedClient()
		provider, err := cli.getHTTPProvider()
		if err != nil {
			return 0, "", err
		}

		timeout := 60 * time.Second
		if service == N1qlService {
			timeout = b.sb.N1qlTimeout()
		} else if service == FtsService {
			timeout = b.sb.SearchTimeout()
		} else if service == CbasService {
			timeout = b.sb.AnalyticsTimeout()
		}

		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()

		req := gocbcore.HttpRequest{
			Method:  "GET",
			Path:    url,
			Service: gocbcore.ServiceType(service),
			Context: ctx,
		}

		resp, err := provider.DoHttpRequest(&req)
		if err != nil {
			return 0, req.Endpoint, err
		}

		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close http request: %s", err)
		}

		pingLatency := time.Now().Sub(startTime)

		return pingLatency, req.Endpoint, err
	}

	for _, serviceType := range services {
		switch serviceType {
		case MemdService:
			numServices++
			go func() {
				cli := b.sb.getCachedClient()
				provider, err := cli.getDiagnosticsProvider()
				if err != nil {
					logWarnf("Failed to get KV provider for report: %s", err)
					waitCh <- nil
					return
				}

				pings, err := b.pingKv(provider)
				if err != nil {
					logWarnf("Failed to ping KV for report: %s", err)
					waitCh <- nil
					return
				}

				reportLock.Lock()
				// We intentionally ignore errors here and simply include
				// any non-error pings that we have received.  Note that
				// gocbcore's ping command, when cancelled, still returns
				// any pings that had occurred before the operation was
				// cancelled and then marks the rest as errors.
				for _, ping := range pings {
					wasSuccess := true
					if ping.Error != nil {
						wasSuccess = false
					}

					report.Services = append(report.Services, PingServiceEntry{
						Service:  MemdService,
						Endpoint: ping.Endpoint,
						Success:  wasSuccess,
						Latency:  ping.Latency,
					})
				}
				reportLock.Unlock()
				waitCh <- nil
			}()
		case CapiService:
			// View Service is not currently supported as a ping target
		case N1qlService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(N1qlService, "/admin/ping")

				reportLock.Lock()
				if err != nil {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  N1qlService,
						Endpoint: endpoint,
						Success:  false,
					})
				} else {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  N1qlService,
						Endpoint: endpoint,
						Success:  true,
						Latency:  pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case FtsService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(FtsService, "/api/ping")

				reportLock.Lock()
				if err != nil {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  FtsService,
						Endpoint: endpoint,
						Success:  false,
					})
				} else {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  FtsService,
						Endpoint: endpoint,
						Success:  true,
						Latency:  pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		case CbasService:
			numServices++
			go func() {
				pingLatency, endpoint, err := httpReq(CbasService, "/admin/ping")

				reportLock.Lock()
				if err != nil {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  CbasService,
						Endpoint: endpoint,
						Success:  false,
					})
				} else {
					report.Services = append(report.Services, PingServiceEntry{
						Service:  CbasService,
						Endpoint: endpoint,
						Success:  true,
						Latency:  pingLatency,
					})
				}
				reportLock.Unlock()

				waitCh <- nil
			}()
		}
	}

	for i := 0; i < numServices; i++ {
		<-waitCh
	}

	return report, nil
}
