package gocb

import (
	"bytes"
	"testing"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v7"
)

func TestPingN1QlService(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		<-time.After(50 * time.Millisecond)

		var endpoint string
		switch req.Service {
		case gocbcore.FtsService:
			endpoint = "http://localhost:8093"
		case gocbcore.N1qlService:
			endpoint = "http://localhost:8092"
		case gocbcore.CbasService:
			endpoint = "http://localhost:8094"
		case gocbcore.CapiService:
			endpoint = "http://localhost:8091"
		default:
			return nil, errors.New("invalid service type")
		}

		req.Endpoint = endpoint

		return &gocbcore.HttpResponse{
			Endpoint:   endpoint,
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBufferString(""), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockHTTPProvider:  provider,
	}
	clients["mock-false"] = cli
	c := &Cluster{
		connections: clients,
	}
	b := &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			client:           c.getClient,
			AnalyticsTimeout: c.analyticsTimeout,
			N1qlTimeout:      c.n1qlTimeout,
			SearchTimeout:    c.searchTimeout,
			cachedClient:     cli,
		},
	}

	report, err := b.Ping(&PingOptions{Services: []ServiceType{N1qlService}})
	if err != nil {
		t.Fatalf("Expected ping to not return error but was %v", err)
	}

	if len(report.Services) != 1 {
		t.Fatalf("Expected report to have 1 service but has %d", len(report.Services))
	}

	service := report.Services[0]
	if service.Endpoint != "http://localhost:8092" {
		t.Fatalf("Expected service endpoint to be http://localhost:8092 but was %s", service.Endpoint)
	}

	if service.Service != N1qlService {
		t.Fatalf("Expected service type to be N1qlService but was %d", service.Service)
	}

	if !service.Success {
		t.Fatalf("Expected service success but wasn't")
	}

	if service.Latency < 50*time.Millisecond {
		t.Fatalf("Expected service latency to be over 50ms but was %d", service.Latency)
	}
}

func TestPingTimeout(t *testing.T) {
	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		req.Endpoint = "http://localhost:8092"
		<-req.Context.Done()
		return nil, req.Context.Err()
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	clients := make(map[string]client)
	cli := &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockHTTPProvider:  provider,
	}
	clients["mock-false"] = cli
	c := &Cluster{
		connections: clients,
	}
	c.ssb.n1qlTimeout = 10 * time.Millisecond

	b := &Bucket{
		sb: stateBlock{
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},

			client:           c.getClient,
			AnalyticsTimeout: c.analyticsTimeout,
			N1qlTimeout:      c.n1qlTimeout,
			SearchTimeout:    c.searchTimeout,
			cachedClient:     cli,
		},
	}

	report, err := b.Ping(&PingOptions{Services: []ServiceType{N1qlService}})
	if err != nil {
		t.Fatalf("Expected ping to not return error but was %v", err)
	}

	if len(report.Services) != 1 {
		t.Fatalf("Expected report to have 1 service but has %d", len(report.Services))
	}

	service := report.Services[0]
	if service.Endpoint != "http://localhost:8092" {
		t.Fatalf("Expected service endpoint to be http://localhost:8092 but was %s", service.Endpoint)
	}

	if service.Service != N1qlService {
		t.Fatalf("Expected service type to be N1qlService but was %d", service.Service)
	}

	if service.Success {
		t.Fatalf("Expected service success to be false")
	}

	if service.Latency != 0 {
		t.Fatalf("Expected service latency to be 0 but was %d", service.Latency)
	}
}
