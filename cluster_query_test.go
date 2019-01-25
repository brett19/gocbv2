package gocb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/couchbase/gocbcore.v7"
)

func TestBasicQuery(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_dataset")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	queryOptions := &QueryOptions{
		PositionalParameters: []interface{}{"brewery"},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

		var opts map[string]interface{}
		err := json.Unmarshal(req.Body, &opts)
		if err != nil {
			t.Fatalf("Failed to unmarshal request body %v", err)
		}

		if len(opts) != 3 {
			t.Fatalf("Expected request body to contain 3 options but was %d, %v", len(opts), opts)
		}

		optsStatement, ok := opts["statement"]
		if !ok {
			t.Fatalf("Request query options missing statement")
		}
		if optsStatement != statement {
			t.Fatalf("Expected statement to be %s but was %s", statement, optsStatement)
		}
		optsTimeout, ok := opts["timeout"]
		if !ok {
			t.Fatalf("Request query options missing timeout")
		}
		optsDuration, err := time.ParseDuration(optsTimeout.(string))
		if err != nil {
			t.Fatalf("Failed to parse request timeout %v", err)
		}
		if optsDuration != timeout {
			t.Fatalf("Expected timeout to be %s but was %s", timeout, optsDuration)
		}

		optsParams, ok := opts["args"].([]interface{})
		if !ok {
			t.Fatalf("Request query options missing args")
		}
		if len(optsParams) != 1 {
			t.Fatalf("Expected args to be length 1 but was %d", len(optsParams))
		}
		if optsParams[0] != "brewery" {
			t.Fatalf("Expected args content to be brewery but was %s", optsParams[0])
		}

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8092",
			StatusCode: 200,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	res, err := cluster.Query(statement, queryOptions)
	if err != nil {
		t.Fatal(err)
	}

	testAssertQueryResult(t, &expectedResult, res, true)
}

func TestQueryError(t *testing.T) {
	dataBytes, err := loadRawTestDataset("beer_sample_query_error")
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	var expectedResult n1qlResponse
	err = json.Unmarshal(dataBytes, &expectedResult)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset %v", err)
	}

	queryOptions := &QueryOptions{
		PositionalParameters: []interface{}{"brewery"},
	}

	statement := "select `beer-sample`.* from `beer-sample` WHERE `type` = ? ORDER BY brewery_id, name"
	timeout := 60 * time.Second

	doHTTP := func(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error) {
		testAssertQueryRequest(t, req)

		return &gocbcore.HttpResponse{
			Endpoint:   "http://localhost:8092",
			StatusCode: 400,
			Body:       &testReadCloser{bytes.NewBuffer(dataBytes), nil},
		}, nil
	}

	provider := &mockHTTPProvider{
		doFn: doHTTP,
	}

	cluster := testGetClusterForHTTP(provider, timeout, 0, 0)

	_, err = cluster.Query(statement, queryOptions)
	if err == nil {
		t.Fatal("Expected query to return error")
	}

	queryErrs, ok := err.(QueryErrors)
	if !ok {
		t.Fatalf("Expected error to be QueryErrors but was %s", reflect.TypeOf(err).String())
	}

	if queryErrs.Endpoint() != "localhost:8092" {
		t.Fatalf("Expected error endpoint to be localhost:8092 but was %s", queryErrs.Endpoint())
	}

	if queryErrs.HTTPStatus() != 400 {
		t.Fatalf("Expected error HTTP status to be 400 but was %d", queryErrs.HTTPStatus())
	}

	if queryErrs.ContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected error ContextID to be %s but was %s", expectedResult.ClientContextID, queryErrs.ContextID())
	}

	if len(queryErrs.Errors()) != len(expectedResult.Errors) {
		t.Fatalf("Expected errors to contain 1 error but contained %d", len(queryErrs.Errors()))
	}

	var errs []string
	errors := queryErrs.Errors()
	for i, err := range expectedResult.Errors {
		msg := fmt.Sprintf("[%d] %s", err.ErrorCode, err.ErrorMessage)
		errs = append(errs, msg)

		if errors[i].Code() != err.ErrorCode {
			t.Fatalf("Expected error code to be %d but was %d", errors[i].Code(), err.ErrorCode)
		}

		if errors[i].Message() != err.ErrorMessage {
			t.Fatalf("Expected error message to be %s but was %s", errors[i].Message(), err.ErrorMessage)
		}

		if errors[i].Error() != msg {
			t.Fatalf("Expected error Error() to be %s but was %s", errors[i].Error(), msg)
		}
	}
	joinedErrs := strings.Join(errs, ", ")
	if queryErrs.Error() != joinedErrs {
		t.Fatalf("Expected error Error() to be %s but was %s", joinedErrs, queryErrs.Error())
	}
}

func testAssertQueryRequest(t *testing.T, req *gocbcore.HttpRequest) {
	if req.Service != gocbcore.N1qlService {
		t.Fatalf("Service should have been N1qlService but was %d", req.Service)
	}

	if req.Context == nil {
		t.Fatalf("Context should not have been nil, but was")
	}

	_, ok := req.Context.Deadline()
	if !ok {
		t.Fatalf("Context should have had a deadline") // Difficult to test the actual deadline value
	}

	if req.Method != "POST" {
		t.Fatalf("Request method should have been POST but was %s", req.Method)
	}

	if req.Path != "/query/service" {
		t.Fatalf("Request path should have been /query/service but was %s", req.Path)
	}
}

func testAssertQueryResult(t *testing.T, expectedResult *n1qlResponse, actualResult *QueryResults, expectData bool) {
	if expectData {
		var breweryDocs []testBreweryDocument
		var resDoc testBreweryDocument
		for actualResult.Next(&resDoc) {
			breweryDocs = append(breweryDocs, resDoc)
		}

		var expectedDocs []testBreweryDocument
		for _, doc := range expectedResult.Results {
			var expectedDoc testBreweryDocument
			err := json.Unmarshal(doc, &expectedDoc)
			if err != nil {
				t.Fatalf("Unmarshalling expected result document failed %v", err)
			}
			expectedDocs = append(expectedDocs, expectedDoc)
		}

		if len(breweryDocs) != len(expectedResult.Results) {
			t.Fatalf("Expected results length to be %d but was %d", len(expectedResult.Results), len(breweryDocs))
		}

		for i, doc := range expectedDocs {
			if breweryDocs[i] != doc {
				t.Fatalf("Docs did not match, expected %v but was %v", doc, breweryDocs[i])
			}
		}
	}

	if actualResult.ClientContextID() != expectedResult.ClientContextID {
		t.Fatalf("Expected ClientContextID to be %s but was %s", expectedResult.ClientContextID, actualResult.ClientContextID())
	}

	if actualResult.RequestID() != expectedResult.RequestID {
		t.Fatalf("Expected RequestID to be %s but was %s", expectedResult.RequestID, actualResult.RequestID())
	}
	if actualResult.SourceEndpoint() != "localhost:8092" {
		t.Fatalf("Expected endpoint to be %s but was %s", "localhost:8092", actualResult.SourceEndpoint())
	}

	metrics := actualResult.Metrics()
	elapsedTime, err := time.ParseDuration(expectedResult.Metrics.ElapsedTime)
	if err != nil {
		t.Fatalf("Failed to parse ElapsedTime %v", err)
	}
	if metrics.ElapsedTime != elapsedTime {
		t.Fatalf("Expected metrics ElapsedTime to be %s but was %s", metrics.ElapsedTime, elapsedTime)
	}

	executionTime, err := time.ParseDuration(expectedResult.Metrics.ExecutionTime)
	if err != nil {
		t.Fatalf("Failed to parse ElapsedTime %v", err)
	}
	if metrics.ExecutionTime != executionTime {
		t.Fatalf("Expected metrics ElapsedTime to be %s but was %s", metrics.ExecutionTime, executionTime)
	}

	if metrics.MutationCount != expectedResult.Metrics.MutationCount {
		t.Fatalf("Expected metrics MutationCount to be %d but was %d", metrics.MutationCount, expectedResult.Metrics.MutationCount)
	}

	if metrics.ErrorCount != expectedResult.Metrics.ErrorCount {
		t.Fatalf("Expected metrics ErrorCount to be %d but was %d", metrics.ErrorCount, expectedResult.Metrics.ErrorCount)
	}

	if metrics.ResultCount != expectedResult.Metrics.ResultCount {
		t.Fatalf("Expected metrics ResultCount to be %d but was %d", metrics.ResultCount, expectedResult.Metrics.ResultCount)
	}

	if metrics.ResultSize != expectedResult.Metrics.ResultSize {
		t.Fatalf("Expected metrics ResultSize to be %d but was %d", metrics.ResultSize, expectedResult.Metrics.ResultSize)
	}

	if metrics.SortCount != expectedResult.Metrics.SortCount {
		t.Fatalf("Expected metrics SortCount to be %d but was %d", metrics.SortCount, expectedResult.Metrics.SortCount)
	}

	if metrics.WarningCount != expectedResult.Metrics.WarningCount {
		t.Fatalf("Expected metrics WarningCount to be %d but was %d", metrics.WarningCount, expectedResult.Metrics.WarningCount)
	}
}

func testGetClusterForHTTP(provider *mockHTTPProvider, n1qlTimeout, analyticsTimeout, searchTimeout time.Duration) *Cluster {
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
	c.ssb.n1qlTimeout = n1qlTimeout
	c.ssb.analyticsTimeout = analyticsTimeout
	c.ssb.searchTimeout = searchTimeout

	return c
}
