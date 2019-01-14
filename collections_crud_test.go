package gocb

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/couchbase/gocbcore.v7"
)

func TestGetNoOptions(t *testing.T) {
	expectedBytes, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	var expected testBeerDocument
	err = json.Unmarshal(expectedBytes, &expected)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset: %v", err)
	}

	provider := &mockKvOperator{
		cas:      gocbcore.Cas(1),
		datatype: 1,
		value:    expectedBytes,
	}
	clients := make(map[string]client)
	clients["mock-false"] = &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockKvProvider:    provider,
	}

	c := &Cluster{
		connections: clients,
	}
	b := &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},
		},
	}

	col, err := b.DefaultCollection(nil)
	if err != nil {
		t.Fatalf("Opening collection encountered error: %v", err)
	}

	res, err := col.Get("key", nil)
	if err != nil {
		t.Fatalf("Get encountered error: %v", err)
	}

	if res.HasExpiration() {
		t.Fatalf("Expected document to not have an expiry")
	}

	if res.Cas() != Cas(1) {
		t.Fatalf("Expected cas value to be %d but was %d", Cas(1), res.Cas())
	}

	var doc testBeerDocument
	err = res.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content from result: %v", err)
	}

	if doc != expected {
		t.Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func TestGetWithExpiry(t *testing.T) {
	expectedBytes, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	var expected testBeerDocument
	err = json.Unmarshal(expectedBytes, &expected)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset: %v", err)
	}

	expiry := 10
	expiryBytes, err := json.Marshal(expiry)
	if err != nil {
		t.Fatalf("Could not marshal expiry: %v", err)
	}

	resultOps := make([]gocbcore.SubDocResult, 2)
	resultOps[0] = gocbcore.SubDocResult{
		Value: expiryBytes,
	}
	resultOps[1] = gocbcore.SubDocResult{
		Value: expectedBytes,
	}

	provider := &mockKvOperator{
		cas:      gocbcore.Cas(1),
		datatype: 1,
		value:    resultOps,
		opWait:   1 * time.Millisecond,
	}
	clients := make(map[string]client)
	clients["mock-false"] = &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockKvProvider:    provider,
	}

	c := &Cluster{
		connections: clients,
	}
	b := &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},
		},
	}

	col, err := b.DefaultCollection(nil)
	if err != nil {
		t.Fatalf("Opening collection encountered error: %v", err)
	}
	res, err := col.Get("key", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get encountered error: %v", err)
	}

	if !res.HasExpiration() {
		t.Fatalf("Expected document to have an expiry")
	}

	if res.Expiration() != uint32(expiry) {
		t.Fatalf("Expected expiry value to be %d but was %d", expiry, res.Expiration())
	}

	if res.Cas() != Cas(1) {
		t.Fatalf("Expected cas value to be %d but was %d", Cas(1), res.Cas())
	}

	var doc testBeerDocument
	err = res.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content from result: %v", err)
	}

	if doc != expected {
		t.Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func TestGetProject(t *testing.T) {
	var expected testBreweryDocument
	err := loadJSONTestDataset("beer_sample_brewery_projection", &expected)
	if err != nil {
		t.Fatalf("Could not load dataset: %v", err)
	}

	cityBytes := marshal(t, expected.City)
	countryBytes := marshal(t, expected.Country)
	accuracyBytes := marshal(t, expected.Geo.Accuracy)
	nameBytes := marshal(t, expected.Name)

	resultOps := make([]gocbcore.SubDocResult, 4)
	resultOps[0] = gocbcore.SubDocResult{
		Value: cityBytes,
	}
	resultOps[1] = gocbcore.SubDocResult{
		Value: countryBytes,
	}
	resultOps[2] = gocbcore.SubDocResult{
		Value: nameBytes,
	}
	resultOps[3] = gocbcore.SubDocResult{
		Value: accuracyBytes,
	}

	provider := &mockKvOperator{
		cas:      gocbcore.Cas(1),
		datatype: 1,
		value:    resultOps,
		opWait:   1 * time.Millisecond,
	}
	col := testGetCollection(t, provider)

	opts := GetOptions{Project: []string{"city", "country", "name", "geo.accuracy"}}
	res, err := col.Get("key", &opts)
	if err != nil {
		t.Fatalf("Get encountered error: %v", err)
	}

	if res.HasExpiration() {
		t.Fatalf("Expected document to not have an expiry")
	}

	if res.Cas() != Cas(1) {
		t.Fatalf("Expected cas value to be %d but was %d", Cas(1), res.Cas())
	}

	var doc testBreweryDocument
	err = res.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content from result: %v", err)
	}

	if doc != expected {
		t.Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func testGetCollection(t *testing.T, provider *mockKvOperator) *Collection {
	clients := make(map[string]client)
	clients["mock-false"] = &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
		mockKvProvider:    provider,
	}
	c := &Cluster{
		connections: clients,
	}
	b := &Bucket{
		sb: stateBlock{
			cluster: c,
			clientStateBlock: clientStateBlock{
				BucketName: "mock",
			},
		},
	}
	col, err := b.DefaultCollection(nil)
	if err != nil {
		t.Fatalf("Opening collection encountered error: %v", err)
	}
	return col
}
