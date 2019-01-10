package gocb

import (
	"bytes"
	"encoding/json"
	"testing"

	"gopkg.in/couchbase/gocbcore.v7"
)

func TestGetResultCas(t *testing.T) {
	cas := Cas(10)
	res := GetResult{
		cas: cas,
	}

	if res.Cas() != cas {
		t.Fatalf("Cas value should have been %d but was %d", cas, res.Cas())
	}
}

func TestGetResultHasExpiry(t *testing.T) {
	res := GetResult{}

	if res.HasExpiration() {
		t.Fatalf("HasExpiry should have returned false but returned true")
	}

	res.withExpiration = true

	if !res.HasExpiration() {
		t.Fatalf("HasExpiry should have returned true but returned false")
	}
}

func TestGetResultExpiry(t *testing.T) {
	res := GetResult{
		expiration: 10,
	}

	if res.Expiration() != 10 {
		t.Fatalf("Expiry value should have been 10 but was %d", res.Expiration())
	}
}

func TestGetResultContent(t *testing.T) {
	dataset, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		t.Fatalf("Failed to load dataset: %v", err)
	}

	var expected testBeerDocument
	err = json.Unmarshal(dataset, &expected)
	if err != nil {
		t.Fatalf("Failed to unmarshal dataset: %v", err)
	}

	res := GetResult{
		contents: dataset,
	}

	var doc testBeerDocument
	err = res.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content: %v", err)
	}

	// expected := "512_brewing_company (512) Bruin North American Ale"
	if doc != expected {
		t.Fatalf("Document value should have been %+v but was %+v", expected, doc)
	}
}

func TestGetResultDecode(t *testing.T) {
	tBytes, err := loadRawTestDataset("beer_sample_single")
	if err != nil {
		t.Fatalf("Failed to load dataset: %v", err)
	}

	flags := uint32(gocbcore.StringType)
	res := GetResult{
		contents: tBytes,
		flags:    flags,
	}

	var doc []byte
	err = res.Decode(&doc, func(dBytes []byte, dFlags uint32, out interface{}) error {
		if bytes.Compare(tBytes, dBytes) != 0 {
			t.Fatalf("Decode function bytes did not match as expected")
		}

		if dFlags != flags {
			t.Fatalf("Decode function flags did not match, expected %d was %d", flags, dFlags)
		}

		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = dBytes
			return nil
		default:
			t.Fatalf("Expected out to be *[]byte but was not")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if bytes.Compare(tBytes, doc) != 0 {
		t.Fatalf("Decoded return did not match as expected")
	}
}

func TestGetResultFromSubDoc(t *testing.T) {
	ops := make([]gocbcore.SubDocOp, 3)
	ops[0] = gocbcore.SubDocOp{
		Path: "id",
	}
	ops[1] = gocbcore.SubDocOp{
		Path: "name",
	}
	ops[2] = gocbcore.SubDocOp{
		Path: "address.house.number",
	}

	results := &LookupInResult{
		contents: make([]lookupInPartial, 3),
	}

	var err error
	results.contents[0].data, err = json.Marshal("key")
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[1].data, err = json.Marshal("barry")
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}
	results.contents[2].data, err = json.Marshal(11)
	if err != nil {
		t.Fatalf("Failed to marshal content: %v", err)
	}

	type house struct {
		Number int `json:"number"`
	}
	type address struct {
		House house `json:"house"`
	}
	type person struct {
		ID      string
		Name    string
		Address address `json:"address"`
	}
	var doc person
	var getResult GetResult
	err = getResult.fromSubDoc(ops, results)
	if err != nil {
		t.Fatalf("Failed to create result from subdoc: %v", err)
	}

	err = getResult.Content(&doc)
	if err != nil {
		t.Fatalf("Failed to get content: %v", err)
	}

	if doc.ID != "key" {
		t.Fatalf("Document value should have been %s but was %s", "key", doc.ID)
	}

	if doc.Name != "barry" {
		t.Fatalf("Document value should have been %s but was %s", "barry", doc.ID)
	}

	if doc.Address.House.Number != 11 {
		t.Fatalf("Document value should have been %d but was %d", 11, doc.Address.House.Number)
	}
}
