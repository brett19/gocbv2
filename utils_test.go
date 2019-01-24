package gocb

import (
	"encoding/json"
	"io/ioutil"
	"testing"
)

type testBeerDocument struct {
	ABV         float32 `json:"abv,omitempty"`
	BreweryID   string  `json:"brewery_id,omitempty"`
	Category    string  `json:"category,omitempty"`
	Description string  `json:"description,omitempty"`
	IBU         int     `json:"IBU,omitempty"`
	Name        string  `json:"name,omitempty"`
	SRM         int     `json:"srm,omitempty"`
	Style       string  `json:"style,omitempty"`
	Type        string  `json:"type,omitempty"`
	UPC         int     `json:"upc,omitempty"`
	Updated     string  `json:"updated,omitempty"`
}

type testBreweryGeo struct {
	Accuracy string  `json:"accuracy,omitempty"`
	Lat      float32 `json:"lat,omitempty"`
	Lon      float32 `json:"lon,omitempty"`
}

type testBreweryDocument struct {
	City        string         `json:"city,omitempty"`
	Code        string         `json:"code,omitempty"`
	Country     string         `json:"country,omitempty"`
	Description string         `json:"description,omitempty"`
	Geo         testBreweryGeo `json:"geo,omitempty"`
	Name        string         `json:"name,omitempty"`
	Phone       string         `json:"phone,omitempty"`
	State       string         `json:"state,omitempty"`
	Type        string         `json:"type,omitempty"`
	Updated     string         `json:"updated,omitempty"`
	Website     string         `json:"website,omitempty"`
}

func loadRawTestDataset(dataset string) ([]byte, error) {
	return ioutil.ReadFile("testdata/" + dataset + ".json")
}

func loadJSONTestDataset(dataset string, valuePtr interface{}) error {
	bytes, err := loadRawTestDataset(dataset)

	err = json.Unmarshal(bytes, &valuePtr)
	if err != nil {
		return err
	}

	return nil
}

func marshal(t *testing.T, value interface{}) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Could not marshal value: %v", err)
	}

	return b
}
