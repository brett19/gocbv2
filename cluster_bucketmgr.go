package gocb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"gopkg.in/couchbase/gocbcore.v7"
)

// BucketManager provides methods for performing bucket management operations.
// See BucketManager for methods that allow creating and removing buckets themselves.
type BucketManager struct {
	httpClient httpProvider
}

// BucketType specifies the kind of bucket
type BucketType int

const (
	// Couchbase indicates a Couchbase bucket type.
	Couchbase = BucketType(0)

	// Memcached indicates a Memcached bucket type.
	Memcached = BucketType(1)

	// Ephemeral indicates an Ephemeral bucket type.
	Ephemeral = BucketType(2)
)

type bucketDataIn struct {
	Name         string `json:"name"`
	BucketType   string `json:"bucketType"`
	AuthType     string `json:"authType"`
	SaslPassword string `json:"saslPassword"`
	Quota        struct {
		Ram    int `json:"ram"`
		RawRam int `json:"rawRAM"`
	} `json:"quota"`
	ReplicaNumber int  `json:"replicaNumber"`
	ReplicaIndex  bool `json:"replicaIndex"`
	Controllers   struct {
		Flush string `json:"flush"`
	} `json:"controllers"`
}

// BucketSettings holds information about the settings for a bucket.
type BucketSettings struct {
	FlushEnabled  bool
	IndexReplicas bool
	Name          string
	Password      string
	Quota         int
	Replicas      int
	Type          BucketType
}

func bucketDataInToSettings(bucketData *bucketDataIn) *BucketSettings {
	settings := &BucketSettings{
		FlushEnabled:  bucketData.Controllers.Flush != "",
		IndexReplicas: bucketData.ReplicaIndex,
		Name:          bucketData.Name,
		Password:      bucketData.SaslPassword,
		Quota:         bucketData.Quota.Ram,
		Replicas:      bucketData.ReplicaNumber,
	}
	if bucketData.BucketType == "membase" {
		settings.Type = Couchbase
	} else if bucketData.BucketType == "memcached" {
		settings.Type = Memcached
	} else if bucketData.BucketType == "ephemeral" {
		settings.Type = Ephemeral
	} else {
		panic("Unrecognized bucket type string.")
	}
	if bucketData.AuthType != "sasl" {
		settings.Password = ""
	}
	return settings
}

// GetBuckets returns a list of all active buckets on the cluster.
func (bm *BucketManager) GetBuckets() ([]*BucketSettings, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    "/pools/default/buckets",
		Method:  "GET",
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return nil, networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	var bucketsData []*bucketDataIn
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&bucketsData)
	if err != nil {
		return nil, err
	}

	var buckets []*BucketSettings
	for _, bucketData := range bucketsData {
		buckets = append(buckets, bucketDataInToSettings(bucketData))
	}

	return buckets, nil
}

// InsertBucket creates a new bucket on the cluster.
func (bm *BucketManager) InsertBucket(settings *BucketSettings) error {
	posts := url.Values{}
	posts.Add("name", settings.Name)
	if settings.Type == Couchbase {
		posts.Add("bucketType", "couchbase")
	} else if settings.Type == Memcached {
		posts.Add("bucketType", "memcached")
	} else if settings.Type == Ephemeral {
		posts.Add("bucketType", "ephemeral")
	} else {
		panic("Unrecognized bucket type.")
	}
	if settings.FlushEnabled {
		posts.Add("flushEnabled", "1")
	} else {
		posts.Add("flushEnabled", "0")
	}
	posts.Add("replicaNumber", fmt.Sprintf("%d", settings.Replicas))
	posts.Add("authType", "sasl")
	posts.Add("saslPassword", settings.Password)
	posts.Add("ramQuotaMB", fmt.Sprintf("%d", settings.Quota))

	data := []byte(posts.Encode())

	req := &gocbcore.HttpRequest{
		Service:     gocbcore.ServiceType(MgmtService),
		Path:        "/pools/default/buckets",
		Method:      "POST",
		Body:        data,
		ContentType: "application/x-www-form-urlencoded",
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 202 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}

// UpdateBucket will update the settings for a specific bucket on the cluster.
func (bm *BucketManager) UpdateBucket(settings *BucketSettings) error {
	// Cluster-side, updates are the same as creates.
	return bm.InsertBucket(settings)
}

// RemoveBucket will delete a bucket from the cluster by name.
func (bm *BucketManager) RemoveBucket(name string) error {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s", name),
		Method:  "DELETE",
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return networkError{statusCode: resp.StatusCode, message: string(data)}
	}

	return nil
}

// Flush will delete all the of the data from a bucket.
// Keep in mind that you must have flushing enabled in the buckets configuration.
func (bm *BucketManager) Flush(name string) error {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(MgmtService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/controller/doFlush", name),
		Method:  "POST",
	}

	resp, err := bm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		err = resp.Body.Close()
		if err != nil {
			logDebugf("Failed to close socket (%s)", err)
		}
		return networkError{statusCode: resp.StatusCode, message: string(data)}
	}
	return nil
}
