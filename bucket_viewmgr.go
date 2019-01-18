package gocb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"gopkg.in/couchbase/gocbcore.v7"
)

type ViewManager struct {
	bucket     *Bucket
	httpClient httpProvider
}

// View represents a Couchbase view within a design document.
type View struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

func (v View) hasReduce() bool {
	return v.Reduce != ""
}

// DesignDocument represents a Couchbase design document containing multiple views.
type DesignDocument struct {
	Name         string          `json:"-"`
	Views        map[string]View `json:"views,omitempty"`
	SpatialViews map[string]View `json:"spatial,omitempty"`
}

// GetDesignDocument retrieves a single design document for the given bucket..
func (vm ViewManager) GetDesignDocument(name string) (*DesignDocument, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", name),
		Method:  "GET",
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
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

	ddocObj := DesignDocument{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocObj)
	if err != nil {
		return nil, err
	}

	ddocObj.Name = name
	return &ddocObj, nil
}

// GetDesignDocuments will retrieve all design documents for the given bucket.
func (vm ViewManager) GetDesignDocuments() ([]*DesignDocument, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/pools/default/buckets/%s/ddocs", vm.bucket.Name()),
		Method:  "GET",
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
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

	var ddocsObj struct {
		Rows []struct {
			Doc struct {
				Meta struct {
					Id string
				}
				Json DesignDocument
			}
		}
	}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&ddocsObj)
	if err != nil {
		return nil, err
	}

	var ddocs []*DesignDocument
	for index, ddocData := range ddocsObj.Rows {
		ddoc := &ddocsObj.Rows[index].Doc.Json
		ddoc.Name = ddocData.Doc.Meta.Id[8:]
		ddocs = append(ddocs, ddoc)
	}

	return ddocs, nil
}

// InsertDesignDocument inserts a design document to the given bucket.
func (vm ViewManager) InsertDesignDocument(ddoc *DesignDocument) error {
	oldDdoc, err := vm.GetDesignDocument(ddoc.Name)
	if oldDdoc != nil || err == nil {
		// return clientError{"Design document already exists"} TODO
	}
	return vm.UpsertDesignDocument(ddoc)
}

// UpsertDesignDocument will insert a design document to the given bucket, or update
// an existing design document with the same name.
func (vm ViewManager) UpsertDesignDocument(ddoc *DesignDocument) error {
	data, err := json.Marshal(&ddoc)
	if err != nil {
		return err
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", ddoc.Name),
		Method:  "PUT",
		Body:    data,
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 201 {
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

// RemoveDesignDocument will remove a design document from the given bucket.
func (vm ViewManager) RemoveDesignDocument(name string) error {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.ServiceType(CapiService),
		Path:    fmt.Sprintf("/_design/%s", name),
		Method:  "DELETE",
	}

	resp, err := vm.httpClient.DoHttpRequest(req)
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
