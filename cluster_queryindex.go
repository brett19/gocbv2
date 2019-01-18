package gocb

import (
	"strings"
	"time"
)

// QueryIndexManager provides methods for performing Couchbase N1ql index management.
type QueryIndexManager struct {
	ExecuteQuery func(statement string, opts *QueryOptions) (*QueryResults, error)
}

// IndexInfo represents a Couchbase GSI index.
type IndexInfo struct {
	Name      string    `json:"name"`
	IsPrimary bool      `json:"is_primary"`
	Type      IndexType `json:"using"`
	State     string    `json:"state"`
	Keyspace  string    `json:"keyspace_id"`
	Namespace string    `json:"namespace_id"`
	IndexKey  []string  `json:"index_key"`
}

func (qm *QueryIndexManager) createIndex(bucketName, indexName string, fields []string, ignoreIfExists, deferred bool) error {
	var qs string

	if len(fields) == 0 {
		qs += "CREATE PRIMARY INDEX"
	} else {
		qs += "CREATE INDEX"
	}
	if indexName != "" {
		qs += " `" + indexName + "`"
	}
	qs += " ON `" + bucketName + "`"
	if len(fields) > 0 {
		qs += " ("
		for i := 0; i < len(fields); i++ {
			if i > 0 {
				qs += ", "
			}
			qs += "`" + fields[i] + "`"
		}
		qs += ")"
	}
	if deferred {
		qs += " WITH {\"defer_build\": true}"
	}

	rows, err := qm.ExecuteQuery(qs, nil)
	if err != nil {
		if strings.Contains(err.Error(), "already exist") {
			if ignoreIfExists {
				return nil
			}
			return ErrIndexAlreadyExists
		}
		return err
	}

	return rows.Close()
}

// CreateIndex creates an index over the specified fields.
func (qm *QueryIndexManager) CreateIndex(bucketName, indexName string, fields []string, ignoreIfExists, deferred bool) error {
	if indexName == "" {
		return ErrIndexInvalidName
	}
	if len(fields) <= 0 {
		return ErrIndexNoFields
	}
	return qm.createIndex(bucketName, indexName, fields, ignoreIfExists, deferred)
}

// CreatePrimaryIndex creates a primary index.  An empty customName uses the default naming.
func (qm *QueryIndexManager) CreatePrimaryIndex(bucketName string, customName string, ignoreIfExists, deferred bool) error {
	return qm.createIndex(bucketName, customName, nil, ignoreIfExists, deferred)
}

func (qm *QueryIndexManager) dropIndex(bucketName, indexName string, ignoreIfNotExists bool) error {
	var qs string

	if indexName == "" {
		qs += "DROP PRIMARY INDEX ON `" + bucketName + "`"
	} else {
		qs += "DROP INDEX `" + bucketName + "`.`" + indexName + "`"
	}

	rows, err := qm.ExecuteQuery(qs, nil)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			if ignoreIfNotExists {
				return nil
			}
			return ErrIndexNotFound
		}
		return err
	}

	return rows.Close()
}

// DropIndex drops a specific index by name.
func (qm *QueryIndexManager) DropIndex(bucketName, indexName string, ignoreIfNotExists bool) error {
	if indexName == "" {
		return ErrIndexInvalidName
	}
	return qm.dropIndex(bucketName, indexName, ignoreIfNotExists)
}

// DropPrimaryIndex drops the primary index.  Pass an empty customName for unnamed primary indexes.
func (qm *QueryIndexManager) DropPrimaryIndex(bucketName, customName string, ignoreIfNotExists bool) error {
	return qm.dropIndex(bucketName, customName, ignoreIfNotExists)
}

// GetIndexes returns a list of all currently registered indexes.
func (qm *QueryIndexManager) GetIndexes() ([]IndexInfo, error) {
	q := "SELECT `indexes`.* FROM system:indexes"
	rows, err := qm.ExecuteQuery(q, nil)
	if err != nil {
		return nil, err
	}

	var indexes []IndexInfo
	var index IndexInfo
	for rows.Next(&index) {
		indexes = append(indexes, index)
		index = IndexInfo{}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// BuildDeferredIndexes builds all indexes which are currently in deferred state.
func (qm *QueryIndexManager) BuildDeferredIndexes(bucketName string) ([]string, error) {
	indexList, err := qm.GetIndexes()
	if err != nil {
		return nil, err
	}

	var deferredList []string
	for i := 0; i < len(indexList); i++ {
		var index = indexList[i]
		if index.State == "deferred" || index.State == "pending" {
			deferredList = append(deferredList, index.Name)
		}
	}

	if len(deferredList) == 0 {
		// Don't try to build an empty index list
		return nil, nil
	}

	var qs string
	qs += "BUILD INDEX ON `" + bucketName + "`("
	for i := 0; i < len(deferredList); i++ {
		if i > 0 {
			qs += ", "
		}
		qs += "`" + deferredList[i] + "`"
	}
	qs += ")"

	rows, err := qm.ExecuteQuery(qs, nil)
	if err != nil {
		return nil, err
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	return deferredList, nil
}

func checkIndexesActive(indexes []IndexInfo, checkList []string) (bool, error) {
	var checkIndexes []IndexInfo
	for i := 0; i < len(checkList); i++ {
		indexName := checkList[i]

		for j := 0; j < len(indexes); j++ {
			if indexes[j].Name == indexName {
				checkIndexes = append(checkIndexes, indexes[j])
				break
			}
		}
	}

	if len(checkIndexes) != len(checkList) {
		return false, ErrIndexNotFound
	}

	for i := 0; i < len(checkIndexes); i++ {
		if checkIndexes[i].State != "online" {
			return false, nil
		}
	}
	return true, nil
}

// WatchIndexes waits for a set of indexes to come online
func (qm *QueryIndexManager) WatchIndexes(watchList []string, watchPrimary bool, timeout time.Duration) error {
	if watchPrimary {
		watchList = append(watchList, "#primary")
	}

	curInterval := 50 * time.Millisecond
	timeoutTime := time.Now().Add(timeout)
	for {
		indexes, err := qm.GetIndexes()
		if err != nil {
			return err
		}

		allOnline, err := checkIndexesActive(indexes, watchList)
		if err != nil {
			return err
		}

		if allOnline {
			break
		}

		curInterval += 500 * time.Millisecond
		if curInterval > 1000 {
			curInterval = 1000
		}

		if time.Now().Add(curInterval).After(timeoutTime) {
			return timeoutError{}
		}

		// Wait till our next poll interval
		time.Sleep(curInterval)
	}

	return nil
}
