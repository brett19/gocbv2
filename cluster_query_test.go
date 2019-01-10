package gocb

import (
	"fmt"
	"testing"
)

func TestBasicQuery(t *testing.T) {
	query := "select COUNT(*) from test"
	res, err := globalCluster.Query(query, nil, NewQueryOptions())
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(res)
}
