package gocb

import (
	"testing"
)

func TestBasicConnect(t *testing.T) {
	auth := PasswordAuthenticator{
		Username: "Administrator",
		Password: "C0uchbase",
	}

	cluster, err := Connect("couchbase://192.168.0.112", auth)
	if err != nil {
		t.Fatalf("Failed to connect to cluster: %s", err)
	}

	bucket := cluster.Bucket("test")
	col := bucket.DefaultCollection()

	doc, err := col.Get("testkey")
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	duraBucket := bucket.WithDurability(1, 0)
	duraCol := duraBucket.DefaultCollection()

	err := duraCol.Set("testkey", "sometext")

	t.Logf("doc: %+v", doc)
}
