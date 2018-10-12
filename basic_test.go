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

	doc, err := NewDocument("sometext")
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	err = col.Upsert("testkey", doc, &SetOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	err = col.Remove("testkey", &RemoveOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	doc, err = NewDocument("someothertext")
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}
	err = col.Insert("testkey", doc, &SetOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	doc, err = NewDocument("someothertextagain")
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}
	err = col.Replace("testkey", doc, &ReplaceOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	docg, err := col.Get("testkey", &GetOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	// duraBucket := bucket.WithDurability(1, 0)
	// duraCol := duraBucket.DefaultCollection()

	// err = duraCol.Set("testkey", "sometext")
	// if err != nil {
	// 	t.Fatalf("Failed to duraset key: %s", err)
	// }

	t.Logf("doc: %+v", docg)
}
