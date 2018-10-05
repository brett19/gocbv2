package gocb

import (
	"fmt"
	"testing"
)

func TestBasicConnect(t *testing.T) {
	SetLogger(VerboseStdioLogger())
	auth := PasswordAuthenticator{
		Username: "Administrator",
		Password: "password",
	}

	cluster, err := Connect("couchbase://10.112.191.101", auth)
	if err != nil {
		t.Fatalf("Failed to connect to cluster: %s", err)
	}

	bucket := cluster.Bucket("test")
	col := bucket.Collection("test9")
	col2 := col.WithDurability(1, 1)

	doc, err := NewDocument("sometext")
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	err = col.Upsert("testkey", doc, &SetOptions{})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}

	err = col.Remove("testkey", &RemoveOptions{})
	// if err != nil {
	// 	t.Fatalf("Failed to fetch key: %s", err)
	// }

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

	fmt.Print(col2)

	// duraBucket := bucket.WithDurability(1, 0)
	// duraCol := duraBucket.DefaultCollection()

	// err = duraCol.Set("testkey", "sometext")
	// if err != nil {
	// 	t.Fatalf("Failed to duraset key: %s", err)
	// }

	t.Logf("doc: %+v", docg)
}
