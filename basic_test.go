package gocb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

type details struct {
	Name        string   `json:"name,omitempty"`
	Id          int      `json:"id,omitempty"`
	MiddleNames []string `json:"middleNames,omitempty"`
}

var globalCollection *Collection
var globalCluster *Cluster

func TestMain(m *testing.M) {
	SetLogger(VerboseStdioLogger())
	auth := PasswordAuthenticator{
		Username: "Administrator",
		Password: "password",
	}

	var err error
	globalCluster, err = Connect("couchbase://10.112.192.101", auth)
	if err != nil {
		panic("Failed to connect to cluster: " + err.Error())
	}

	opts := &BucketOptions{UseMutationTokens: true}
	bucket, err := globalCluster.Bucket("test", opts)
	if err != nil {
		panic("Failed to open bucket: " + err.Error())
	}

	globalCollection = bucket.DefaultCollection()
	globalCollection.SetKvTimeout(2 * time.Minute)

	os.Exit(m.Run())
}

func TestScenarioA(t *testing.T) {
	val := details{
		Name: "Barry",
		Id:   32,
	}

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second) // This for the entire operations set for the scenario
	res, err := globalCollection.Upsert("scenarioa", val, &UpsertOptions{
		Context:  ctx,
		Encode:   DefaultEncode, // if missing DefaultEncode will be used anyways...
		ExpireAt: time.Now().Add(1 * time.Minute),
		Timeout:  30000 * time.Millisecond,
		// ParentSpanContext: mytrace.Context(),
	})
	if err != nil {
		if IsTimeoutError(err) {
			t.Fatalf("Failed to fetch key, timeout: %s", err)
		}
		t.Fatalf("Failed to upsert key: %s", err)
	}
	fmt.Printf("Upsert result: %+v\n", res)

	doc, err := globalCollection.Get("scenarioa", &GetOptions{WithExpiry: true, Context: ctx})
	if err != nil {
		t.Fatalf("Failed to fetch key: %s", err)
	}
	fmt.Printf("Get result: %+v\n", doc)

	if !doc.withExpiry || doc.expireAt == 0 {
		t.Fatalf("Doc has no expiry")
	}

	var retVal details
	err = doc.Content(&retVal)
	if err != nil {
		t.Fatalf("Failed to extract: %s", err)
	}
	fmt.Printf("Content result: %+v\n", retVal)

	err = doc.Decode(&retVal, DefaultDecode) //will default to use defaultdecode anyway
	if err != nil {
		t.Fatalf("Failed to extract: %s", err)
	}
	fmt.Printf("Decode result %+v\n", retVal)

	retVal.Name = "Harry"

	replaceRes, err := globalCollection.Replace("scenarioa", retVal, nil) // We don't set options so this also won't follow the context level timeout
	if err != nil {
		t.Fatalf("Failed to replace key: %s", err)
	}
	fmt.Printf("Replace result: %+v\n", replaceRes)
}

// 2018/11/30 14:10:18 Fetching Agent
// 2018/11/30 14:10:18 Transcoding
// Upsert result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587018526425088}
// Get result: &{id: value:<nil> bytes:[123 34 110 97 109 101 34 58 34 66 97 114 114 121 34 44 34 105 100 34 58 51 50 44 34 109 105 100 100 108 101 78 97 109 101 115 34 58 110 117 108 108 125] flags:0 cas:1543587018526425088 expireAt:1543587078}
// Content result: {Name:Barry Id:32 MiddleNames:[]}
// Decode result {Name:Barry Id:32 MiddleNames:[]}
// 2018/11/30 14:10:20 Fetching Agent
// 2018/11/30 14:10:20 Transcoding
// Replace result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587021014630400}

func TestScenarioB(t *testing.T) {
	val := details{
		Name:        "Barry",
		Id:          32,
		MiddleNames: []string{"Borry", "Larry"},
	}

	res, err := globalCollection.Upsert("scenariob", val, nil)
	if err != nil {
		t.Fatalf("Failed to upsert key: %s", err)
	}
	fmt.Printf("Upsert result: %+v\n", res)

	opts := GetOptions{Timeout: 500 * time.Millisecond}.Project("middleNames", "id")
	doc, err := globalCollection.Get("scenariob", &opts)
	if err != nil {
		t.Fatalf("Failed to extract: %s", err)
	}

	var projection details
	err = doc.Content(&projection)
	if err != nil {
		t.Fatalf("Failed to extract: %s", err)
	}

	fmt.Printf("Lookup result %+v\n", projection)

	projection.MiddleNames = append(projection.MiddleNames, "James")

	mutRes, err := globalCollection.Mutate("scenariob", MutateSpec{}.Replace("middleNames", projection.MiddleNames), &MutateOptions{})
	if err != nil {
		t.Fatalf("Failed to extract: %s", err)
	}
	fmt.Printf("Mutate result %+v\n", mutRes)
}

// 2018/11/30 14:11:16 Fetching Agent
// 2018/11/30 14:11:16 Transcoding
// Upsert result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587077156438016}
// Lookup result [Borry Larry]
// Mutate result &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587077158666240}

func TestScenarioC(t *testing.T) {
	// Would need a check to ensure both types of durability can't be set at once if we go this route
	globalCollection.Upsert("scenarioc", struct{}{}, &UpsertOptions{ReplicateTo: 2, PersistTo: 1})

	globalCollection.Upsert("scenarioc", struct{}{}, &UpsertOptions{WithDurability: DurabilityLevelMajorityAndPersistActive})
}

func TestScenarioD(t *testing.T) {
	_, err := globalCollection.Upsert("scenariod", details{}, nil)
	if err != nil {
		t.Fatalf("Failed to upsert key: %s", err)
	}

	for {
		doc, err := globalCollection.Get("scenariod", nil)
		if err != nil {
			t.Fatalf("Failed to fetch key: %s", err)
		}

		var thing details
		err = doc.Content(&thing)
		if err != nil {
			t.Fatalf("Failed to fetch key: %s", err)
		}
		thing.Name = "Barry"

		_, err = globalCollection.Replace("scenariod", thing, &ReplaceOptions{Cas: doc.Cas()})
		if err != nil {
			if IsCasMismatch(err) {
				fmt.Println(err.Error()) //could not perform replace: key already exists, or CAS mismatch (KEY_EEXISTS)
				continue
			} else {
				panic(err)
			}
		}
	}
}

type mockClient struct {
	bucketName        string
	useMutationTokens bool
	collectionId      uint32
	scopeId           uint32
}

type mockKvOperator struct {
	opWait time.Duration
}

type mockPendingOp struct {
}

func (mpo *mockPendingOp) Cancel() bool {
	return true
}

func (mko *mockKvOperator) AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		cb(&gocbcore.StoreResult{
			Cas:           gocbcore.Cas(0),
			MutationToken: gocbcore.MutationToken{},
		}, nil)
	})
	return &mockPendingOp{}, nil
}

func (mko *mockKvOperator) SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	cb(&gocbcore.StoreResult{}, nil)
	return &mockPendingOp{}, nil

}

func (mko *mockKvOperator) ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	cb(&gocbcore.StoreResult{}, nil)
	return &mockPendingOp{}, nil

}

func (mko *mockKvOperator) GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		cb(&gocbcore.GetResult{
			Cas:      gocbcore.Cas(0),
			Flags:    0,
			Datatype: uint8(gocbcore.DatatypeFlagJson),
			Value:    []byte{},
		}, nil)
	})
	return &mockPendingOp{}, nil

}

func (mko *mockKvOperator) DeleteEx(opts gocbcore.DeleteOptions, cb gocbcore.DeleteExCallback) (gocbcore.PendingOp, error) {
	cb(&gocbcore.DeleteResult{}, nil)
	return &mockPendingOp{}, nil

}

func (mko *mockKvOperator) LookupInEx(opts gocbcore.LookupInOptions, cb gocbcore.LookupInExCallback) (gocbcore.PendingOp, error) {
	cb(&gocbcore.LookupInResult{}, nil)
	return &mockPendingOp{}, nil

}

func (mko *mockKvOperator) MutateInEx(opts gocbcore.MutateInOptions, cb gocbcore.MutateInExCallback) (gocbcore.PendingOp, error) {
	cb(&gocbcore.MutateInResult{}, nil)
	return &mockPendingOp{}, nil

}

func (mc *mockClient) Hash() string {
	return fmt.Sprintf("%s-%t",
		mc.bucketName,
		mc.useMutationTokens)
}

func (mc *mockClient) connect() error {
	return nil
}

func (mc *mockClient) fetchCollectionManifest() (bytesOut []byte, errOut error) {
	return []byte{}, nil
}

func (mc *mockClient) fetchCollectionID(scopeName string, collectionName string) (uint32, uint32, error) {
	return mc.scopeId, mc.collectionId, nil
}

func (mc *mockClient) getKvProvider() (kvProvider, error) {
	return &mockKvOperator{}, nil
}

func (mc *mockClient) getQueryProvider() (queryProvider, error) {
	return nil, nil
}

func TestMockingClient(t *testing.T) {
	clients := make(map[string]client)
	clients["mock-false"] = &mockClient{
		bucketName:        "mock",
		collectionId:      0,
		scopeId:           0,
		useMutationTokens: false,
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

	col := b.DefaultCollection()
	col.Get("key", &GetOptions{})

}
