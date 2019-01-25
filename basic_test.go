// +build integration

package gocb

type details struct {
	Name        string   `json:"name,omitempty"`
	Id          int      `json:"id,omitempty"`
	MiddleNames []string `json:"middleNames,omitempty"`
}

// func TestScenarioA(t *testing.T) {
// 	val := details{
// 		Name: "Barry",
// 		Id:   32,
// 	}
//
// 	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second) // This for the entire operations set for the scenario
// 	res, err := globalCollection.Upsert("scenarioa", val, &UpsertOptions{
// 		Context: ctx,
// 		// Encode:     DefaultEncode, // if missing DefaultEncode will be used anyways...
// 		Expiration: 60,
// 		Timeout:    300 * time.Millisecond,
// 		// ParentSpanContext: mytrace.Context(),
// 	})
// 	if err != nil {
// 		if IsTimeoutError(err) {
// 			if ctx.Err() != nil {
// 				// probably bail here
// 			}
// 			// maybe retry here
// 		} else {
// 		}
//
// 		t.Fatalf("Failed to upsert key: %s", err)
// 	}
// 	fmt.Printf("Upsert result: %+v\n", res)
//
// 	doc, err := globalCollection.Get("scenarioa", &GetOptions{WithExpiry: true, Context: ctx})
// 	if err != nil {
// 		t.Fatalf("Failed to fetch key: %s", err)
// 	}
// 	fmt.Printf("Get result: %+v\n", doc)
//
// 	if !doc.withExpiration || doc.expiration == 0 {
// 		t.Fatalf("Doc has no expiry")
// 	}
//
// 	var retVal details
// 	err = doc.Content(&retVal)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
// 	fmt.Printf("Content result: %+v\n", retVal)
//
// 	// err = doc.Decode(&retVal, DefaultDecode) // will default to use defaultdecode anyway
// 	// if err != nil {
// 	// 	t.Fatalf("Failed to extract: %s", err)
// 	// }
// 	// fmt.Printf("Decode result %+v\n", retVal)
//
// 	retVal.Name = "Harry"
//
// 	replaceRes, err := globalCollection.Replace("scenarioa", retVal, nil) // We don't set options so this also won't follow the context level timeout
// 	if err != nil {
// 		t.Fatalf("Failed to replace key: %s", err)
// 	}
// 	fmt.Printf("Replace result: %+v\n", replaceRes)
// }
//
// // 2018/11/30 14:10:18 Fetching Agent
// // 2018/11/30 14:10:18 Transcoding
// // Upsert result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587018526425088}
// // Get result: &{id: value:<nil> bytes:[123 34 110 97 109 101 34 58 34 66 97 114 114 121 34 44 34 105 100 34 58 51 50 44 34 109 105 100 100 108 101 78 97 109 101 115 34 58 110 117 108 108 125] flags:0 cas:1543587018526425088 expiration:1543587078}
// // Content result: {Name:Barry Id:32 MiddleNames:[]}
// // Decode result {Name:Barry Id:32 MiddleNames:[]}
// // 2018/11/30 14:10:20 Fetching Agent
// // 2018/11/30 14:10:20 Transcoding
// // Replace result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587021014630400}
//
// func TestScenarioB(t *testing.T) {
// 	val := details{
// 		Name:        "Barry",
// 		Id:          32,
// 		MiddleNames: []string{"Borry", "Larry"},
// 	}
//
// 	res, err := globalCollection.Upsert("scenariob", val, &UpsertOptions{Expiration: 10})
// 	if err != nil {
// 		t.Fatalf("Failed to upsert key: %s", err)
// 	}
// 	fmt.Printf("Upsert result: %+v\n", res)
//
// 	opts := GetOptions{Timeout: 500 * time.Millisecond, Project: []string{"middleNames", "id"}}
// 	doc, err := globalCollection.Get("scenariob", &opts)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
//
// 	var projection details
// 	err = doc.Content(&projection)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
//
// 	fmt.Printf("Lookup result %+v\n", projection)
//
// 	lOpts := LookupInOptions{Timeout: 500 * time.Millisecond, WithExpiry: true}.Path("middleNames").Path("id")
// 	partial, err := globalCollection.LookupIn("scenariob", &lOpts)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
//
// 	var names []string
// 	err = partial.ContentAt(0, &names)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
//
// 	fmt.Printf("Lookup result %+v\n", names)
//
// 	if !partial.withExpiration || partial.expiration == 0 {
// 		t.Fatalf("Doc has no expiry")
// 	}
//
// 	projection.MiddleNames = append(projection.MiddleNames, "James")
// 	mutateOpts := MutateInOptions{}.Replace("middleNames", projection.MiddleNames)
//
// 	mutRes, err := globalCollection.Mutate("scenariob", &mutateOpts)
// 	if err != nil {
// 		t.Fatalf("Failed to extract: %s", err)
// 	}
// 	fmt.Printf("Mutate result %+v\n", mutRes)
// }

// 2018/11/30 14:11:16 Fetching Agent
// 2018/11/30 14:11:16 Transcoding
// Upsert result: &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587077156438016}
// Lookup result [Borry Larry]
// Mutate result &{mt:{token:{VbId:0 VbUuid:0 SeqNo:0} bucketName:test} cas:1543587077158666240}

// func TestScenarioC(t *testing.T) {
// 	// Would need a check to ensure both types of durability can't be set at once if we go this route
// 	globalCollection.Upsert("scenarioc", struct{}{}, &UpsertOptions{ReplicateTo: 2, PersistTo: 1})
//
// 	globalCollection.Upsert("scenarioc", struct{}{}, &UpsertOptions{DurabilityLevel: DurabilityLevelMajorityAndPersistActive})
// }

// func TestScenarioD(t *testing.T) {
// 	_, err := globalCollection.Upsert("scenariod", details{}, nil)
// 	if err != nil {
// 		t.Fatalf("Failed to upsert key: %s", err)
// 	}
//
// 	for {
// 		doc, err := globalCollection.Get("scenariod", nil)
// 		if err != nil {
// 			t.Fatalf("Failed to fetch key: %s", err)
// 		}
//
// 		var thing details
// 		err = doc.Content(&thing)
// 		if err != nil {
// 			t.Fatalf("Failed to fetch key: %s", err)
// 		}
// 		thing.Name = "Barry"
//
// 		_, err = globalCollection.Replace("scenariod", thing, &ReplaceOptions{Cas: doc.Cas()})
// 		if err != nil {
// 			if IsCasMismatchError(err) {
// 				fmt.Println(err.Error()) //could not perform replace: key already exists, or CAS mismatch (KEY_EEXISTS)
// 				continue
// 			} else {
// 				panic(err)
// 			}
// 		}
// 	}
// }
