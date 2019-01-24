// +build integration

package gocb

import (
	"reflect"
	"testing"
	"time"
)

func TestErrorNonExistant(t *testing.T) {
	res, err := globalCollection.Get("doesnt-exist", nil)
	if err == nil {
		t.Fatalf("Expected error to be non-nil")
	}

	if res != nil {
		t.Fatalf("Expected result to be nil but was %v", res)
	}
}

func TestErrorDoubleInsert(t *testing.T) {
	_, err := globalCollection.Insert("doubleInsert", "test", nil)
	if err != nil {
		t.Fatalf("Expected error to be nil but was %v", err)
	}
	_, err = globalCollection.Insert("doubleInsert", "test", nil)
	if err == nil {
		t.Fatalf("Expected error to be non-nil")
	}

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
	}
}

func TestInsertGet(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Insert("insertDoc", doc, nil)
	if err != nil {
		t.Fatalf("Insert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Insert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("insertDoc", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestUpsertGetRemove(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	upsertedDoc, err := globalCollection.Get("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var upsertedDocContent testBeerDocument
	err = upsertedDoc.Content(&upsertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != upsertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, upsertedDocContent)
	}

	existsRes, err := globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Remove failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Remove CAS was 0")
	}

	existsRes, err = globalCollection.Exists("upsertDoc", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		t.Fatalf("Expected exists to return false")
	}
}

func TestUpsertGetProject(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("projectDoc", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	projectedDoc, err := globalCollection.Get("projectDoc", &GetOptions{
		Project: []string{"brewery_id", "style"},
	})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	expectedDoc := testBeerDocument{
		BreweryID: doc.BreweryID,
		Style:     doc.Style,
	}

	var projectedDocContent testBeerDocument
	err = projectedDoc.Content(&projectedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if expectedDoc != projectedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, projectedDocContent)
	}
}

func TestRemoveWithCas(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("removeWithCas", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	existsRes, err := globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas() + 0xFECA})
	if err == nil {
		t.Fatalf("Expected remove to fail")
	}

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if !existsRes.Exists() {
		t.Fatalf("Expected exists to return true")
	}

	_, err = globalCollection.Remove("removeWithCas", &RemoveOptions{Cas: mutRes.Cas()})
	if err != nil {
		t.Fatalf("Remove failed, error was %v", err)
	}

	existsRes, err = globalCollection.Exists("removeWithCas", nil)
	if err != nil {
		t.Fatalf("Exists failed, error was %v", err)
	}

	if existsRes.Exists() {
		t.Fatalf("Expected exists to return false")
	}
}

func TestUpsertAndReplace(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("upsertAndReplace", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	insertedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var insertedDocContent testBeerDocument
	err = insertedDoc.Content(&insertedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != insertedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}

	doc.Name = "replaced"
	mutRes, err = globalCollection.Replace("upsertAndReplace", doc, &ReplaceOptions{Cas: mutRes.Cas()})
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	replacedDoc, err := globalCollection.Get("upsertAndReplace", nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var replacedDocContent testBeerDocument
	err = replacedDoc.Content(&replacedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != replacedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, insertedDocContent)
	}
}

func TestGetAndTouch(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndTouch", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndTouch("getAndTouch", 10, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	expireDoc, err := globalCollection.Get("getAndTouch", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	if !expireDoc.HasExpiration() {
		t.Fatalf("Expected doc to have an expiry")
	}

	if expireDoc.Expiration() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiration())
	}

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}
}

func TestGetAndLock(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("getAndLock", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	mutRes, err = globalCollection.Upsert("getAndLock", doc, nil)
	if err == nil {
		t.Fatalf("Expected error but was nil")
	}

	if !IsKeyExistsError(err) {
		t.Fatalf("Expected error to be KeyExistsError but is %s", reflect.TypeOf(err).String())
	}

	globalCluster.TimeTravel(2000 * time.Millisecond)

	mutRes, err = globalCollection.Upsert("getAndLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}
}

func TestUnlock(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlock", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.Unlock("unlock", &UnlockOptions{Cas: lockedDoc.Cas()})
	if err != nil {
		t.Fatalf("Unlock failed, error was %v", err)
	}

	mutRes, err = globalCollection.Upsert("unlock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}
}

func TestUnlockInvalidCas(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("unlockInvalidCas", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("unlockInvalidCas", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.Unlock("unlockInvalidCas", &UnlockOptions{Cas: lockedDoc.Cas() + 1})
	if err == nil {
		t.Fatalf("Unlock should have failed")
	}

	if !IsTempFailError(err) {
		t.Fatalf("Expected error to be TempFailError but was %s", reflect.TypeOf(err).String())
	}
}

func TestDoubleLockFail(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("doubleLock", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndLock("doubleLock", 1, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	_, err = globalCollection.GetAndLock("doubleLock", 1, nil)
	if err == nil {
		t.Fatalf("Expected GetAndLock to fail")
	}

	if !IsTempFailError(err) {
		t.Fatalf("Expected error to be TempFailError but was %v", err)
	}
}

func TestUnlockMissingDocFail(t *testing.T) {
	_, err := globalCollection.Unlock("unlockMissing", &UnlockOptions{Cas: 123})
	if err == nil {
		t.Fatalf("Expected Unlock to fail")
	}

	if !IsKeyNotFoundError(err) {
		t.Fatalf("Expected error to be KeyNotFoundError but was %v", err)
	}
}

func TestTouch(t *testing.T) {
	var doc testBeerDocument
	err := loadJSONTestDataset("beer_sample_single", &doc)
	if err != nil {
		t.Fatalf("Could not read test dataset: %v", err)
	}

	mutRes, err := globalCollection.Upsert("touch", doc, nil)
	if err != nil {
		t.Fatalf("Upsert failed, error was %v", err)
	}

	if mutRes.Cas() == 0 {
		t.Fatalf("Upsert CAS was 0")
	}

	lockedDoc, err := globalCollection.GetAndTouch("touch", 2, nil)
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	var lockedDocContent testBeerDocument
	err = lockedDoc.Content(&lockedDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != lockedDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}

	globalCluster.TimeTravel(1 * time.Second)

	_, err = globalCollection.Touch("touch", 3, nil)
	if err != nil {
		t.Fatalf("Touch failed, error was %v", err)
	}

	globalCluster.TimeTravel(2 * time.Second)

	expireDoc, err := globalCollection.Get("touch", &GetOptions{WithExpiry: true})
	if err != nil {
		t.Fatalf("Get failed, error was %v", err)
	}

	if !expireDoc.HasExpiration() {
		t.Fatalf("Expected doc to have an expiry")
	}

	if expireDoc.Expiration() == 0 {
		t.Fatalf("Expected doc to have an expiry > 0, was %d", expireDoc.Expiration())
	}

	var expireDocContent testBeerDocument
	err = expireDoc.Content(&expireDocContent)
	if err != nil {
		t.Fatalf("Content failed, error was %v", err)
	}

	if doc != expireDocContent {
		t.Fatalf("Expected resulting doc to be %v but was %v", doc, lockedDocContent)
	}
}

func TestTouchMissingDocFail(t *testing.T) {
	_, err := globalCollection.Touch("touchMissing", 3, nil)
	if err == nil {
		t.Fatalf("Touch should have failed")
	}

	if !IsKeyNotFoundError(err) {
		t.Fatalf("Expected error to be KeyNotFoundError but was %v", err)
	}
}
