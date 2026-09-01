package state

import (
	"path/filepath"
	"testing"
)

func TestCheckpointAndLegacyBucketsPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Bind("endpoint\x00feed\x00account"); err != nil {
		t.Fatal(err)
	}
	if err := db.Bind("endpoint\x00feed\x00account"); err != nil {
		t.Fatal(err)
	}
	if err := db.Bind("other"); err == nil {
		t.Fatal("scope mismatch accepted")
	}
	if err := db.MarkDone("1", "created"); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkLegacy("2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.HasDone("1") || !db.HasLegacy("2") {
		t.Fatal("persisted keys missing")
	}
	if err := db.ClearLegacy(); err != nil {
		t.Fatal(err)
	}
	if db.HasLegacy("2") || !db.HasDone("1") {
		t.Fatal("bucket isolation failed")
	}
}
