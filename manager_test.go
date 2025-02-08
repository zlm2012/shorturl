package shorturl

import (
	"github.com/bwmarrin/snowflake"
	"os"
	"testing"
	"time"
)

func createBk(t *testing.T) *sqliteBackend {
	f, err := os.CreateTemp(t.TempDir(), "rdb-")
	if err != nil {
		t.Fatal("failed on creating file", err)
	}
	if err = os.Remove(f.Name()); err != nil {
		t.Fatal("failed on deleting temp file for name", err)
	}
	bk, err := SqliteOpen(f.Name(), true, 0)
	if err != nil {
		t.Fatal("failed on creating db", err)
	}
	return bk
}

func TestManager_General(t *testing.T) {
	bk := createBk(t)
	mgr, err := NewManager(bk)
	if err != nil {
		t.Fatal("failed to create manager.", err)
	}
	id, err := mgr.InsertOrReuse("https://test.mrzm.io/test1", -1)
	if err != nil {
		t.Fatal("failed on insert.", err)
	}
	t.Log("new ID:", id.Base58())
	newId, err := mgr.InsertOrReuse("https://test.mrzm.io/test1", -1)
	if err != nil {
		t.Fatal("failed on reinsert.", err)
	}
	if newId != id {
		t.Fatal("should reuse id")
	}

	expiredAt := time.Now().Add(2 * time.Second).Unix()
	sameButExpId, err := mgr.InsertOrReuse("https://test.mrzm.io/test1", expiredAt)
	if err != nil {
		t.Fatal("failed on insert.", err)
	}
	if sameButExpId == id {
		t.Fatal("should use new id")
	}

	expId, err := mgr.InsertOrReuse("https://test.mrzm.io/test2", expiredAt)
	if err != nil {
		t.Fatal("failed on insert exp", err)
	}
	t.Log("exp ID:", expId)
	newId, err = mgr.InsertOrReuse("https://test.mrzm.io/test2", expiredAt)
	if err != nil {
		t.Fatal("failed on reinsert exp.", err)
	}
	if newId != expId {
		t.Fatal("should be the same id (exp).")
	}
	newId, err = mgr.InsertOrReuse("https://test.mrzm.io/test2", expiredAt-1)
	if err != nil {
		t.Fatal("failed on insert exp.", err)
	}
	if newId == expId {
		t.Fatal("should use new id.")
	}

	ids := []snowflake.ID{id, sameButExpId, expId, newId}
	for _, idToCheck := range ids {
		entry, err := bk.QueryById(uint64(idToCheck))
		if err != nil {
			t.Fatal("failed on query.", err)
		}
		if entry == nil {
			t.Fatal("should exists.")
		}
	}

	time.Sleep(2 * time.Second)

	for _, idToCheck := range ids {
		entry, err := bk.QueryById(uint64(idToCheck))
		if err != nil {
			t.Fatal("failed on query.", err)
		}
		if idToCheck == id && entry == nil {
			t.Fatal("should exists.")
		} else if idToCheck != id && entry != nil {
			t.Fatal("should not hit")
		}
	}
	count, err := bk.count()
	if err != nil {
		t.Fatal("failed on count.", err)
	}
	if count != 4 {
		t.Fatal("should remains 4 rows.")
	}

	err = mgr.Clean()
	if err != nil {
		t.Fatal("failed on clean.", err)
	}
	count, err = bk.count()
	if err != nil {
		t.Fatal("failed on count.", err)
	}
	if count != 1 {
		t.Fatal("should remains 4 rows.")
	}
}
