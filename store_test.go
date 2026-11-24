package kolokv_test

import (
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kolokv "github.com/ciranda-dev/kolo-kv"
	"github.com/ciranda-dev/kolo-kv/kolokvtest"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestStore(t *testing.T) {
	open := func(t *testing.T) kolokv.Store {
		s := kolostore.New(kolostore.NewMemory())
		t.Cleanup(func() { s.Close() })
		id, err := koloid.Generate()
		if err != nil {
			t.Fatal(err)
		}
		l, err := s.Create(id)
		if err != nil {
			t.Fatal(err)
		}
		db, err := kolokv.Open(l)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	kolokvtest.TestStore(t, open, func(*testing.T, kolokv.Store) {})
}
