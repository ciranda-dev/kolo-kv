package kolokv

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func newStore(t testing.TB) *kolostore.Store {
	t.Helper()
	s := kolostore.New(kolostore.NewMemory())
	t.Cleanup(func() { s.Close() })
	return s
}

func newIdentity(t testing.TB) *koloid.Identity {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// newDB returns a writer on a new log of a new in-memory store.
func newDB(t testing.TB) (*DB, *kolostore.Store, *koloid.Identity) {
	t.Helper()
	s := newStore(t)
	id := newIdentity(t)
	l, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, s, id
}

func reopen(t testing.TB, db *DB, s *kolostore.Store, id *koloid.Identity) *DB {
	t.Helper()
	logID := db.log.ID()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := s.Open(id, logID)
	if err != nil {
		t.Fatal(err)
	}
	db, err = Open(l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// newReplica opens an empty replica of db's log in a store of its own.
func newReplica(t testing.TB, db *DB) *kolostore.Replica {
	t.Helper()
	hdr := db.log.Header()
	r, err := newStore(t).OpenReplica(hdr.LogKey, hdr.LogID, hdr.Encode())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// grow sends r the writer's new entries through Replica.Update, the path kolo-sync uses.
func grow(t testing.TB, r *kolostore.Replica, l *kolostore.Log) {
	t.Helper()
	h, err := l.Head()
	if err != nil {
		t.Fatal(err)
	}
	p, err := l.ConsistencyProof(r.Len(), h.Size)
	if err != nil {
		t.Fatal(err)
	}
	var entries []kololog.Entry
	for i := r.Len(); i < h.Size; i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	if err := r.Update(h, p, entries); err != nil {
		t.Fatal(err)
	}
}

func applyMarkers(t testing.TB, r *kolostore.Replica, l *kolostore.Log) {
	t.Helper()
	for i := uint64(0); i < r.Len(); i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		if e.Marker != nil {
			if err := r.ApplyDelete(e.Marker); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func put(t testing.TB, db *DB, key, value string) {
	t.Helper()
	if err := db.Put(context.Background(), []byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func del(t testing.TB, db *DB, key string) {
	t.Helper()
	if err := db.Delete(context.Background(), []byte(key)); err != nil {
		t.Fatal(err)
	}
}

type getter interface {
	Get(key []byte) ([]byte, error)
}

func wantValue(t testing.TB, g getter, key, value string) {
	t.Helper()
	got, err := g.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, []byte(value)) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, value)
	}
}

func wantMissing(t testing.TB, g getter, key string) {
	t.Helper()
	if _, err := g.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) error = %v, want ErrNotFound", key, err)
	}
}

// pairs drains it into "key=value" strings and closes it.
func pairs(t testing.TB, it *Iterator) []string {
	t.Helper()
	var out []string
	for it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func keys(t testing.TB, it *Iterator) []string {
	t.Helper()
	var out []string
	for it.Next() {
		out = append(out, string(it.Key()))
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

// faultBackend wraps a memory backend: with corrupt set, Get flips a byte of every value it returns;
// with failAfter > 0, the commit that brings the count of commits to failAfter fails.
type faultBackend struct {
	kolostore.Backend
	mu        sync.Mutex
	corrupt   bool
	commits   int
	failAfter int
}

var errDisk = errors.New("disk failed")

func newFaultBackend() *faultBackend { return &faultBackend{Backend: kolostore.NewMemory()} }

func (b *faultBackend) set(f func(b *faultBackend)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f(b)
}

func (b *faultBackend) Get(key []byte) ([]byte, error) {
	v, err := b.Backend.Get(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil && b.corrupt && len(v) > 0 {
		v[len(v)-1] ^= 1
	}
	return v, err
}

func (b *faultBackend) NewBatch() kolostore.Batch { return &faultBatch{b.Backend.NewBatch(), b} }

type faultBatch struct {
	kolostore.Batch
	b *faultBackend
}

func (t *faultBatch) Commit() error {
	t.b.mu.Lock()
	t.b.commits++
	fail := t.b.failAfter > 0 && t.b.commits == t.b.failAfter
	t.b.mu.Unlock()
	if fail {
		return errDisk
	}
	return t.Batch.Commit()
}
