package kolokv

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"slices"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestReaderSeesWriterState(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Len() != 0 || len(keys(t, reader.Range(nil, nil))) != 0 {
		t.Fatal("reader of an empty replica is not empty")
	}

	put(t, db, "a", "1")
	put(t, db, "b", "2")
	put(t, db, "a", "3")
	grow(t, r, db.log)
	wantMissing(t, reader, "a")
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	if reader.Len() != db.Len() {
		t.Fatalf("reader Len %d, writer %d", reader.Len(), db.Len())
	}
	want := pairs(t, db.Range(nil, nil))
	if got := pairs(t, reader.Range(nil, nil)); !slices.Equal(got, want) {
		t.Fatalf("reader %q, writer %q", got, want)
	}

	put(t, db, "c", "4")
	grow(t, r, db.log)
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantValue(t, reader, "c", "4")
	if got := keys(t, reader.Prefix([]byte("b"))); !slices.Equal(got, []string{"b"}) {
		t.Fatalf("reader prefix %q", got)
	}
}

func TestReaderDeleteAfterMarker(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "v1")
	put(t, db, "k", "v2")
	put(t, db, "j", "x")
	grow(t, r, db.log)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	wantValue(t, reader, "k", "v2")
	snap, err := reader.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	del(t, db, "k")
	applyMarkers(t, r, db.log)
	// The markers alone destroy the values, before the delete op arrives.
	wantMissing(t, reader, "k")
	wantMissing(t, snap, "k")
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantMissing(t, reader, "k")

	grow(t, r, db.log)
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantMissing(t, reader, "k")
	wantValue(t, reader, "j", "x")
	if reader.Len() != db.Len() {
		t.Fatalf("reader Len %d, writer %d", reader.Len(), db.Len())
	}
}

func TestReaderDeleteOpBeforeMarker(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "v1")
	grow(t, r, db.log)
	del(t, db, "k")
	grow(t, r, db.log)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	wantMissing(t, reader, "k")
	if _, err := r.Get(0); err != nil {
		t.Fatalf("value before its marker: %v", err)
	}
	applyMarkers(t, r, db.log)
	if _, err := r.Get(0); !errors.Is(err, kololog.ErrDeleted) {
		t.Fatalf("value after its marker: %v", err)
	}
}

func TestReaderIsReadOnly(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	r := newReplica(t, db)
	grow(t, r, db.log)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.Put(context.Background(), []byte("a"), []byte("2")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Put on a reader: %v", err)
	}
	if err := reader.Delete(context.Background(), []byte("a")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Delete on a reader: %v", err)
	}
	wantValue(t, reader, "a", "1")
	if r.Len() != 1 {
		t.Fatalf("replica grew to %d", r.Len())
	}
}

// growWithout grows r to the writer's head like grow, without the entry keys of the listed entries.
func growWithout(t *testing.T, r *kolostore.Replica, l *kolostore.Log, nokey ...uint64) {
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
		if slices.Contains(nokey, i) {
			e.Key = nil
		}
		entries = append(entries, e)
	}
	if err := r.Update(h, p, entries); err != nil {
		t.Fatal(err)
	}
}

func openReader(t *testing.T, r *kolostore.Replica) *DB {
	t.Helper()
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return reader
}

func wantLen(t *testing.T, db *DB, n uint64) {
	t.Helper()
	if db.Len() != n {
		t.Fatalf("Len = %d, want %d", db.Len(), n)
	}
}

func TestReaderStopsAtEntryWithoutKey(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	put(t, db, "b", "2")
	put(t, db, "c", "3")
	r := newReplica(t, db)
	growWithout(t, r, db.log, 1)
	reader := openReader(t, r)
	wantLen(t, reader, 1)
	wantValue(t, reader, "a", "1")
	wantMissing(t, reader, "b")
	wantMissing(t, reader, "c")
}

func TestReaderKeylessDeleteOp(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "secret")
	put(t, db, "j", "x")
	grow(t, r, db.log)
	del(t, db, "k")
	put(t, db, "j", "y")
	growWithout(t, r, db.log, 2)
	reader := openReader(t, r)
	// The state stops before the delete op it cannot read: the writer's state at length 2, not a mix.
	wantLen(t, reader, 2)
	wantValue(t, reader, "j", "x")
	applyMarkers(t, r, db.log)
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantLen(t, reader, 0)
	wantMissing(t, reader, "k")
}

func TestReaderKeylessDeleteOpAfterCrash(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "v1")
	if _, err := db.log.Append(encodeOp(op{del: true, key: []byte("k"), list: []uint64{0}})); err != nil {
		t.Fatal(err)
	}
	growWithout(t, r, db.log, 1)
	reader := openReader(t, r)
	wantLen(t, reader, 1)
}

func TestReaderKeylessOverwrite(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "old")
	grow(t, r, db.log)
	put(t, db, "k", "new")
	growWithout(t, r, db.log, 1)
	reader := openReader(t, r)
	wantLen(t, reader, 1)
	wantValue(t, reader, "k", "old")
}

// A sync that reads the writer's entries one by one while it deletes can bring the marker of the newer value only.
func TestReaderMarkerBeforeDeleteOp(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	put(t, db, "k", "v1")
	put(t, db, "k", "v2")
	put(t, db, "j", "x")
	grow(t, r, db.log)
	reader := openReader(t, r)
	wantValue(t, reader, "k", "v2")
	del(t, db, "k")
	e1, err := db.log.Entry(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyDelete(e1.Marker); err != nil {
		t.Fatal(err)
	}
	wantMissing(t, reader, "k")
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	// Entry 1 is destroyed and the delete op that explains it is not held: the state stops before entry 1.
	wantLen(t, reader, 1)
	wantMissing(t, reader, "j")
	grow(t, r, db.log)
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantLen(t, reader, db.Len())
	wantMissing(t, reader, "k")
	wantValue(t, reader, "j", "x")
}

func TestReplayRules(t *testing.T) {
	put := func(k string) []byte { return encodeOp(op{key: []byte(k), value: []byte("v")}) }
	del := func(k string, list ...uint64) []byte { return encodeOp(op{del: true, key: []byte(k), list: list}) }
	gone := []byte("gone")
	nokey := []byte("nokey")
	tests := []struct {
		name    string
		entries [][]byte
		reader  bool
		n       uint64
		keys    []string
		err     error
	}{
		{"explained destroyed puts", [][]byte{gone, put("j"), gone, del("k", 0, 2)}, false, 4, []string{"j"}, nil},
		{"leftover put listed", [][]byte{put("k"), gone, del("k", 0, 1)}, false, 3, nil, nil},
		{"unexplained on the writer", [][]byte{put("a"), gone}, false, 0, nil, ErrCorrupt},
		{"unexplained on a reader", [][]byte{put("a"), gone, put("b")}, true, 1, []string{"a"}, nil},
		{"explained after the prefix", [][]byte{put("a"), gone, nokey, del("k", 1)}, true, 1, []string{"a"}, nil},
		{"explainer after an unexplained entry", [][]byte{put("a"), gone, put("b"), gone, del("k", 1)}, true, 1, []string{"a"}, nil},
		{"no key on the writer", [][]byte{put("a"), nokey}, false, 0, nil, ErrCorrupt},
		{"no key on a reader", [][]byte{put("a"), nokey, put("b")}, true, 1, []string{"a"}, nil},
		{"list names another key", [][]byte{put("a"), del("k", 0)}, true, 0, nil, ErrCorrupt},
		{"list names a delete op", [][]byte{put("k"), del("k", 0), del("k", 1)}, true, 0, nil, ErrCorrupt},
		{"list names an entry twice", [][]byte{gone, del("k", 0), del("k", 0)}, true, 0, nil, ErrCorrupt},
		{"list names a later entry", [][]byte{del("k", 1), put("k")}, true, 0, nil, ErrCorrupt},
	}
	for _, tc := range tests {
		get := func(i uint64) ([]byte, error) {
			switch string(tc.entries[i]) {
			case "gone":
				return nil, kololog.ErrDeleted
			case "nokey":
				return nil, kololog.ErrKeyUnavailable
			}
			return tc.entries[i], nil
		}
		st, err := replay(uint64(len(tc.entries)), get, tc.reader)
		if tc.err != nil {
			if !errors.Is(err, tc.err) {
				t.Errorf("%s: %v, want %v", tc.name, err, tc.err)
			}
			continue
		}
		var keys []string
		for _, x := range st.index {
			keys = append(keys, x.key)
		}
		if err != nil || st.n != tc.n || !slices.Equal(keys, tc.keys) {
			t.Errorf("%s: n %d keys %q err %v, want n %d keys %q", tc.name, st.n, keys, err, tc.n, tc.keys)
		}
	}
}

func TestReplayErase(t *testing.T) {
	entries := [][]byte{
		encodeOp(op{key: []byte("k"), value: []byte("1")}),
		encodeOp(op{key: []byte("k"), value: []byte("2")}),
		encodeOp(op{del: true, key: []byte("k"), list: []uint64{0, 1}}),
	}
	get := func(i uint64) ([]byte, error) {
		if i == 0 {
			return nil, kololog.ErrDeleted
		}
		return entries[i], nil
	}
	st, err := replay(3, get, false)
	if err != nil || !slices.Equal(st.erase, []uint64{1}) || len(st.index) != 0 {
		t.Fatalf("erase %v index %v err %v", st.erase, st.index, err)
	}
}

func TestReplayPassesStorageErrorsThrough(t *testing.T) {
	ok := encodeOp(op{key: []byte("a"), value: []byte("1")})
	for _, storage := range []error{kolostore.ErrClosed, errors.New("disk failed")} {
		get := func(i uint64) ([]byte, error) {
			if i == 1 {
				return nil, storage
			}
			return ok, nil
		}
		for _, reader := range []bool{false, true} {
			_, err := replay(2, get, reader)
			if !errors.Is(err, storage) || errors.Is(err, ErrCorrupt) {
				t.Errorf("reader %v: %v, want %v and not ErrCorrupt", reader, err, storage)
			}
		}
	}
	for _, bad := range []error{kololog.ErrMalformedEntry, kololog.ErrMalformed, kololog.ErrBadEntry, kololog.ErrStorageMismatch, kolostore.ErrCorrupt} {
		get := func(uint64) ([]byte, error) { return nil, bad }
		if _, err := replay(1, get, true); !errors.Is(err, ErrCorrupt) || !errors.Is(err, bad) {
			t.Errorf("%v: %v, want ErrCorrupt wrapping it", bad, err)
		}
	}
}

func TestReaderChunkedCatchUp(t *testing.T) {
	db, _, _ := newDB(t)
	r := newReplica(t, db)
	var h kololog.Head
	chunk := func(from, to uint64) {
		t.Helper()
		p, err := db.log.ConsistencyProof(to, h.Size)
		if err != nil {
			t.Fatal(err)
		}
		var entries []kololog.Entry
		for i := from; i < to; i++ {
			e, err := db.log.Entry(i)
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, e)
		}
		if err := r.Update(h, p, entries); err != nil {
			t.Fatal(err)
		}
	}
	head := func() {
		t.Helper()
		var err error
		if h, err = db.log.Head(); err != nil {
			t.Fatal(err)
		}
	}
	wantKeys := func(reader *DB, n uint64, want ...string) {
		t.Helper()
		if err := reader.Refresh(); err != nil {
			t.Fatal(err)
		}
		if got := keys(t, reader.Range(nil, nil)); !slices.Equal(got, want) || reader.Len() != n {
			t.Fatalf("keys %q, Len %d; want %q, %d", got, reader.Len(), want, n)
		}
	}

	put(t, db, "a", "a")
	put(t, db, "b", "b")
	put(t, db, "c", "c")
	head()
	chunk(0, 2)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	wantKeys(reader, 0)
	chunk(2, 3)
	wantKeys(reader, 3, "a", "b", "c")

	put(t, db, "d", "d")
	put(t, db, "e", "e")
	head()
	chunk(3, 4)
	wantKeys(reader, 3, "a", "b", "c")
	chunk(4, 5)
	wantKeys(reader, 5, "a", "b", "c", "d", "e")
}

func TestReaderKeyArrivesLater(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "k", "old")
	put(t, db, "k", "new")
	r := newReplica(t, db)
	h, err := db.log.Head()
	if err != nil {
		t.Fatal(err)
	}
	p, err := db.log.ConsistencyProof(0, h.Size)
	if err != nil {
		t.Fatal(err)
	}
	var entries []kololog.Entry
	var key []byte
	for i := range h.Size {
		e, err := db.log.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			key, e.Key = e.Key, nil
		}
		entries = append(entries, e)
	}
	if err := r.Update(h, p, entries); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	// Without the key of entry 1 the state stops at length 1, where k was old.
	wantLen(t, reader, 1)
	wantValue(t, reader, "k", "old")
	if err := r.AddKey(1, key); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantLen(t, reader, 2)
	wantValue(t, reader, "k", "new")
}

// Entry 1 is listed by a later delete op (5) than entry 2 (3); without entry 6 the prefix must stop at 1.
func TestReplayCrossingDeletes(t *testing.T) {
	entries := [][]byte{
		encodeOp(op{key: []byte("a"), value: []byte("old")}),
		nil, // put a=new, listed by entry 5
		nil, // put b, listed by entry 3
		encodeOp(op{del: true, key: []byte("b"), list: []uint64{2}}),
		nil, // put c, listed by an entry the reader does not hold
		encodeOp(op{del: true, key: []byte("a"), list: []uint64{0, 1}}),
	}
	get := func(i uint64) ([]byte, error) {
		if entries[i] == nil {
			return nil, kololog.ErrDeleted
		}
		return entries[i], nil
	}
	st, err := replay(uint64(len(entries)), get, true)
	if err != nil {
		t.Fatal(err)
	}
	if st.n != 1 || len(st.index) != 1 || st.index[0].at != 0 {
		t.Fatalf("n %d index %v, want n 1 with a at entry 0", st.n, st.index)
	}
}

// heapGrowth replays n entries from get and returns how much the live heap grew by the last read.
func heapGrowth(t *testing.T, n uint64, get func(uint64) ([]byte, error)) int64 {
	t.Helper()
	var before runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var growth int64
	_, err := replay(n, func(i uint64) ([]byte, error) {
		if i == n-1 {
			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			growth = int64(m.HeapAlloc) - int64(before.HeapAlloc)
		}
		return get(i)
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	return growth
}

func TestReplayDoesNotKeepOverwrittenKeys(t *testing.T) {
	const n, keySize = 400, 16 << 10
	put := encodeOp(op{key: bytes.Repeat([]byte("k"), keySize), value: []byte("v")})
	growth := heapGrowth(t, n, func(uint64) ([]byte, error) { return put, nil })
	if growth > 1<<20 {
		t.Fatalf("replay of %d puts of one %d byte key held %d bytes, want under 1 MiB", n, keySize, growth)
	}
}

func TestReplayDoesNotKeepDeletedKeys(t *testing.T) {
	const pairs, keySize = 200, 16 << 10
	growth := heapGrowth(t, 2*pairs, func(i uint64) ([]byte, error) {
		if i%2 == 0 {
			return nil, kololog.ErrDeleted
		}
		key := bytes.Repeat([]byte("k"), keySize)
		binary.BigEndian.PutUint64(key, i)
		return encodeOp(op{del: true, key: key, list: []uint64{i - 1}}), nil
	})
	if growth > 1<<20 {
		t.Fatalf("replay of %d delete entries with %d byte keys held %d bytes, want under 1 MiB", pairs, keySize, growth)
	}
}
