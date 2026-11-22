package kolokv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// model is the writer's history as the test expects it.
type model struct {
	ents  []ment
	hist  []map[string]mval // hist[L] is the state after L entries
	puts  map[string][]uint64
	delBy map[uint64]uint64 // the delete op that lists each destroyed entry
}

type ment struct {
	del  bool
	key  string
	list []uint64
}

type mval struct {
	v  string
	at uint64
}

func (m *model) cur() map[string]mval { return m.hist[len(m.hist)-1] }

func (m *model) appendEnt(e ment, v string) {
	next := maps.Clone(m.cur())
	at := uint64(len(m.ents))
	if e.del {
		delete(next, e.key)
		for _, j := range e.list {
			m.delBy[j] = at
		}
		delete(m.puts, e.key)
	} else {
		next[e.key] = mval{v, at}
		m.puts[e.key] = append(m.puts[e.key], at)
	}
	m.ents = append(m.ents, e)
	m.hist = append(m.hist, next)
}

var mkeys = []string{"", "a", "ab", "b", "\xff", "\xff\xff", "a\x00"}

type snapCheck struct {
	s   *Snapshot
	L   uint64
	rep bool
}

// TestModel runs random writes, crashes, reopens, snapshots and out-of-order replication against a model.
// It runs 4 fixed seeds; KOLOKV_MODEL=2m runs random seeds for that long, KOLOKV_MODEL_SEED=n runs seed n.
func TestModel(t *testing.T) {
	defer func(n int) { maxPuts = n }(maxPuts)
	maxPuts = 3
	run := func(seed uint64) {
		if !t.Run(fmt.Sprint(seed), func(t *testing.T) { modelRun(t, seed) }) {
			t.Fatalf("seed %d failed", seed)
		}
	}
	if s := os.Getenv("KOLOKV_MODEL_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		run(seed)
		return
	}
	if s := os.Getenv("KOLOKV_MODEL"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatal(err)
		}
		runs := 0
		for seed, end := uint64(time.Now().UnixNano()), time.Now().Add(d); time.Now().Before(end); seed++ {
			run(seed)
			runs++
		}
		t.Logf("%d runs", runs)
		return
	}
	for seed := range uint64(4) {
		run(seed)
	}
}

func modelRun(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed*7+1))
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
	defer func() { db.Close() }()
	logID := l.ID()
	m := &model{hist: []map[string]mval{{}}, puts: map[string][]uint64{}, delBy: map[uint64]uint64{}}
	var orig []kololog.Entry // each entry as first written, key included
	var heads []kololog.Head
	record := func() {
		for i := uint64(len(orig)); i < db.log.Len(); i++ {
			e, err := db.log.Entry(i)
			if err != nil {
				t.Fatal(err)
			}
			orig = append(orig, e)
		}
		h, err := db.log.Head()
		if err != nil {
			t.Fatal(err)
		}
		heads = append(heads, h)
	}
	rs := newStore(t)
	hdr := db.log.Header()
	rep, err := rs.OpenReplica(hdr.LogKey, hdr.LogID, hdr.Encode())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(rep)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { reader.Close() }()
	var snaps []snapCheck

	checkWriter := func(where string) {
		t.Helper()
		if db.Len() != uint64(len(m.ents)) {
			t.Fatalf("%s: writer Len %d, model %d", where, db.Len(), len(m.ents))
		}
		want := m.cur()
		got := pairs(t, db.Range(nil, nil))
		var exp []string
		for _, k := range sortedKeys(want) {
			exp = append(exp, k+"="+want[k].v)
		}
		if !slices.Equal(got, exp) {
			t.Fatalf("%s: writer %q, want %q", where, got, exp)
		}
		for _, k := range mkeys {
			v, err := db.Get([]byte(k))
			if x, ok := want[k]; ok {
				if err != nil || string(v) != x.v {
					t.Fatalf("%s: writer Get(%q) = %q, %v, want %q", where, k, v, err, x.v)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s: writer Get(%q) = %q, %v", where, k, v, err)
			}
		}
	}
	// expectLen is the longest prefix a reader can replay exactly from rep.
	expectLen := func() uint64 {
		h, err := rep.Head()
		if errors.Is(err, kololog.ErrNoHead) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		n := h.Size
		for i := uint64(0); i < n; i++ {
			if _, err := rep.Get(i); errors.Is(err, kololog.ErrKeyUnavailable) {
				n = i
				break
			}
		}
		for L := n; ; L-- {
			ok := true
			for j := uint64(0); j < L && ok; j++ {
				if _, err := rep.Get(j); errors.Is(err, kololog.ErrDeleted) {
					d, listed := m.delBy[j]
					ok = listed && d < L
				}
			}
			if ok {
				return L
			}
		}
	}
	// checkView checks that a view at length L shows hist[L], minus values destroyed in src since.
	checkView := func(where string, g getter, it func() *Iterator, L uint64, src interface {
		Get(uint64) ([]byte, error)
	}, exact bool) {
		t.Helper()
		want := m.hist[L]
		gone := func(k string) bool {
			_, err := src.Get(want[k].at)
			return errors.Is(err, kololog.ErrDeleted)
		}
		for _, k := range mkeys {
			v, err := g.Get([]byte(k))
			x, ok := want[k]
			switch {
			case !ok:
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("%s: Get(%q) at L=%d = %q, %v; want not found", where, k, L, v, err)
				}
			case errors.Is(err, ErrNotFound) && !exact && gone(k):
			case err != nil || string(v) != x.v:
				t.Fatalf("%s: Get(%q) at L=%d = %q, %v; want %q", where, k, L, v, err, x.v)
			}
		}
		got := pairs(t, it())
		var exp []string
		for _, k := range sortedKeys(want) {
			if !exact && gone(k) {
				continue
			}
			exp = append(exp, k+"="+want[k].v)
		}
		if !slices.Equal(got, exp) {
			t.Fatalf("%s: range at L=%d %q, want %q", where, L, got, exp)
		}
	}
	checkReader := func(where string, refreshed bool) {
		t.Helper()
		L := reader.Len()
		if refreshed {
			if want := expectLen(); L != want {
				t.Fatalf("%s: reader Len %d, want %d", where, L, want)
			}
		}
		if L > uint64(len(m.ents)) {
			t.Fatalf("%s: reader Len %d past writer %d", where, L, len(m.ents))
		}
		checkView(where+" reader", reader, func() *Iterator { return reader.Range(nil, nil) }, L, rep, refreshed)
		for _, sc := range snaps {
			src := interface{ Get(uint64) ([]byte, error) }(db.log)
			if sc.rep {
				src = rep
			}
			checkView(where+" snapshot", sc.s, func() *Iterator { return sc.s.Range(nil, nil) }, sc.L, src, false)
		}
	}
	reopenWriter := func() {
		t.Helper()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		l, err := s.Open(id, logID)
		if err != nil {
			t.Fatal(err)
		}
		if db, err = Open(l); err != nil {
			t.Fatalf("reopen writer: %v", err)
		}
		snaps = slices.DeleteFunc(snaps, func(sc snapCheck) bool { return !sc.rep })
	}
	val := func() string {
		if rng.IntN(5) == 0 {
			return ""
		}
		return fmt.Sprintf("v%d", rng.IntN(1000))
	}

	record()
	steps := 40 + rng.IntN(200)
	for step := 0; step < steps; step++ {
		k := mkeys[rng.IntN(len(mkeys))]
		where := fmt.Sprintf("step %d", step)
		switch c := rng.IntN(20); {
		case c < 6: // put
			v := val()
			if len(m.puts[k]) >= maxPuts {
				m.appendEnt(ment{del: true, key: k, list: slices.Clone(m.puts[k])}, "")
			}
			m.appendEnt(ment{key: k}, v)
			if err := db.Put(context.Background(), []byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			record()
			checkWriter(where + " put")
		case c < 8: // delete
			if _, ok := m.cur()[k]; ok {
				m.appendEnt(ment{del: true, key: k, list: slices.Clone(m.puts[k])}, "")
			}
			if err := db.Delete(context.Background(), []byte(k)); err != nil {
				t.Fatal(err)
			}
			record()
			checkWriter(where + " delete")
		case c < 9: // crash between the delete op and its destroys
			if _, ok := m.cur()[k]; !ok {
				continue
			}
			list := slices.Clone(m.puts[k])
			if !slices.Equal(list, db.puts[k]) {
				t.Fatalf("writer puts %v, model %v", db.puts[k], list)
			}
			m.appendEnt(ment{del: true, key: k, list: list}, "")
			if _, err := db.log.Append(encodeOp(op{del: true, key: []byte(k), list: list})); err != nil {
				t.Fatal(err)
			}
			for _, j := range list[:rng.IntN(len(list)+1)] {
				if err := db.log.Delete(j); err != nil {
					t.Fatal(err)
				}
			}
			record()
			// A reader may sync the half-done delete before the writer restarts.
			if rng.IntN(2) == 0 {
				syncStep(t, rng, rep, db.log, orig, heads)
			}
			reopenWriter()
			for _, j := range list {
				if _, err := db.log.Get(j); !errors.Is(err, kololog.ErrDeleted) {
					t.Fatalf("listed entry %d not destroyed after reopen: %v", j, err)
				}
			}
			checkWriter(where + " crash")
		case c < 10:
			reopenWriter()
			checkWriter(where + " reopen")
		case c < 11:
			sn, err := reader.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			snaps = append(snaps, snapCheck{sn, sn.Len(), true})
			sn, err = db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			snaps = append(snaps, snapCheck{sn, sn.Len(), false})
		case c < 17:
			syncStep(t, rng, rep, db.log, orig, heads)
			checkReader(where+" sync", false)
		case c < 19:
			if err := reader.Refresh(); err != nil {
				t.Fatalf("%s: refresh: %v", where, err)
			}
			checkReader(where+" refresh", true)
		default: // reopen the reader from its store
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			snaps = slices.DeleteFunc(snaps, func(sc snapCheck) bool { return sc.rep })
			if rep, err = rs.OpenReplica(hdr.LogKey, hdr.LogID, nil); err != nil {
				t.Fatalf("%s: reopen replica: %v", where, err)
			}
			if reader, err = OpenReader(rep); err != nil {
				t.Fatalf("%s: reopen reader: %v", where, err)
			}
			checkReader(where+" reader reopen", true)
		}
	}
	// After a full sync the reader converges to the writer.
	full(t, rep, db.log, orig)
	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	checkReader("final", true)
	if reader.Len() != uint64(len(m.ents)) {
		t.Fatalf("final reader Len %d, writer %d", reader.Len(), len(m.ents))
	}
}

func sortedKeys(m map[string]mval) []string {
	return slices.Sorted(maps.Keys(m))
}

func cloneEntry(e kololog.Entry) kololog.Entry {
	return kololog.Entry{Record: bytes.Clone(e.Record), Ciphertext: bytes.Clone(e.Ciphertext), Marker: bytes.Clone(e.Marker), Key: kololog.EntryKey(bytes.Clone(e.Key))}
}

// refused reports whether err is kolo-log refusing an update the random sync made up, not a failure.
func refused(err error) bool {
	for _, r := range []error{kololog.ErrRollback, kololog.ErrDiverged, kololog.ErrBadProof, kololog.ErrBadEntry} {
		if errors.Is(err, r) {
			return true
		}
	}
	return false
}

func syncStep(t *testing.T, rng *rand.Rand, rep *kolostore.Replica, l *kolostore.Log, orig []kololog.Entry, heads []kololog.Head) {
	t.Helper()
	switch rng.IntN(4) {
	case 0, 1: // Update toward a past or current head, a chunk at a time
		h := heads[len(heads)-1]
		if rng.IntN(3) == 0 {
			h = heads[rng.IntN(len(heads))]
		}
		n := rep.Len()
		if h.Size < n {
			if err := rep.Update(h, kololog.ConsistencyProof{}, nil); err != nil && !refused(err) {
				t.Fatal(err)
			}
			return
		}
		m := n + uint64(rng.IntN(int(h.Size-n)+1))
		if rng.IntN(2) == 0 {
			m = h.Size
		}
		var p kololog.ConsistencyProof
		var err error
		if m < h.Size {
			p, err = l.ConsistencyProof(m, h.Size)
		} else if n > 0 && n < h.Size {
			p, err = l.ConsistencyProof(n, h.Size)
		}
		if err != nil {
			return
		}
		var es []kololog.Entry
		for i := n; i < m; i++ {
			var e kololog.Entry
			if rng.IntN(3) == 0 {
				e = cloneEntry(orig[i]) // read before a delete
			} else {
				e, err = l.Entry(i)
				if err != nil {
					t.Fatal(err)
				}
			}
			if e.Marker == nil && rng.IntN(4) == 0 {
				e.Key = nil
			}
			es = append(es, e)
		}
		if err := rep.Update(h, p, es); err != nil && !refused(err) {
			t.Fatal(err)
		}
	case 2: // late key
		if rep.Len() == 0 {
			return
		}
		i := uint64(rng.IntN(int(rep.Len())))
		if _, err := rep.Get(i); errors.Is(err, kololog.ErrKeyUnavailable) {
			if err := rep.AddKey(i, bytes.Clone(orig[i].Key)); err != nil {
				t.Fatalf("AddKey %d: %v", i, err)
			}
		}
	case 3: // a delete marker, in any order
		if rep.Len() == 0 {
			return
		}
		i := uint64(rng.IntN(int(rep.Len())))
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		if e.Marker != nil {
			if err := rep.ApplyDelete(e.Marker); err != nil {
				t.Fatalf("ApplyDelete %d: %v", i, err)
			}
		}
	}
}

// full brings rep to the writer's head with every key and marker.
func full(t *testing.T, rep *kolostore.Replica, l *kolostore.Log, orig []kololog.Entry) {
	t.Helper()
	h, err := l.Head()
	if err != nil {
		t.Fatal(err)
	}
	// A pending target must be reached first: fill it to the writer's head in one chunk.
	n := rep.Len()
	var p kololog.ConsistencyProof
	if n > 0 && n < h.Size {
		if p, err = l.ConsistencyProof(n, h.Size); err != nil {
			t.Fatal(err)
		}
	}
	var es []kololog.Entry
	for i := n; i < h.Size; i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		es = append(es, e)
	}
	if err := rep.Update(h, p, es); err != nil {
		t.Fatalf("full update: %v", err)
	}
	for i := uint64(0); i < rep.Len(); i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		if e.Marker != nil {
			if err := rep.ApplyDelete(e.Marker); err != nil {
				t.Fatal(err)
			}
		} else if _, err := rep.Get(i); errors.Is(err, kololog.ErrKeyUnavailable) {
			if err := rep.AddKey(i, bytes.Clone(orig[i].Key)); err != nil {
				t.Fatal(err)
			}
		}
	}
}
