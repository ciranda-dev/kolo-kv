package kolokv

import (
	"bytes"
	"errors"
	"runtime"
	"slices"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
)

func FuzzDecodeOp(f *testing.F) {
	f.Add(encodeOp(op{key: []byte("k"), value: []byte("v")}))
	f.Add(encodeOp(op{del: true, key: []byte("k"), list: []uint64{0, 1, 300}}))
	f.Add(encodeOp(op{}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		o, err := decodeOp(b)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			return
		}
		if len(o.key) > MaxKeySize || len(o.value) > MaxValueSize || o.del && o.value != nil {
			t.Fatal("decoded op out of bounds")
		}
		if !bytes.Equal(encodeOp(o), b) {
			t.Fatal("decoded op does not encode back to its input")
		}
	})
}

// fuzzEntries splits b into entries: one state byte (0 readable, 1 destroyed, 2 no key), a length byte, the bytes.
// A readable entry whose first data byte is even is an encoded put, or a delete that lists earlier entries,
// of a short key, so replays get deep.
func fuzzEntries(b []byte) (entries [][]byte, gone []error) {
	for len(b) >= 2 {
		state, n := b[0]%3, int(b[1])
		b = b[2:]
		n = min(n, len(b))
		data := b[:n]
		b = b[n:]
		if len(data) > 0 && data[0]%2 == 0 {
			key := data[1:min(len(data), 3)]
			o := op{key: key, value: data}
			if pos := uint64(len(entries)); data[0]%4 == 0 && pos > 0 {
				o = op{del: true, key: key}
				for _, c := range data[min(len(data), 3):] {
					if j := uint64(c) % pos; len(o.list) == 0 || j > o.list[len(o.list)-1] {
						o.list = append(o.list, j)
					}
				}
				if len(o.list) == 0 {
					o.list = []uint64{0}
				}
			}
			data = encodeOp(o)
		}
		entries = append(entries, data)
		gone = append(gone, [...]error{nil, kololog.ErrDeleted, kololog.ErrKeyUnavailable}[state])
	}
	return entries, gone
}

func FuzzReplay(f *testing.F) {
	f.Add([]byte{0, 3, 2, 'a', 'b', 0, 3, 4, 'a', 'b', 1, 3, 2, 'a', 'c'}, true)
	f.Add([]byte{2, 1, 2, 0, 3, 'x', 'y', 'z'}, false)
	f.Add([]byte{1, 1, 2, 0, 3, 2, 'k', 0, 1, 4, 4, 'k', 0, 0}, true)
	f.Fuzz(func(t *testing.T, b []byte, reader bool) {
		entries, gone := fuzzEntries(b)
		get := func(i uint64) ([]byte, error) {
			if gone[i] != nil {
				return nil, gone[i]
			}
			return entries[i], nil
		}
		st, err := replay(uint64(len(entries)), get, reader)

		// The model, written plainly. limit is the first entry without its key on a reader.
		limit := len(entries)
		ops := make([]op, len(entries))
		bad := false
		for i := range entries {
			if reader && gone[i] == kololog.ErrKeyUnavailable {
				limit = i
				break
			}
			if gone[i] == kololog.ErrDeleted {
				continue
			}
			o, derr := decodeOp(entries[i])
			if gone[i] != nil || derr != nil {
				bad = true
				break
			}
			ops[i] = o
		}
		// listedBy[j] is every delete op below limit that lists entry j.
		listedBy := map[uint64][]int{}
		for i := 0; i < limit && !bad; i++ {
			if gone[i] != nil || !ops[i].del {
				continue
			}
			for _, j := range ops[i].list {
				listedBy[j] = append(listedBy[j], i)
				switch {
				case j >= uint64(i), len(listedBy[j]) > 1:
					bad = true
				case gone[j] == nil && (ops[j].del || string(ops[j].key) != string(ops[i].key)):
					bad = true
				}
			}
		}
		if bad {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("bad log: %v, want ErrCorrupt", err)
			}
			return
		}
		// A prefix of length l is exact if every destroyed entry in it is listed by a delete op in it.
		exact := func(l int) bool {
			for j := range l {
				if gone[j] == kololog.ErrDeleted && (len(listedBy[uint64(j)]) == 0 || listedBy[uint64(j)][0] >= l) {
					return false
				}
			}
			return true
		}
		end := limit
		for !exact(end) {
			end--
		}
		if !reader && end != len(entries) {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("writer with an inexact log: %v, want ErrCorrupt", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if st.n != uint64(end) {
			t.Fatalf("n = %d, want %d", st.n, end)
		}
		model := map[string]uint64{}
		for i := range end {
			switch {
			case gone[i] != nil:
			case ops[i].del:
				delete(model, string(ops[i].key))
			default:
				model[string(ops[i].key)] = uint64(i)
			}
		}
		if len(st.index) != len(model) || len(st.puts) != len(model) {
			t.Fatalf("index %d, puts %d, model %d", len(st.index), len(st.puts), len(model))
		}
		for i, x := range st.index {
			if i > 0 && st.index[i-1].key >= x.key {
				t.Fatal("index not sorted")
			}
			if model[x.key] != x.at {
				t.Fatalf("key at %d, model %d", x.at, model[x.key])
			}
			p := st.puts[x.key]
			if len(p) == 0 || p[len(p)-1] != x.at || !slices.IsSorted(p) {
				t.Fatalf("puts %v for entry %d", p, x.at)
			}
		}
		for _, e := range st.erase {
			if gone[e] != nil || ops[e].del || len(listedBy[e]) == 0 || listedBy[e][0] >= end {
				t.Fatalf("erase lists entry %d, not a readable put listed in the prefix", e)
			}
		}
	})
}

// FuzzOps runs puts and deletes against a map, then checks the reopened store and a reader.
func FuzzOps(f *testing.F) {
	f.Add([]byte{0, 'a', 0, 'b', 1, 'a', 0, 'a', 2, 'a'})
	f.Add([]byte{0, 'x', 0, 'x', 0, 'x', 1, 'x'})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64 {
			return
		}
		db, s, id := newDB(t)
		model := map[string]string{}
		pending := map[string][]uint64{} // readable puts of each live key
		var destroyed []uint64
		for i := 0; i+1 < len(b); i += 2 {
			k := string(b[i+1] % 8)
			if b[i]%3 == 1 {
				if _, ok := model[k]; ok {
					destroyed = append(destroyed, pending[k]...)
				}
				del(t, db, k)
				delete(model, k)
				delete(pending, k)
				continue
			}
			pending[k] = append(pending[k], db.Len())
			v := string(b[i : i+2])
			put(t, db, k, v)
			model[k] = v
		}
		r := newReplica(t, db)
		grow(t, r, db.log)
		applyMarkers(t, r, db.log)
		db = reopen(t, db, s, id)
		reader, err := OpenReader(r)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		var want []string
		for k, v := range model {
			want = append(want, k+"="+v)
		}
		slices.Sort(want)
		for _, g := range []*DB{db, reader} {
			if got := pairs(t, g.Range(nil, nil)); !slices.Equal(got, want) {
				t.Fatalf("got %q, want %q", got, want)
			}
		}
		for i := range db.Len() {
			_, err := db.log.Get(i)
			if errors.Is(err, kololog.ErrDeleted) != slices.Contains(destroyed, i) {
				t.Fatalf("entry %d: %v, destroyed %v", i, err, destroyed)
			}
		}
	})
}

func TestDecodeOpRejects(t *testing.T) {
	good := encodeOp(op{key: []byte("k"), value: []byte("v")})
	other := slices.Clone(good)
	other[len("kolo-kv/op/v")+1] = '1'
	kind := slices.Clone(good)
	kind[len(opTag)+1] = 2
	for name, b := range map[string][]byte{
		"empty":             nil,
		"trailing":          append(slices.Clone(good), 0),
		"truncated":         good[:len(good)-1],
		"tag":               other,
		"kind":              kind,
		"delete with value": append(encodeOp(op{del: true, key: []byte("k")}), 1, 'v'),
	} {
		if _, err := decodeOp(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
	if o, err := decodeOp(good); err != nil || string(o.key) != "k" || string(o.value) != "v" || o.del {
		t.Fatalf("good op: %+v %v", o, err)
	}
}

func TestDecodeOpRejectsListLongerThanInput(t *testing.T) {
	b := hugeListOp()
	if _, err := decodeOp(b); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("list of %d claimed in %d bytes: %v, want ErrCorrupt", maxPuts, len(b), err)
	}
	// A list that fits its input is still accepted.
	o := op{del: true, key: []byte("k"), list: []uint64{0, 1, 2}}
	if got, err := decodeOp(encodeOp(o)); err != nil || !slices.Equal(got.list, o.list) {
		t.Fatalf("list that fits: %+v %v", got, err)
	}
}

func TestDecodeOpHugeListAllocatesLittle(t *testing.T) {
	b := hugeListOp()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 100 {
		decodeOp(b)
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / 100; per > 1024 {
		t.Fatalf("a %d byte op that claims %d entries allocated %d bytes per decode", len(b), maxPuts, per)
	}
}

func BenchmarkDecodeOpHugeList(b *testing.B) {
	in := hugeListOp()
	b.ReportAllocs()
	for b.Loop() {
		decodeOp(in)
	}
}

// hugeListOp is a delete op that claims maxPuts listed entries and holds one.
func hugeListOp() []byte {
	var e kolocodec.Encoder
	e.PutString(opTag)
	e.PutUvarint(opDelete)
	e.PutBytes([]byte("k"))
	e.PutUvarint(uint64(maxPuts))
	e.PutUvarint(0)
	return e.Bytes()
}
