package kolokv

import (
	"cmp"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

// opTag starts every entry; it versions the op format and marks the log as a kolokv log.
const opTag = "kolo-kv/op/v0"

const (
	opPut    = 0
	opDelete = 1
)

// maxPuts is how many values a key keeps between deletes, which bounds the list in a delete op.
// A var so tests can lower it.
var maxPuts = 1 << 16

var errBadOp = fmt.Errorf("%w: malformed op", ErrCorrupt)

// op is a put, or a delete that lists, ascending, the entries of every value of key it destroys.
type op struct {
	del   bool
	key   []byte
	value []byte
	list  []uint64
}

func encodeOp(o op) []byte {
	var e kolocodec.Encoder
	e.PutString(opTag)
	if o.del {
		e.PutUvarint(opDelete)
		e.PutBytes(o.key)
		e.PutUvarint(uint64(len(o.list)))
		for i, at := range o.list {
			if i > 0 {
				at -= o.list[i-1] + 1
			}
			e.PutUvarint(at)
		}
		return e.Bytes()
	}
	e.PutUvarint(opPut)
	e.PutBytes(o.key)
	e.PutBytes(o.value)
	return e.Bytes()
}

func decodeOp(b []byte) (op, error) {
	d := kolocodec.NewDecoder(b)
	tag, err := d.String(len(opTag))
	if err != nil || tag != opTag {
		return op{}, errBadOp
	}
	kind, err := d.UvarintMax(opDelete)
	if err != nil {
		return op{}, errBadOp
	}
	o := op{del: kind == opDelete}
	if o.key, err = d.Bytes(MaxKeySize); err != nil {
		return op{}, errBadOp
	}
	if o.del {
		n, err := d.UvarintMax(uint64(maxPuts))
		// Each list entry takes at least one byte, so a longer claim is refused before it sizes an allocation.
		if err != nil || n == 0 || n > uint64(d.Len()) {
			return op{}, errBadOp
		}
		o.list = make([]uint64, n)
		for i := range o.list {
			v, err := d.Uvarint()
			if err != nil {
				return op{}, errBadOp
			}
			if i > 0 {
				prev := o.list[i-1]
				if v >= math.MaxUint64-prev {
					return op{}, errBadOp
				}
				v += prev + 1
			}
			o.list[i] = v
		}
	} else {
		if o.value, err = d.Bytes(MaxValueSize); err != nil {
			return op{}, errBadOp
		}
	}
	if d.Done() != nil {
		return op{}, errBadOp
	}
	return o, nil
}

// item is one live key and the log index of its put entry.
type item struct {
	key string
	at  uint64
}

func compareItem(x item, key string) int {
	return cmp.Compare(x.key, key)
}

// state is the result of a replay.
type state struct {
	index []item              // live keys in byte order
	n     uint64              // entries the state is built from
	puts  map[string][]uint64 // readable puts of each live key, oldest first
	erase []uint64            // readable puts that a delete op lists
}

// replay rebuilds the state from the longest prefix of the n entries that it can replay exactly: it stops at an
// entry without its key (only a reader may hold one) and before any destroyed entry whose delete op is not in the
// prefix. On the writer the prefix must be all n entries. Each distinct key is held once, not per entry.
func replay(n uint64, get func(uint64) ([]byte, error), reader bool) (state, error) {
	type entry struct {
		gone bool     // destroyed by a delete marker
		del  bool     // a delete op
		key  [32]byte // SHA-256 of the key, so entries compare keys without holding them
		list []uint64 // the entries a delete op lists
		by   uint64   // the delete op that lists this entry, 0 for none
	}
	var es []entry
	names := make(map[[32]byte]string)
	for i := uint64(0); i < n; i++ {
		b, err := get(i)
		if errors.Is(err, kololog.ErrDeleted) {
			es = append(es, entry{gone: true})
			continue
		}
		if reader && errors.Is(err, kololog.ErrKeyUnavailable) {
			break
		}
		if err != nil {
			return state{}, readErr(err)
		}
		o, err := decodeOp(b)
		if err != nil {
			return state{}, err
		}
		e := entry{del: o.del, key: sha256.Sum256(o.key), list: o.list}
		if o.del {
			for _, j := range o.list {
				if j >= i || es[j].by != 0 || !es[j].gone && (es[j].del || es[j].key != e.key) {
					return state{}, fmt.Errorf("%w: delete op %d lists a wrong entry", ErrCorrupt, i)
				}
				es[j].by = i
			}
		} else if _, ok := names[e.key]; !ok {
			names[e.key] = string(o.key)
		}
		es = append(es, e)
	}

	// m is the last delete op that explains a destroyed entry seen so far.
	var m, end uint64
	for i, e := range es {
		if e.gone && e.by == 0 {
			break
		}
		if e.gone {
			m = max(m, e.by)
		}
		if m <= uint64(i) {
			end = uint64(i) + 1
		}
	}
	if !reader && end != n {
		return state{}, fmt.Errorf("%w: destroyed entry without its delete op", ErrCorrupt)
	}

	st := state{n: end, puts: make(map[string][]uint64)}
	live := make(map[[32]byte]uint64)
	puts := make(map[[32]byte][]uint64)
	for i, e := range es[:end] {
		switch {
		case e.gone:
		case e.del:
			delete(live, e.key)
			delete(puts, e.key)
			for _, j := range e.list {
				if !es[j].gone {
					st.erase = append(st.erase, j)
				}
			}
		default:
			live[e.key] = uint64(i)
			puts[e.key] = append(puts[e.key], uint64(i))
		}
	}
	st.index = make([]item, 0, len(live))
	for h, at := range live {
		k := names[h]
		st.index = append(st.index, item{k, at})
		st.puts[k] = puts[h]
	}
	slices.SortFunc(st.index, func(a, b item) int { return cmp.Compare(a.key, b.key) })
	return st, nil
}

// readErr keeps storage errors as they are and reports a bad entry as ErrCorrupt.
func readErr(err error) error {
	for _, bad := range []error{kololog.ErrMalformedEntry, kololog.ErrMalformed, kololog.ErrKeyUnavailable, kololog.ErrBadEntry, kololog.ErrStorageMismatch, kolostore.ErrCorrupt} {
		if errors.Is(err, bad) {
			return fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
	}
	return fmt.Errorf("kolokv: %w", err)
}

// span returns the items of index in [start, end); a nil end has no upper bound.
func span(index []item, start, end []byte) []item {
	lo, _ := slices.BinarySearchFunc(index, string(start), compareItem)
	hi := len(index)
	if end != nil {
		hi, _ = slices.BinarySearchFunc(index, string(end), compareItem)
	}
	if lo >= hi {
		return nil
	}
	return index[lo:hi]
}

// upper returns the smallest key after every key that starts with prefix, or nil if there is none.
func upper(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			end := slices.Clone(prefix[:i+1])
			end[i]++
			return end
		}
	}
	return nil
}
