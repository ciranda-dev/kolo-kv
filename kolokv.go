package kolokv

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

const (
	// MaxKeySize is the largest key in bytes. Empty keys are allowed.
	MaxKeySize = 32 << 10
	// MaxValueSize is the largest value in bytes.
	MaxValueSize = 1 << 20
)

var (
	// ErrNotFound is returned for a key that is not in the store.
	ErrNotFound = errors.New("kolokv: not found")
	// ErrReadOnly is returned by Put and Delete on a store from OpenReader.
	ErrReadOnly = errors.New("kolokv: read-only store")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("kolokv: closed")
	// ErrKeyTooLarge is returned for a key over MaxKeySize.
	ErrKeyTooLarge = errors.New("kolokv: key too large")
	// ErrValueTooLarge is returned for a value over MaxValueSize.
	ErrValueTooLarge = errors.New("kolokv: value too large")
	// ErrCorrupt is returned when the log is not a valid kolokv log or does not match its signed head,
	// and by reads when a stored entry no longer matches its record.
	ErrCorrupt = errors.New("kolokv: corrupt log")
)

// DB is a key-value store kept in a kolo-log log. It is safe for concurrent use.
type DB struct {
	mu     sync.RWMutex
	log    *kolostore.Log      // nil on a reader
	rep    *kolostore.Replica  // nil on the writer
	index  []item              // live keys in byte order
	puts   map[string][]uint64 // writer: readable put entries of each live key, oldest first
	n      uint64
	closed bool
}

// Open rebuilds the store from l, your own log, and takes it over: write to l only through the DB, which closes it.
// The log must use the kololog.Bytes value encoding. If Open fails, l stays open.
func Open(l *kolostore.Log) (*DB, error) {
	if l == nil {
		return nil, errors.New("kolokv: nil log")
	}
	db := &DB{log: l}
	if err := db.load(); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenReader rebuilds a read-only store from r, a replica of someone's log, and takes it over: DB.Close closes it.
// Keep growing r with Update, AddKey and ApplyDelete, then call Refresh.
func OpenReader(r *kolostore.Replica) (*DB, error) {
	if r == nil {
		return nil, errors.New("kolokv: nil replica")
	}
	db := &DB{rep: r}
	if err := db.load(); err != nil {
		return nil, err
	}
	return db, nil
}

// load rebuilds the state from the entries the verified head covers.
func (db *DB) load() error {
	var hdr kololog.LogHeader
	var h kololog.Head
	var err error
	if db.log != nil {
		hdr = db.log.Header()
		h, err = db.log.Head()
	} else {
		hdr = db.rep.Header()
		h, err = db.rep.Head()
	}
	if hdr.Encoding != kololog.Bytes {
		return fmt.Errorf("%w: value encoding is not bytes", ErrCorrupt)
	}
	// A replica without a head may already hold the first chunk of a catch-up; it is not covered yet.
	if db.rep != nil && errors.Is(err, kololog.ErrNoHead) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("kolokv: %w", err)
	}
	vh, err := kololog.VerifyHead(hdr.LogKey, hdr.LogID, h)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	n := vh.Size()
	if db.log != nil && n != db.log.Len() {
		return fmt.Errorf("%w: head does not cover the log", ErrCorrupt)
	}
	st, err := replay(n, db.get, db.rep != nil)
	if err != nil {
		return err
	}
	db.index, db.n = st.index, st.n
	if db.log != nil {
		db.puts = st.puts
		// A Delete cut short by a crash left these readable; its delete op is already in the log.
		return db.erase(st.erase)
	}
	return nil
}

func (db *DB) get(i uint64) ([]byte, error) {
	if db.log != nil {
		return db.log.Get(i)
	}
	return db.rep.Get(i)
}

func (db *DB) erase(entries []uint64) error {
	if err := db.log.Delete(entries...); err != nil {
		return fmt.Errorf("kolokv: delete: %w", err)
	}
	return nil
}

func (db *DB) writable() error {
	if db.closed {
		return ErrClosed
	}
	if db.rep != nil {
		return ErrReadOnly
	}
	return nil
}

// Put sets key to value. Both are copied. Put does not destroy the older value of key; Delete does. Once key holds
// 65536 values since its last delete, the next Put first deletes them; if that delete fails after its delete entry
// was written, Put returns the error without setting key, and key is already deleted.
// A ctx that is already done writes nothing and returns ctx.Err(); after that check the write runs to its end.
func (db *DB) Put(ctx context.Context, key, value []byte) error {
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writable(); err != nil {
		return err
	}
	k := string(key)
	if len(db.puts[k]) >= maxPuts {
		if err := db.remove(k); err != nil {
			return err
		}
	}
	at, err := db.log.Append(encodeOp(op{key: key, value: value}))
	if err != nil {
		return fmt.Errorf("kolokv: put: %w", err)
	}
	db.n = at + 1
	db.puts[k] = append(db.puts[k], at)
	if i, ok := slices.BinarySearchFunc(db.index, k, compareItem); ok {
		db.index[i].at = at
	} else {
		db.index = slices.Insert(db.index, i, item{k, at})
	}
	return nil
}

// Delete removes key with a delete entry that lists every value of key since its last delete, then destroys
// their entry keys in one write. A missing key is not an error.
// If Delete fails after its delete entry was written, the key is gone but its old entries are not destroyed yet;
// open the log again to destroy them. A ctx that is already done writes nothing and returns ctx.Err().
func (db *DB) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writable(); err != nil {
		return err
	}
	return db.remove(string(key))
}

// remove appends a delete op that lists every value entry of k, then destroys them.
func (db *DB) remove(k string) error {
	i, ok := slices.BinarySearchFunc(db.index, k, compareItem)
	if !ok {
		return nil
	}
	puts := db.puts[k]
	at, err := db.log.Append(encodeOp(op{del: true, key: []byte(k), list: puts}))
	if err != nil {
		return fmt.Errorf("kolokv: delete: %w", err)
	}
	db.n = at + 1
	db.index = slices.Delete(db.index, i, i+1)
	delete(db.puts, k)
	return db.erase(puts)
}

// Get returns a copy of the value of key, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	i, ok := slices.BinarySearchFunc(db.index, string(key), compareItem)
	var x item
	if ok {
		x = db.index[i]
	}
	db.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return db.value(x)
}

// value reads the value of x through the log; a value destroyed since x was indexed is ErrNotFound.
func (db *DB) value(x item) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	b, err := db.get(x.at)
	if errors.Is(err, kololog.ErrDeleted) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, readErr(err)
	}
	o, err := decodeOp(b)
	if err != nil {
		return nil, err
	}
	if o.del || string(o.key) != x.key {
		return nil, errBadOp
	}
	return o.value, nil
}

// Range returns an iterator over the keys in [start, end) in byte order, as of now; a nil end has no upper bound.
func (db *DB) Range(start, end []byte) *Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return &Iterator{err: ErrClosed}
	}
	return &Iterator{db: db, items: slices.Clone(span(db.index, start, end))}
}

// Prefix returns an iterator over the keys that start with prefix in byte order, as of now.
func (db *DB) Prefix(prefix []byte) *Iterator {
	return db.Range(prefix, upper(prefix))
}

// Snapshot returns a read view of the store at its current log length.
func (db *DB) Snapshot() (*Snapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return &Snapshot{db: db, index: slices.Clone(db.index), n: db.n}, nil
}

func (db *DB) isClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

// Len returns the number of log entries the state is built from. On a reader it is at most the size of the
// replica's last complete verified head, and less while an entry key or a delete entry is missing.
func (db *DB) Len() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.n
}

// Refresh rebuilds a reader's state from the longest prefix of its replica it can replay exactly; on the writer it
// does nothing. On an error the state stays as it was.
func (db *DB) Refresh() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.rep == nil {
		return nil
	}
	return db.load()
}

// Close closes the store and its log or replica. Closing twice is not an error.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	db.index, db.puts = nil, nil
	if db.log != nil {
		return db.log.Close()
	}
	return db.rep.Close()
}
