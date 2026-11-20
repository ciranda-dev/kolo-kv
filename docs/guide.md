# Guide

Walkthrough of the API and the design. The on-disk form is in [formats.md](formats.md) and the bounds in [limits.md](limits.md). A complete program is in the README, and [examples/basic](../examples/basic/main.go) also reopens the store and reads it from a replica.

## API

- `Open(l)` rebuilds a writable store from your own kolo-store `Log`.
  `OpenReader(r)` rebuilds a read-only store from a kolo-store `Replica`
  of someone else's log. The `DB` takes the handle over: `Close` closes it,
  and on the writer every write must go through the `DB`. The log must use
  the `kololog.Bytes` value encoding. If `Open` fails, the handle stays
  open.
- `Put(key, value)` sets a key. Once a key holds 65536 values since its
  last delete, the next `Put` first deletes them, which bounds the size of
  a delete entry. If that delete fails after its delete entry is written,
  `Put` returns the error without setting the key, and the key is already
  deleted. Keys are at most `MaxKeySize` (32 KiB) and
  values at most `MaxValueSize` (1 MiB), or the call returns
  `ErrKeyTooLarge` or `ErrValueTooLarge`. The empty key is a valid key.
  Arguments are copied.
- `Get(key)` returns a copy of the value, or `ErrNotFound`.
- `Delete(key)` removes a key. A missing key is not an error and writes
  nothing.
- `Range(start, end)` iterates over the keys in `[start, end)` in byte
  order. A nil `end` has no upper bound; `start >= end` is empty.
  `Prefix(p)` iterates over the keys that start with `p`. An iterator sees
  the keys as they were when it was made.
- `Iterator`: call `Next` before each `Key` and `Value`, then check `Err`.
  `Next` is false at the end, on an error and after `Close`. `Close` is
  idempotent and returns `Err`. Key and value slices are the caller's to
  keep.
- `Snapshot()` returns a read view at the current log length, with `Get`,
  `Range`, `Prefix` and `Len`. It copies the index (keys and entry
  numbers, no values), holds no resources and needs no closing.
- `Len()` is the number of log entries the state is built from: the state
  is exactly the writer's state after that many entries. On a reader it is
  at most the size of the replica's last complete verified head
  (`Replica.Head`), and less when the replica cannot replay all of it
  yet (see [Design](#design)). It can go down after a `Refresh`.
- `Refresh()` rebuilds a reader from its replica's latest verified head,
  after kolo-sync grew it, gave it a missing entry key or applied delete
  markers. It is one pass over the log. On the writer it does nothing.
- `Close()` closes the store and its log or replica. A second `Close`
  returns nil.

Errors, for `errors.Is`:

- `ErrNotFound`: the key is not in the store.
- `ErrReadOnly`: `Put` or `Delete` on a store from `OpenReader`.
- `ErrClosed`: the store is closed. A snapshot or iterator of a closed
  store returns it too.
- `ErrKeyTooLarge`, `ErrValueTooLarge`: over the size limits.
- `ErrCorrupt`: from `Open`, `OpenReader` and `Refresh`, the log is not a
  kolo-kv log, uses another value encoding, holds an entry that does not
  decode or a delete entry that lists a wrong entry, does not match its
  signed head, or, on the writer, holds an entry without its key or a
  destroyed entry no delete entry lists. From `Get`, `Snapshot.Get` and
  an `Iterator`, a stored entry no longer matches its record or does not
  decode. The kolo-log or kolo-store error is wrapped with it.
- Other kolo-store and kolo-log errors from the log or replica, such as
  `kolostore.ErrClosed` or a backend error, pass through wrapped.

A `DB` and a `Snapshot` are safe for concurrent use. Writes are serialized.
An `Iterator` is for one goroutine. On a reader, kolo-sync keeps calling
`Update`, `AddKey` and `ApplyDelete` on the same `Replica`; the `DB` shows
the new entries after `Refresh`. No call waits on the network, so none
takes a `context.Context`.

## Design

The state is the replay of a prefix of the log in order: a put sets its
key to that entry, a delete removes the key. The index maps each key to
the entry of its value, and reads decrypt that entry through kolo-log, so
a destroyed value is never kept elsewhere. Open verifies the head with
`kololog.VerifyHead` under the log's owner key; kolo-store has already
checked every entry, record, delete marker and the Merkle root against it.

Delete appends a delete entry that lists every entry holding a value of
the key since its last delete, removes the key from the index, then
destroys all the listed entries with one `Log.Delete`, which is one
durable write. All of them, not only the last: an overwrite keeps the
older entry, and a rebuild would otherwise bring an older value back. The
writer destroys entries only this way. The delete entry and the destroy
are two writes. If a crash or a failed write comes between them, the
delete entry is already in the log, so the key stays deleted and its
values stay readable on disk until the next `Open` destroys them.

The writer replays every entry its head covers. A destroyed entry is
skipped when a delete entry lists it; anything else (an unlisted destroyed
entry, a missing entry key, a list that names a wrong entry, an entry that
does not decode) is `ErrCorrupt`.

A reader replays the longest prefix it can replay exactly, up to its last
complete verified head, so its state is always the writer's state at some
length, and `Len` says which. The prefix stops:

- at the first entry the replica holds without its key (a peer can send
  an entry without it; `Refresh` goes further once `AddKey` gives it), and
- before a destroyed entry whose delete entry is not in the prefix. The
  reader cannot read a destroyed entry, so without its delete entry it
  cannot tell which key it held, and skipping it could show an older
  value of that key as current.

A reader that gets a delete marker before the delete entry that lists the
entry, for example from a sync that reads entries while the writer
deletes, goes back to the state before the destroyed entry until the
delete entry arrives. A value destroyed after the reader built its state
reads as missing at once.

Deletion is best effort. It holds on peers that follow the protocol and
apply the delete marker, which destroys the entry key. A peer that already
received the key can keep it, and old copies of a disk keep a working key
(see kolo-store). A deleted key's name stays in its delete entry; keep
secrets in values, not in keys. A snapshot or iterator made before a
`Delete` does not return the destroyed value: deletion wins over older
views.

There are no transactions, no secondary indexes and no compaction. The
index (each live key and the number of its entry) is in memory; values
stay in the log and are read from storage when asked for. A put of a new
key moves the index in memory.
