# Limits

Every bound in kolo-kv, with its value. This page is reference material: the README does not repeat it.

| What | Value | Error |
|---|---|---|
| Key size (`MaxKeySize`) | 32 KiB (32768 bytes); the empty key is valid | `ErrKeyTooLarge` |
| Value size (`MaxValueSize`) | 1 MiB (1048576 bytes) | `ErrValueTooLarge` |
| Values per key since its last delete | 65536; the next `Put` first deletes them, which bounds the size of a delete entry | |
| Entries listed by one delete entry | 1 to 65536 | `ErrCorrupt` when a stored one is outside the range |
| Log value encoding | `kololog.Bytes` | `ErrCorrupt` from `Open` for another encoding |

## Behavior at the bounds

- If the delete that `Put` makes when a key reaches 65536 values fails after its delete entry is written, `Put` returns the error without setting the key, and the key is already deleted.
- Arguments to `Put` and `Delete` are copied. `Get` returns a copy.
- A `Delete` of a missing key is not an error and writes nothing.
- Values are at most 1 MiB, not 2 GiB: every value is one log entry that each reader stores and that a sync sends to each peer.

## Memory

The index (each live key and the number of its entry) is in memory; values stay in the log and are read from storage when asked for. A writer also keeps the entry number of each readable value of every live key, 8 bytes each. A `Snapshot` copies the whole index (keys and entry numbers, no values), holds no resources and needs no closing; a `Range` copies only the keys in its range.

`Open`, `OpenReader` and `Refresh` read every entry once. While they run they hold a record of under 100 bytes for each entry, plus each distinct key of a readable put once, so a key that is overwritten many times, or whose older entries are destroyed, is not held once per entry. A log that leaves its destroyed entries readable can make the replay hold every distinct key it contains, which is at most the key bytes the log already stores.

A put of a new key inserts into a sorted slice and moves the keys after it while it holds the write lock. In a measurement on one laptop (Apple M5) an insert took about 14 microseconds with 100,000 keys and about 0.4 milliseconds with 1,000,000 keys; reads wait for that time.

## Concurrency

A `DB` and a `Snapshot` are safe for concurrent use. Writes are serialized. An `Iterator` is for one goroutine. No call waits on the network: `Put` and `Delete` take a `context.Context` for the `Store` interface and check it once before they write.

## Not provided

There are no transactions, no secondary indexes and no compaction.

## Deletion

Deletion is best effort. It holds on peers that follow the protocol and apply the delete marker, which destroys the entry key. A peer that already received the key can keep it, and old copies of a disk keep a working key (see kolo-store). A deleted key's name stays in its delete entry: keep secrets in values, not in keys. A `Put` over an existing key does not destroy the older value; only `Delete` does.

## Build

Pebble, used by kolo-store, links the C zstd library when cgo is enabled. Build with `CGO_ENABLED=0` for a pure Go binary.
