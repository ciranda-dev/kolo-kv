// Package kolokv is a single-writer ordered key-value store on top of a kolo-log log.
//
// Every Put and Delete is one log entry, so the store is signed, verifiable and replicates like any kolo-log log.
// Open rebuilds the state from the log of a kolo-store Log; OpenReader does the same, read-only, from a
// kolo-store Replica. Delete destroys the entry keys of every value of the key, so on peers that follow the
// protocol the old values are gone; a peer that already read them can keep them.
//
// Experimental: the API may change before v1.0.0.
package kolokv
