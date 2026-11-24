# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- `DB.Put` and `DB.Delete` take a `context.Context` as their first argument. A `ctx` that is already done writes nothing and returns `ctx.Err()`.

### Added

- `Store`, the interface that `*DB` implements: `Put`, `Delete`, `Get`, `Range`, `Prefix` and `Close`.
- `NewIterator`, an `Iterator` over an `iter.Seq2` for a `Store` outside this package.
- `kolokvtest.TestStore`, a test suite for the `Store` contract.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-kv.
