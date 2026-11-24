# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** `DB.Put` and `DB.Delete` take a `context.Context` as their first argument. A `ctx` that is already done writes nothing and returns `ctx.Err()`.

### Added

- `Store`, the interface that `*DB` implements: `Put`, `Delete`, `Get`, `Range`, `Prefix` and `Close`.
- `NewIterator`, an `Iterator` over an `iter.Seq2` for a `Store` outside this package.
- `kolokvtest.TestStore`, a test suite for the `Store` contract. A nil `settle` means writes are readable at once.

### Fixed

- `NewIterator` with a nil `stop` panicked when the iteration ended or on `Close`; nil now means nothing to release. A nil `seq` yields nothing instead of panicking.

## [0.1.0] - 2026-11-20

### Added

- First release of kolo-kv.
