# Local storage

This document describes the local storage role of SQLite.

## SQLite file

The local metadata database lives at:

`.git/git-meta.sqlite`

## Purpose

SQLite is the local read/write store for metadata.

It is responsible for:

- fast current-value lookup
- local mutation operations
- mutation history / provenance tracking
- tombstone tracking
- preparing data for serialization
- recording materialization state

The exchange format is Git trees and commits, not SQLite.

## Current state and history

There are two important local data views:

1. the current materialized value for a `(target, key)`
2. a mutation log showing how that value changed over time

Mutation history should retain at least:

- timestamp
- author email
- mutation type

## Current reference schema direction

The original project-level spec described these tables:

- `metadata` — current key rows and string values
- `list_values` — list items as one row per entry
- `metadata_log` — mutation history
- `metadata_tombstones` — latest removals

As new value types are added, the exact schema may evolve, but the local storage must still support:

- O(1)-ish access to current value state
- efficient append/member mutation for collection types
- enough provenance to resolve merge decisions during materialization

## Large values

Implementations may choose to keep some large values in Git blobs and store references locally rather than storing all payload bytes directly in SQLite.

That is a local implementation detail, not an exchange-format requirement.

## Materialization bookkeeping

Local storage should also record enough state to support incremental materialization and serialization, including:

- the last metadata commit successfully materialized per source ref
- which local values changed since last materialization / serialization
- any tombstones that still need to be emitted on serialize

These are local optimization and correctness concerns and need not be fully represented in the exchange tree.

## Refs

When serializing metadata, the commit/tree produced updates a local metadata ref so it can be pushed.

The local serialized metadata head by default should be:

- `refs/meta/local/main`

If `meta.namespace` Git config is set, that namespace should be used instead of `meta`.

Fetched remote metadata heads should be stored under a remote-specific namespace, for example:

- `refs/meta/remotes/origin`

If user has filter rules that route keys to additional destinations, those destinations get their own refs:

- `refs/meta/local/main` (default, always present)
- `refs/meta/local/private`
- `refs/meta/local/vendor`
