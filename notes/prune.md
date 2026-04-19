# git meta prune

## Overview

`git meta prune` re-materializes the serialized meta tree, keeping only values written within a
specified date range. It is designed to shrink a long-lived `refs/meta/local` history so that
a shallow fetch/clone is all most users need, while the full history remains available by
deepening the clone or fetching the pre-prune commit(s) explicitly.

## Command

```
git meta prune --since=<date> [--min-size=<bytes>]
```

- `--since=<date>` — retain only entries whose most-recent log timestamp is on or after this date.
  Accepts ISO-8601 dates (`2025-01-01`) or relative values (`90d`, `6m`, `1y`).
- `--min-size=<bytes>` — only prune a target subtree when its current size in the serialized tree
  exceeds this threshold. Subtrees under the threshold are kept in full regardless of age.
  Accepts human-friendly suffixes (`512k`, `10m`). If omitted, all target subtrees are pruned.

## What it does

1. Reads the current `refs/meta/local` commit and its tree.
2. For each non-`project` target subtree in the tree, checks:
   - If `--min-size` is set and the subtree is smaller than the threshold, leave it untouched.
   - Otherwise, drop any key blobs whose most-recent log entry in SQLite is older than `--since`.
     List entry blobs whose individual timestamp (encoded in the blob name) is older than
     `--since` are also dropped.
   - Tombstone entries are kept if the removal itself falls within the window; older tombstones
     are dropped (the key is simply absent from the new tree).
3. The `project` subtree is **never pruned** — project-level metadata is assumed to be small and
   always relevant.
4. Builds a new tree from the surviving blobs.
5. Writes a new commit on top of `refs/meta/local` with the new tree and a structured commit
   message (see below). The previous commit becomes the parent, so `git log` still shows the
   full ancestry and `git fetch --deepen` can recover older data.
6. Updates `git-meta.sqlite` — marks the prune epoch so subsequent `serialize` calls know not to
   re-emit dropped entries unless they are written again locally.

## Commit message format

```
git-meta: prune --since=2025-01-01

pruned: true
since: 2025-01-01T00:00:00Z
min-size: 512k
targets-pruned: 142
keys-dropped: 8731
keys-retained: 1204
```

Tooling can detect a prune commit by looking for `pruned: true` in the commit message body.

## Interaction with fetch/clone

Because `refs/meta/local` is an ordinary Git ref with a linear ancestry, the pre-prune history
is still accessible:

- `git fetch --depth=1` gives only the pruned, compact tree — the common case.
- `git fetch --unshallow` or `git fetch --deepen=N` retrieves older meta commits, including the
  full pre-prune tree, without any special tooling.
- `git meta materialize` already operates on whatever commits are locally available; if older commits
  are deepened in, a subsequent `git meta materialize` can incorporate them.

## Caveats

- Prune is a lossy operation on the live ref. It does not archive or rename the old ref — the
  older data is preserved only in git history, accessible via deepen. If you want a permanent
  named snapshot of the pre-prune state, tag the current head before running prune.
- Running `git meta prune` and then `git meta serialize` in quick succession is safe: serialize checks
  the prune epoch in SQLite and will not re-emit keys that were pruned unless they have been
  written again locally since the prune.
- Conflict resolution during a subsequent `git meta materialize` from a remote that has not yet
  pruned falls back to the normal merge rules. Keys the remote still carries that were locally
  pruned will be re-introduced via the merge — this is intentional, since the remote is the
  authoritative source for those values.
