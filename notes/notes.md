## Notes

- delete specific list entries (by value?)
- serialize filter rules
- path based pruning rules (path no longer exists, etc)
- remote management and pushspecs
- run initial blobless fetch and first tree materialization
- namespaces (local, shared, internal, etc - push targets (none, remote)
  - materialize targets too
  - on conflicts, which wins?

## Scenarios

- simple
  - user A adds a key, serializes, pushes to meta remote
  - user B fetches, materializes, adds a key, modifies the first key, pushes to remote
  - user A adds a third key
  - user A fetches and materializes, has all 3 keys

## Stuff Butler needs to do

- transfer metadata
