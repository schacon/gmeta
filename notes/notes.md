## Notes

- better pruning rules
  - path no longer exists (prune paths at all?)
  - don't prune otherwise prunable peers
- prune filters (different rules for different key patterns - always keep X, only keep Y for a week, etc)

- run initial blobless fetch and first tree materialization
- namespaces (local, shared, internal, etc - push targets (none, remote)
  - materialize targets too
  - on conflicts, which wins?
- add a README.md to the serialized tree in case someone actually clones or tries to view the commit on a forge
- serialize check for keys (high entropy)
