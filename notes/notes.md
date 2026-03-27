## Notes

- watch adds prompts only to agent:prompts on change-id
- better pruning rules
  - path no longer exists (prune paths at all?)
  - don't prune otherwise prunable peers
- prune filters (different rules for different key patterns - always keep X, only keep Y for a week, etc)
- remote management and pushspecs
- serialize check for keys (high entropy)
- run initial blobless fetch and first tree materialization
- namespaces (local, shared, internal, etc - push targets (none, remote)
  - materialize targets too
  - on conflicts, which wins?
- add a README.md to the serialized tree in case someone actually clones or tries to view the commit on a forge
