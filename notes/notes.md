## Notes

- partial blob hydration on get
- ability to set timestamp
- better `git meta get` default formatting (dont show large blobs, only first line)
- simultaneous prune testing

- namespaces (local, shared, internal, etc - push targets (none, remote)
  - materialize targets too
  - on conflicts, which wins?

- benchmarking on entire
- full report on entire (if they switched)

- plan out gb use, enablement, integration points

---

- better pruning rules
  - path no longer exists (prune paths at all?)
  - don't prune otherwise prunable peers
- prune filters (different rules for different key patterns - always keep X, only keep Y for a week, etc)

- serialize check for keys (high entropy)
