# Workflow

This document describes how a higher-level version control system might incorporate gmeta's lower-level serialize and materialize operations into normal push and pull flows.

The exchange-format documents define the tree shape, merge rules, and materialization semantics. This document is about orchestration: when those operations happen, what they are associated with, and how a host tool like GitButler or Jujutsu may make them feel integrated.

## Scope

A higher-level system may treat gmeta as a metadata engine with two core exchange operations:

- **serialize** local shareable metadata state into a metadata commit
- **materialize** one or more incoming metadata commits into the local database

The host VCS remains responsible for deciding:

- when serialize runs
- when materialize runs
- which code remote or metadata remote is associated with the operation
- which metadata namespaces are shared vs local-only
- how retries, status reporting, and user prompts are surfaced

## Push integration

A common integration model is to serialize metadata as part of push.

### Desired behavior

When the host tool pushes source-code changes, it may also:

1. determine which metadata is eligible to share
2. serialize that state into a metadata commit
3. update the local metadata ref
4. push the metadata ref alongside the code push

In practice, this means the user experiences metadata exchange as part of normal collaboration, rather than as a separate manual step.

### Why serialize on push

This keeps local metadata mutation cheap while still allowing shared metadata to converge:

- local writes stay in SQLite or another efficient local store
- metadata is only converted into Git trees when exchange is needed
- repeated local edits do not require repeated Git object writes
- the host tool can apply destination filters before publishing

### Typical push flow

A host tool might implement push roughly as:

1. prepare the code push
2. gather shareable metadata for the destination
3. run `git meta serialize`
4. attempt to push the metadata ref
5. if the metadata ref advanced remotely, fetch that metadata head, merge/retry, and push again
6. complete the overall push once both code and metadata are in the desired state

The metadata history should usually remain linear even if the source-code history is not.

## Pull integration

A common integration model is to materialize metadata as part of pull, fetch, sync, or checkout update.

### Desired behavior

When the host tool fetches or syncs new source-code state, it may also:

1. fetch the metadata ref or refs
2. identify the new metadata head for each source
3. materialize those heads into the local metadata database
4. make the newly visible metadata available to local queries and UI

### Why materialize on pull

This lets the host tool keep metadata in a query-friendly local form:

- UI and CLI queries do not have to walk Git trees every time
- conflict decisions are applied once during materialization
- provenance and current visible state can be indexed locally
- multiple metadata sources can be combined into one local view

### Typical pull flow

A host tool might implement pull roughly as:

1. fetch code refs
2. fetch metadata refs
3. for each metadata source, compare the new metadata head with the last materialized point
4. run `git meta materialize`
5. update local bookkeeping to record success
6. continue with any normal source-code update behavior

## Integrated sync model

Some systems may present a single `sync` concept rather than separate `push` and `pull` commands.

In that case, the orchestration can still be thought of as:

- **outbound path**: serialize before publishing local metadata
- **inbound path**: materialize after receiving remote metadata

A combined sync might therefore:

1. fetch code and metadata
2. materialize incoming metadata
3. update local code state
4. serialize any newly shareable local metadata
5. push code and metadata

The exact order may vary by product, but inbound materialization and outbound serialization are the key boundary points.

## Relationship to code operations

A host tool does not need a 1:1 mapping between source-code commits and metadata commits.

Instead, it may treat metadata commits as exchange checkpoints that represent current shareable metadata state at the time of synchronization.

That means:

- several local metadata mutations may collapse into one serialized metadata commit
- one source-code push may publish metadata about many commits, paths, or branches
- materialization may update metadata for objects that were already present locally
- metadata history is primarily about convergence of current state, not preserving the same narrative shape as code history

## GitButler- or Jujutsu-style integrations

Higher-level systems such as GitButler or Jujutsu may be especially good fits because they already mediate user workflows around sync, history movement, and state materialization.

Possible patterns include:

- attaching metadata sync to an existing "push branch" or "sync workspace" action
- materializing metadata after remote updates are incorporated into the local view
- serializing metadata for virtual branches, change IDs, or other higher-level targets before publish
- using host-managed remotes or destination rules to decide where metadata should go

The important part is not the exact UI shape, but that the host tool treats gmeta as low-level plumbing and coordinates it at the same moments users already think about collaboration.

## Multiple destinations

An implementation may support multiple metadata destinations, such as:

- public metadata
- organization-internal metadata
- personal/private metadata

In that case, the host system may:

- maintain separate local metadata heads per destination
- apply key-based or namespace-based publish filters on serialize
- fetch and materialize multiple remote metadata heads on pull
- present the resulting local view as a union, subject to implementation policy

This is implementation-specific orchestration and does not change the core exchange format.

## Failure handling

The host tool should surface metadata exchange failures clearly.

Examples:

- code push succeeded but metadata push failed
- metadata fetch succeeded but materialization failed
- a no-common-ancestor metadata merge required retry logic
- a destination filter excluded some local metadata from publication

Depending on product goals, the tool may treat metadata sync as:

- best effort
- required for successful collaboration
- required only for specific metadata namespaces

## Minimal contract

A host VCS integration should preserve the following mental model:

- local metadata is edited in an efficient local store
- **serialize on push** publishes shareable metadata state
- **materialize on pull** imports and merges remote metadata state
- exchange uses Git-native objects and refs
- higher-level workflow policy belongs to the integrating tool, not the wire format
