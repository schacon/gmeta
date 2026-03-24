# gmeta spec

This directory breaks the project spec into smaller documents so we can evolve the design before or alongside implementation.

## Why this project exists

gmeta is trying to provide a more flexible and scalable metadata system for Git projects than the tools Git commonly uses today, especially `git notes`, commit headers, trailers, or checked-in helper files like `CODEOWNERS`.

Those existing approaches tend to have some important limitations:

- limited control over metadata granularity and mutability
- poor support for many independently addressable fields on the same object
- awkward merge behavior for structured data
- file pollution when metadata has to live in the working tree
- poor scaling characteristics for very large metadata sets

The project is motivated by metadata use cases such as:

- trust and review information
- provenance for generated code, prompts, and transcripts
- comments and review discussions
- testing results and attestations
- path- or project-scoped metadata like ownership or policy hints

The core idea is:

- store metadata locally in a format that is fast to query and mutate
- exchange metadata using normal Git trees, commits, refs, and transport
- model mergeable data as many small tree entries instead of one large structured blob

That combination is meant to make metadata:

- easy to read and write locally
- exchangeable over existing Git hosting infrastructure
- eventually consistent across collaborators
- practical even when metadata grows large

## Exchange format

- [Shared principles](./exchange-format/principles.md)
- [Targets and keys](./exchange-format/targets.md)
- [Exchange format and refs](./exchange-format/exchange.md)
- [Materialization and merge workflow](./exchange-format/materialization.md)

### Value types

- [Strings](./exchange-format/strings.md)
- [Lists](./exchange-format/lists.md)
- [Sets](./exchange-format/sets.md)

Ordered lists are intentionally not covered yet.

## Implementation

- [Local storage](./implementation/storage.md)
- [Standard keys](./implementation/standard-keys.md)
- [CLI surface](./implementation/cli.md)
- [Output and query semantics](./implementation/output.md)
- [Workflow](./implementation/workflow.md)

## Design status

These documents are proposal drafts. They are intended to answer:

- what is the serialized tree shape?
- what is the current value derived from that shape?
- how do conflicts resolve?
- what happens in no-common-ancestor scenarios?
- what tombstones are needed?

They are not yet implementation docs.
