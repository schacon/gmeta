# git metadata project

This is a proposed specification for a new standardized way to attach and exchange arbitrary metadata in Git projects.

## Why this project exists

> [!YOUTUBE]
> https://www.youtube.com/watch?v=46bV6KT0SsQ&list=PLOU2XLYxmsILM5cRwAK6yKdtKnCK6Y4Oh&index=4
> Rodrigo Bovendorp's metadata talk at JJCon 2025, outlining the overall problem set this project is meant to address.

This is a proposal to provide a more flexible and scalable metadata system for Git projects than the tools Git commonly uses today such as `git notes`, commit headers, message trailers, and checked-in helper files like `CODEOWNERS`.

### Limitations of Current Solutions

These existing approaches tend to have some important limitations:

- limited control over metadata granularity and mutability
- poor support for many independently addressable fields on the same object
- awkward merge behavior for structured data
- file pollution when metadata has to live in the working tree
- poor scaling characteristics for very large metadata sets

### Metadata Use Cases

This project is meant to more elagantly support a wider variety of metadata use cases such as:

- provenance for generated code, prompts, and transcripts
- trust and review information
- comments and review discussions
- testing results and attestations
- path- or project-scoped metadata like ownership or policy hints

### Overall Goals

The core idea is to:

- support more fine grained metadata targets (branches, paths, etc)
- store metadata locally in a format that is fast to query and mutate
- exchange metadata using normal Git trees, commits, refs, and transport mechanisms
- model mergeable data as many small tree entries instead of large structured blobs

That combination is meant to make metadata:

- easy to read and write locally
- exchangeable over existing Git hosting infrastructure
- eventually consistent across collaborators
- practical even when metadata grows large

## Approach

This project has two parts. One is specifying an agreed upon exchange format that accomplishes these goals. The other is a local reference implementation that can be used, referenced or built upon.

## Exchange format

The important part is the exchange format - how metadata is stored, transmitted and updated among teams working on a project. This is what would need to be agreed upon for wider interop.

We refer to this as "serialization" - how to write data into Git primiatives for exchange, and "materialization" - how to update local values when receiving new data.

- [Targets and keys](./exchange-format/targets.md)
- [Exchange format and refs](./exchange-format/exchange.md)
- [Materialization and merge workflow](./exchange-format/materialization.md)

### Value types

There are three initial data types that can be used for any given target key. Here are the specifications for how those data types are serialized.

- [Strings](./exchange-format/strings.md)
- [Lists](./exchange-format/lists.md)
- [Sets](./exchange-format/sets.md)

## Implementation

Finally, there is a reference implementation for how one might actually use this concept locally in higher level version control tooling. As long as the exchange format and semantics are followed, local implementation isn't highly important, but it may be valuable to see a practical application.

- [Local storage](./implementation/storage.md)
- [CLI surface](./implementation/cli.md)
- [Output and query semantics](./implementation/output.md)
- [Workflow](./implementation/workflow.md)

### Key Naming Standards

If this were to be widely used, it would be nice to generally agree on how to name common keys, so we're not all reinventing slightly different wheels. We've put together a list of standardized key naming suggestions for common problem sets.

- [Standard keys](./implementation/standard-keys.md)
