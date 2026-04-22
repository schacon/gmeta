# Standard keys

This document suggests a small set of commonly useful metadata keys.

These are not mandatory exchange-format rules. They are implementation-level recommendations intended to improve interoperability between tools that use git-meta.

> [!NOTE]
> To propose a new standard key, change to an existing one, or report an issue with this list, [open a GitHub issue](https://github.com/git-meta/git-meta/issues/new?labels=standard-keys&title=Standard+keys%3A+).

## Metadata Keys

### Agents

Agent-generated metadata is likely to be common, so a stable namespace is useful.

These keys can be attached to `commit`, `change-id` or `branch` targets.

Implementations should avoid storing secrets in agent metadata keys.

```key agent:provider
type: string
meaning: service or runtime provider that produced the content
examples:
  - openai
  - anthropic
  - local
```

```key agent:model
type: string
meaning: model identifier used for generation or analysis
examples:
  - gpt-5
  - claude-sonnet-4
  - llama-3.3-70b
```

```key agent:session-id
type: string
meaning: provider or tool session identifier
format: opaque stable string from the originating system
```

```key agent:prompt
type: string
meaning: canonical prompt or final instruction associated with the target
```

```key agent:summary
type: string
meaning: human-readable summary of what the agent did or concluded
```

```key agent:transcript
type: list
meaning: ordered record of the agent session, one message per list item
format: each item is a single JSON Lines (JSONL) record encoding one message (role, content, and any tool calls)
```

### Commit Data

These keys cover common commit/change level metadata that often needs to live alongside or after the commit itself.

These keys can generally be attached to `commit`, or `change-id` targets, though some only make sense on `commit` as it's contents cannot change (such as signoffs).

```key branch-id
type: string
meaning: stable identifier of the branch the commit originated on, preserved across squash, rebase, or merge operations that would otherwise erase the source branch name
```

```key signed-off
type: set
meaning: identities that have signed off on the commit (e.g. DCO sign-offs, legal or security approvals)
format: each entry is `Name <email>` or another stable identifier for the signer
```

```key review:reviewed
type: set
meaning: identities of users that reviewed the change
format: each entry is `Name <email>` or another stable identifier for the signer
```

```key review:approved
type: set
meaning: identities that approved the change (a subset of reviewers with an explicit approve verdict)
format: each entry is `Name <email>` or another stable identifier for the signer
```

```key conventional:type
type: set
meaning: conventional commit style high-level classification of the change
examples:
  - feat
  - fix
  - docs
  - refactor
  - chore
  - breaking
```

```key released-in
type: set
meaning: release tags or versions that introduced this change
```

```key attestation
type: list
meaning: attestations describing how the content was built or verified, using something like gitsign
format: each list item is an Signed JSON (DSSE) envelope
```

### Branch Data

These keys cover common branch level metadata that describes the purpose, lifecycle, and integration state of a branch.

These keys are attached to `branch` targets.

```key description
type: string
meaning: human-readable description of the branch's purpose
```

```key status
type: string
meaning: lifecycle state of the branch
examples:
  - draft
  - ready
  - in-review
  - merged
  - abandoned
  - archived
```

```key review:url
type: string
meaning: canonical pull-request, merge-request, or change-list URL or identifier associated with the branch
```

```key ci:url
type: string
meaning: canonical URL of the most recent CI run for the branch
```

```key issue:id
type: set
meaning: external issue or ticket references the branch addresses
examples:
  - GH-1234
  - JIRA-5678
```

```key issue:url
type: set
meaning: canonical URLs for issues this branch addresses
```

```key label
type: set
meaning: free-form tags or labels applied to the branch
examples:
  - security
  - infra
  - experiment
```

### Path Data

These keys describe ownership, provenance, and documentation for a file or directory path in the project. They are useful for replacing checked-in helper files like `CODEOWNERS` and for annotating vendored or imported code.

These keys are attached to `path` targets, but most can also be attached to project if they are the default for all paths.

```key owner
type: set
meaning: identities or teams that own the path
format: each entry is `Name <email>`, a team handle, or another stable identifier
```

```key reviewer
type: set
meaning: identities or teams that should be automatically requested to review changes to this path
format: each entry is `Name <email>`, a team handle, or another stable identifier
```

```key description
type: string
meaning: human-readable description of what the file or directory is for
```

```key topic
type: set
meaning: domain topics this path relates to
examples:
  - auth
  - billing
  - search
```

```key license
type: string
meaning: SPDX license identifier for paths whose license differs from the project default
examples:
  - Apache-2.0
  - MIT
  - GPL-3.0-or-later
```

```key upstream:url
type: string
meaning: canonical origin URL for vendored or imported code at this path
```

```key upstream:version
type: string
meaning: pinned upstream version, tag, or commit identifier for vendored or imported code
```

```key docs:url
type: string
meaning: canonical documentation URL for this module or path
```

```key agent:instructions
type: string
meaning: persistent instructions, system prompt, or rules provided to the agent when working on this path
```

### Project Data

These keys cover project-wide metadata that applies to the repository as a whole.

These keys are attached to `project` targets.

```key description
type: string
meaning: human-readable description of the project
```

```key homepage:url
type: string
meaning: canonical URL for the project's homepage
```

```key repository:url
type: string
meaning: canonical URL of the project's source repository
```

```key license
type: string
meaning: SPDX license identifier for the project
examples:
  - Apache-2.0
  - MIT
  - GPL-3.0-or-later
```

The `meta:` namespace is reserved for git-meta protocol configuration (auto-pruning, serialization filters, and similar tooling rules). Project metadata that is not protocol configuration should use a different namespace.

```key meta:prune:since
type: string
meaning: retention window used when auto-pruning is triggered during serialization
format: ISO-8601 date (e.g. `2025-01-01`) or relative duration (`90d`, `6m`, `1y`)
```

```key meta:prune:max-keys
type: string
meaning: integer threshold; when the total number of metadata keys in the serialized tree exceeds this value, an auto-prune is triggered
examples:
  - "10000"
```

```key meta:prune:max-size
type: string
meaning: size threshold; when the total size of all blobs in the serialized tree exceeds this value, an auto-prune is triggered
format: integer with optional human-friendly suffix (`512k`, `10m`, `1g`)
```

```key meta:prune:min-size
type: string
meaning: minimum subtree size threshold passed through to the prune operation; target subtrees smaller than this are kept in full regardless of age
format: integer with optional human-friendly suffix (`512k`, `10m`)
```

```key meta:filter
type: set
meaning: shared serialization filter rules that exclude keys from serialization or route them to alternative refs
format: each entry is `<action> <pattern> [<destination>]` where action is `exclude` or `route`, pattern is a `:`-segmented glob, and destination is required for `route`
examples:
  - exclude draft:**
  - route myteam:** private
```

```key meta:local:filter
type: set
meaning: personal serialization filter rules that are themselves never serialized; same format as `meta:filter`
format: each entry is `<action> <pattern> [<destination>]`
examples:
  - exclude scratch:**
  - route myname:** mine
```

## Format Recommendations

### Naming

Keys should use a stable namespace-like structure with `:` separators.

Recommended conventions:

- use short, lowercase segments
- use a broad domain first, then more specific segments
- prefer singular nouns for scalar values
- reserve plural concepts for collection-typed keys
- keep the same key meaning across all target types when possible

### Format

Where possible, string values should use simple portable text formats:

- timestamps as RFC 3339 / ISO 8601 UTC strings
- UUIDs in canonical lowercase hyphenated form
- commit IDs as full Git object IDs unless a host system has a stronger stable identifier
- enums as short lowercase tokens like `pending`, `success`, `failed`

If a value needs structured payloads, implementations should prefer:

1. multiple related keys
2. a collection type
3. a stable serialized string format such as JSON only when necessary
