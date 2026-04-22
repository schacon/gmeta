# Standard keys

This document suggests a small set of commonly useful metadata keys and conventions.

These are not mandatory exchange-format rules. They are implementation-level recommendations intended to improve interoperability between tools that use git-meta.

## Goals

Suggested standard keys should:

- cover common collaboration and automation use cases
- keep names predictable across tools
- make value types obvious from the key name
- avoid overloading one key with several unrelated meanings
- leave room for vendor-specific extensions

## Naming recommendations

Keys should use a stable namespace-like structure with `:` separators.

Recommended conventions:

- use short, lowercase segments
- kebab/dash case preferred (`branch-id` rather than `branchId` or `branch_id`)
- use a broad domain first, then more specific segments
- prefer singular nouns for scalar values
- reserve plural concepts for collection-typed keys
- keep the same key meaning across all target types when possible
- tool specific fields should top-level namespace (ie `gitbutler:` or `google:`)

Examples:

- `owner`
- `codeowners`
- `reviewer`
- `agent:model`
- `agent:provider`
- `agent:session-id`
- `agent:prompt`
- `automation:status`

## Type recommendations

A suggested key should also imply a default value type.

General guidance:

- use `string` for one canonical value
- use `list` for ordered history, logs, or preference-ranked values
- use `set` for unordered unique membership

Examples:

- `owner` → `string`
- `codeowners` → `set`
- `reviewer` → `set`
- `agent:model` → `string`
- `agent:message-id` → `list` when preserving ordered conversation/message references

## Format recommendations

Where possible, string values should use simple portable text formats:

- email addresses as plain email strings
- timestamps as RFC 3339 / ISO 8601 UTC strings
- UUIDs in canonical lowercase hyphenated form
- commit IDs as full Git object IDs unless a host system has a stronger stable identifier
- booleans as the literal strings `true` and `false` when a dedicated boolean type does not exist
- enums as short lowercase tokens like `open`, `closed`, `pending`, `success`

If a value needs structured payloads, implementations should prefer:

1. multiple related keys
2. a collection type
3. a stable serialized string format such as JSON only when necessary

## Commonly useful standard keys

The following are recommended cross-tool conventions.

### Ownership

#### `owner`

- suggested type: `string`
- intended meaning: one primary responsible person or team
- suggested formats:
  - email address: `alice@example.com`
  - team slug: `team:release`

Useful targets:

- `project`
- `branch`
- `path`
- `change-id`

#### `codeowners`

- suggested type: `set`
- intended meaning: all owners associated with a target
- suggested member formats:
  - email address
  - team slug like `team:platform`
  - Git hosting handle only if the hosting product is the intended authority

Useful targets:

- `path`
- `project`

This key is especially useful when materializing ownership data derived from or aligned with a `CODEOWNERS` file.

Tools should treat `codeowners` as advisory metadata, not as a replacement for repository-native enforcement rules.

### Review and collaboration

#### `reviewer`

- suggested type: `set`
- intended meaning: requested, assigned, or associated reviewers
- suggested member formats:
  - email address
  - team slug
  - provider-scoped identity like `github:user/octocat`

Useful targets:

- `change-id`
- `commit`
- `branch`

#### `review:status`

- suggested type: `string`
- intended meaning: high-level review state
- suggested values:
  - `unreviewed`
  - `in-review`
  - `approved`
  - `changes-requested`

Useful targets:

- `change-id`
- `commit`
- `branch`

### Agent metadata

Agent-generated metadata is likely to be common, so a stable namespace is useful.

#### `agent:provider`

- suggested type: `string`
- intended meaning: service or runtime provider
- examples:
  - `openai`
  - `anthropic`
  - `local`
  - `pi`

#### `agent:model`

- suggested type: `string`
- intended meaning: model identifier used for generation or analysis
- examples:
  - `gpt-5`
  - `claude-sonnet-4`
  - `llama-3.3-70b`

#### `agent:session-id`

- suggested type: `string`
- intended meaning: provider or tool session identifier
- suggested format:
  - opaque stable string from the originating system

#### `agent:message-id`

- suggested type: `list`
- intended meaning: ordered references to one or more related agent messages or turns
- suggested format:
  - opaque provider-issued IDs

#### `agent:prompt`

- suggested type: `string`
- intended meaning: canonical prompt, summary prompt, or final instruction associated with the target

#### `agent:summary`

- suggested type: `string`
- intended meaning: human-readable summary of what the agent did or concluded

Useful targets for agent keys:

- `commit`
- `change-id`
- `branch`
- `path`

Implementations should avoid storing secrets in agent metadata keys.

### Automation and CI

#### `automation:status`

- suggested type: `string`
- intended meaning: latest high-level automation state
- suggested values:
  - `pending`
  - `running`
  - `success`
  - `failed`

#### `automation:system`

- suggested type: `string`
- intended meaning: the system that produced the automation metadata
- examples:
  - `github-actions`
  - `buildkite`
  - `jenkins`

#### `automation:run-id`

- suggested type: `string`
- intended meaning: stable external run identifier

Useful targets:

- `commit`
- `change-id`
- `branch`

### Provenance and external references

#### `source:url`

- suggested type: `string`
- intended meaning: canonical URL for an external discussion, issue, task, or record

#### `source:id`

- suggested type: `string`
- intended meaning: external system identifier without requiring URL parsing

#### `tag`

- suggested type: `set`
- intended meaning: lightweight labels for filtering and discovery
- suggested member format:
  - lowercase tokens such as `bug`, `docs`, `release-blocker`

Useful targets:

- any target type

## Target-specific guidance

### Project targets

Good candidates for `project` metadata include:

- `owner`
- `codeowners`
- `source:url`

### Path targets

Good candidates for `path` metadata include:

- `owner`
- `codeowners`
- `tag`
- `agent:summary`

### Commit and change targets

Good candidates for `commit` and `change-id` metadata include:

- `reviewer`
- `review:status`
- `automation:status`
- `automation:run-id`
- `agent:provider`
- `agent:model`
- `agent:session-id`

### Branch targets

Good candidates for `branch` metadata include:

- `owner`
- `reviewer`
- `automation:status`
- `agent:summary`

## Extension guidance

Tools may define additional namespaced keys beyond these recommendations.

Suggested practice:

- use a product or organization prefix for private conventions
- avoid redefining the meaning of the standard keys above
- document any key whose format is not self-evident

Examples:

- `github:pr-number`
- `gitbutler:change-owner`
- `acme:policy:tier`

## Non-goals

This document does not:

- require all implementations to support all keys
- define access control or privacy policy
- define a schema registry
- prevent future dedicated value types beyond `string`, `list`, and `set`
