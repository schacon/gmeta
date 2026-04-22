# Standard keys

This document suggests a small set of commonly useful metadata keys.

These are not mandatory exchange-format rules. They are implementation-level recommendations intended to improve interoperability between tools that use git-meta.

## Agent metadata

Agent-generated metadata is likely to be common, so a stable namespace is useful.

These keys can be attached to `commit`, `change-id` or `branch` targets.

Implementations should avoid storing secrets in agent metadata keys.

### `agent:provider`

- suggested type: `string`
- intended meaning: service or runtime provider that produced the content
- examples:
  - `openai`
  - `anthropic`
  - `local`

### `agent:model`

- suggested type: `string`
- intended meaning: model identifier used for generation or analysis
- examples:
  - `gpt-5`
  - `claude-sonnet-4`
  - `llama-3.3-70b`

### `agent:session-id`

- suggested type: `string`
- intended meaning: provider or tool session identifier
- suggested format:
  - opaque stable string from the originating system

### `agent:prompt`

- suggested type: `string`
- intended meaning: canonical prompt or final instruction associated with the target

### `agent:summary`

- suggested type: `string`
- intended meaning: human-readable summary of what the agent did or concluded

## Naming recommendations

Keys should use a stable namespace-like structure with `:` separators.

Recommended conventions:

- use short, lowercase segments
- use a broad domain first, then more specific segments
- prefer singular nouns for scalar values
- reserve plural concepts for collection-typed keys
- keep the same key meaning across all target types when possible

## Type recommendations

A suggested key should also imply a default value type.

General guidance:

- use `string` for one canonical value
- use `list` for ordered history, logs, or preference-ranked values
- use `set` for unordered unique membership

## Format recommendations

Where possible, string values should use simple portable text formats:

- timestamps as RFC 3339 / ISO 8601 UTC strings
- UUIDs in canonical lowercase hyphenated form
- commit IDs as full Git object IDs unless a host system has a stronger stable identifier
- enums as short lowercase tokens like `pending`, `success`, `failed`

If a value needs structured payloads, implementations should prefer:

1. multiple related keys
2. a collection type
3. a stable serialized string format such as JSON only when necessary
