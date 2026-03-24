# Output and query semantics

This document describes implementation-level query behavior and display choices.

The exchange format defines what data means on the wire; this document defines how a specific implementation may expose that data through commands like `gmeta get`.

## Query forms

`gmeta get` should support three basic forms:

```bash
gmeta get <target>
gmeta get <target> <key>
gmeta get <target> <partial-key>
```

Behavior:

- with only a target, return all visible keys for that target
- with an exact key, return that key if present
- with a partial key, return matching keys in that namespace subtree

## Human-readable output

Example:

```bash
❯ gmeta get commit:13a7d29cde8f8557b54fd6474f547a56822180ae
agent:model  claude-4.6
agent:provider  anthropic
```

Human-readable output should be stable and easy to scan, but exact formatting is an implementation detail.

## JSON output

`--json` should return a structured object representation.

Example:

```bash
❯ gmeta get --json commit:13a7d29cde8f8557b54fd6474f547a56822180ae
{
  "agent": {
    "model": "claude-4.6",
    "provider": "anthropic"
  }
}
```

## JSON with authorship

`--with-authorship` augments JSON values with provenance metadata for the last visible mutation.

Example:

```bash
❯ gmeta get --json --with-authorship commit:13a7d29cde8f8557b54fd6474f547a56822180ae
{
  "agent": {
    "model": {
      "value": "claude-4.6",
      "author": "schacon@gmail.com",
      "timestamp": 1771232450000
    },
    "provider": {
      "value": "anthropic",
      "author": "schacon@gmail.com",
      "timestamp": 1771232450000
    }
  }
}
```

## Collection output

Type-specific JSON output should be unsurprising:

- strings render as strings, or as `{ value, author, timestamp }` with authorship
- lists render as arrays in visible list order
- sets render as arrays in deterministic display order

The exact JSON envelope may evolve, but two rules should hold:

1. output should preserve enough structure to distinguish namespaces
2. collection ordering in output should be deterministic
