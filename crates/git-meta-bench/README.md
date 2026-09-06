# git-meta-bench

Scale benchmarks for git-meta. Not published; `cargo run -p git-meta-bench` only.

It generates synthetic metadata histories and measures the things that decide
whether the design survives a real project's lifetime:

- **write cost** as the key space and the commit history grow — and, by
  comparing `grow` against `churn` mode, which of the two is responsible
- **read cost** from the local SQLite store and from the serialized git tree
- **prune settings** — what each retention window costs and keeps, across both
  prune implementations
- **sparse fetch** — what a blobless consumer pays to pull and to read values,
  including values that prune has moved out of the tip tree

## Running

```
cargo run --release -p git-meta-bench -- all --tiers 500,2000,8000
```

Subcommands: `scale`, `prune`, `sparse`, `all`. Useful flags:

| flag | meaning |
| --- | --- |
| `--tiers` | metadata commit counts to test |
| `--value-bytes` | size of each string value, when `--value-sizes` is not given |
| `--value-sizes` | value size distribution as `weight:bytes` pairs, e.g. `90:64,8:1024,2:16384` |
| `--steady N` | after generating, measure N further single-target publishes |
| `--list-entries` | list entries appended per target |
| `--prune-every N` | prune every N commits while generating, so old values end up genuinely deep in history |
| `--span-days` | simulated span of the history, so retention windows mean something |
| `--no-pack` | skip `git gc`, which dominates runtime on large tiers |
| `--json PATH` | write the full result set as JSON |
| `--keep` | keep the generated repositories for inspection |
| `--no-churn` | skip the churn control tier |

Timestamps are simulated: generation pins each write to a point in the
`--span-days` window, and prune cutoffs are measured back from the end of that
window rather than from the wall clock. Runs are deterministic for a given
configuration.

## Steady state

The generation curve shows what building a history costs on average. What a tool
actually feels is the cost of *one more* change once the repository is already
full, which is what `--steady` measures: it makes N further single-target
publishes against the finished history and reports write and serialize
latencies.

## Consumer phases

The sparse scenario reports where a consumer's time goes between an empty
directory and a usable store: clone, metadata fetch, tip-blob hydration,
materialize (recreating every key in SQLite), and history indexing. `pull` does
all of these in one call, so the breakdown is measured on a second identical
clone with the steps run separately.

## Modes

`grow` writes every commit's keys to new targets, so keys and commits grow
together — the shape a real project has. `churn` rewrites a fixed pool of
targets, holding the live key count flat while history grows. Comparing the two
separates "long history is expensive" from "wide key space is expensive".
