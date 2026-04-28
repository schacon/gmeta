# Implementation plan: GitHub import and PR blame

This plan covers two related commands:

- `git meta import gh`: import merged GitHub pull request metadata into git-meta.
- `git meta blame <path>`: show file blame grouped by the pull request/branch metadata that introduced each line.

The current CLI already has a hidden `git meta import --format <entire|git-ai>` command in `crates/git-meta-cli/src/commands/import.rs`. The GitHub importer should reuse that import plumbing where possible, but expose the requested porcelain spelling as a first-class subcommand.

## Goals

- Import enough GitHub PR metadata to answer "which PR introduced this line?" without reaching back to GitHub at blame time.
- Keep imports idempotent: running the command again with no new merged PRs or tags should write no metadata.
- Prefer git-meta's existing metadata store over a separate side database unless a later performance pass proves it is needed.
- Keep the GitHub-specific code isolated from the core library so non-GitHub users do not inherit API or dependency assumptions.
- Start with a useful non-interactive blame output, then layer on the interactive TUI.

## Non-goals for the first pass

- Reconstructing a full evolog across force-pushes.
- Importing CI status beyond reserving the key shape.
- Supporting non-GitHub forges.
- Depending on the GitHub API at blame time.

## CLI shape

Add structured import args in `crates/git-meta-cli/src/cli.rs`:

- Replace or extend the hidden `Import { format, dry_run, since }` variant with an `Import(ImportArgs)` subcommand.
- Add `ImportAction::Gh` so `git meta import gh` parses naturally.
- Keep `git meta import --format git-ai` and `git meta import --format entire` as hidden compatibility shims if desired, but route them through the same enum internally.

Suggested options for `git meta import gh`:

- `--dry-run`: print planned writes without mutating the metadata store.
- `--limit <n>`: cap PRs fetched/imported for testing and first runs.
- `--since <YYYY-MM-DD>`: optional lower bound for merged PRs.
- `--repo <OWNER/NAME>`: override repository detection from `gh`.
- `--include-comments`: import issue comments and review body comments. This can default to true once payload size is understood.
- `--no-tags`: skip release tag mapping.

Add `git meta blame` as a visible porcelain command:

- `git meta blame <path>`: print PR-grouped blame to stdout.
- `git meta blame --porcelain <path>`: machine-readable JSON or line-delimited JSON.
- `git meta blame --no-tui <path>`: force non-interactive output when a TUI becomes the default on a TTY.
- `git meta blame --rev <rev> <path>`: pass a revision to the underlying blame command.

## Module layout

Add focused modules rather than growing `commands/import.rs` further:

- `crates/git-meta-cli/src/commands/import/mod.rs`: shared import dispatch and `ImportFormat` compatibility.
- `crates/git-meta-cli/src/commands/import/git_ai.rs`: existing git-ai import moved here.
- `crates/git-meta-cli/src/commands/import/entire.rs`: existing entire import moved here.
- `crates/git-meta-cli/src/commands/import/gh.rs`: GitHub importer orchestration.
- `crates/git-meta-cli/src/commands/blame.rs`: blame command orchestration and output.
- `crates/git-meta-cli/src/commands/blame/porcelain.rs`: parser for `git blame --porcelain`.
- `crates/git-meta-cli/src/commands/blame/group.rs`: grouping blame lines by PR/branch metadata.
- `crates/git-meta-cli/src/commands/blame/tui.rs`: interactive UI, gated behind the first working non-interactive implementation.

If the module split is too much for one change, add `commands/import/gh.rs` first and defer moving existing import formats.

## Metadata model

Use the standard keys from `spec/implementation/standard-keys.md` where they already exist.

Branch target: `branch:<branch-id>`

- `title` string: PR title.
- `description` string: PR body.
- `review:number` string: GitHub PR number.
- `review:url` string: canonical PR URL.
- `issue:id` set: referenced closing issues such as `GH-25` or `#25`.
- `review:comment` list: serialized comment records.
- `review:reviewed` set: reviewers who left feedback.
- `review:approved` set: reviewers who approved.
- `released-in` set: release tags or tag ranges that include the PR.

Commit target: `commit:<sha>`

- `branch-id` string: branch id derived from the PR head branch and number.
- `conventional:type` set: conventional commit type parsed from the commit subject.
- `released-in` set: release tags or tag ranges that include the commit.

Project target: `project`

- `github:repo` string: `OWNER/NAME` used for imports.
- `github:imported-pr` set: imported PR numbers or GraphQL node IDs.
- `github:imported-tag` set: imported tag names.
- `github:last-imported-merged-at` string: newest merged timestamp successfully imported.

Branch id format:

- Use `<headRefName>#<number>` as the logical id before storage.
- Sanitize only characters that conflict with target parsing or tree paths by using the repo's existing target/path encoding helpers where available.
- Store the original head branch name in a GitHub-specific key such as `github:head-ref` if the sanitized branch id is lossy.

Value types:

- Use `Store::set` for string keys.
- Use `Store::set_add` for sets like `issue:id`, `review:reviewed`, `review:approved`, `released-in`, and `conventional:type`.
- Use `Store::list_push` or encoded list helpers for `review:comment`.
- Store larger comments/descriptions through the existing git-ref large-value path used by the current import code.

## GitHub importer flow

1. Open `CommandContext`.
2. Verify `gh` exists and is authenticated by running `gh auth status`.
3. Resolve the GitHub repository:
   - Prefer `--repo`.
   - Otherwise ask `gh repo view --json owner,name,url,defaultBranchRef`.
   - Fail with an actionable message if `gh` cannot infer the repo.
4. Load project import state from the metadata store.
5. Fetch merged PRs in reverse chronological pages.
6. Stop scanning when a PR is already in `github:imported-pr`, unless `--since` or `--limit` says to keep scanning for a bounded backfill.
7. For each unimported PR, fetch full details:
   - title, body, number, URL, author, head ref, base ref, merged time.
   - closing issues.
   - commits and commit subjects.
   - reviews and review states.
   - issue comments, PR review comments, and review bodies.
8. Build an in-memory `GitHubPullRequestImport` struct with typed fields.
9. Convert it into a list of git-meta write operations.
10. Apply operations in deterministic timestamp order using the PR merged timestamp as the base timestamp.
11. Mark the PR imported only after all writes succeed.
12. Print a concise summary: fetched, imported, skipped, comments imported, commits annotated, errors.

Prefer GraphQL for the full PR detail fetch because it can retrieve reviews, comments, closing issues, and commits in one typed request. Keep the `gh` dependency at the process boundary by invoking `gh api graphql` and deserializing JSON into concrete Rust structs.

## Commit mapping

For each imported PR:

- Use GitHub's PR commit list for merge and rebase merges.
- Include `mergeCommit.oid` when present.
- For squash merges, map the merge commit to the PR even when the individual original commits are not present in local history.
- Verify every candidate object exists in the local repository before writing commit metadata.
- If a PR commit is missing locally, count it as skipped and continue.

Conventional commit parsing:

- Parse commit subjects with a small typed parser.
- Recognize `type(scope)!: subject` and `type!: subject`.
- Store only the type segment, plus `breaking` if `!` is present.
- Keep this parser unit-tested and independent of GitHub code.

## Release tag mapping

First pass:

- On each import run, list local tags by creation or target commit date.
- Load `github:imported-tag` from project metadata.
- For each new tag, find commits reachable from the tag and not reachable from the previous imported tag.
- For any commit in that range with a `branch-id`, add `released-in` to both the commit and its branch target.
- Mark the tag imported after all writes succeed.

Open decision:

- Whether `released-in` should be the tag name (`v1.2.3`) or a range (`v1.2.2..v1.2.3`). The existing `gh-import.md` says "tag range"; start by storing the tag name as the stable release identifier and add `github:released-range` later if range text is needed.

## Blame flow

1. Run `git blame --porcelain [--rev <rev>] -- <path>`.
2. Parse porcelain records into typed `BlameLine` values:
   - commit oid.
   - original line.
   - final line.
   - group length.
   - summary, author, author time, previous commit/path when present.
   - source text line.
3. Query git-meta for each unique commit's `branch-id`.
4. Query each branch target for `title`, `review:number`, `review:url`, `review:approved`, `review:reviewed`, and `released-in`.
5. Group adjacent blamed lines with the same branch id. If no branch id exists, group by commit oid.
6. Render non-interactive output:
   - line range.
   - branch id or abbreviated commit.
   - PR number/title when available.
   - author or reviewers when useful.
   - source text lines.
7. Add JSON output for tests and external consumers.

TUI second pass:

- Add `ratatui` and `crossterm` only when the non-interactive command is working.
- Left pane: file contents grouped by PR blocks.
- Right pane: selected PR metadata, comments, approvals, release info, and fallback commit data.
- Keys: up/down to move lines, page up/down, enter to expand PR details, `o` to open PR URL, `q` to quit.
- Disable TUI automatically when stdout is not a TTY.

## Error handling

- Missing `gh`: fail with `git meta import gh requires the GitHub CLI ('gh')`.
- Unauthenticated `gh`: fail with the relevant `gh auth login` hint.
- GitHub rate limit or partial page failure: fail before marking the current PR imported.
- Missing local commits: warn and continue.
- No metadata for blame: show commit-level blame with a hint to run `git meta import gh`.
- Non-UTF-8 blame path output: preserve bytes where possible; otherwise emit a clear error for the first implementation.

## Testing plan

Unit tests:

- GitHub JSON deserialization into typed structs.
- Closing issue extraction from PR body and GitHub issue references.
- Branch id construction and sanitization.
- Conventional commit parser.
- `git blame --porcelain` parser, including repeated commit records that omit metadata fields.
- Blame grouping by branch id and fallback commit id.

E2E tests:

- `git meta import gh --dry-run` with a fake `gh` executable on `PATH` returning fixture JSON.
- Idempotency: run importer twice against the same fixture and assert no second-run writes.
- Squash merge mapping: fixture PR with only `mergeCommit.oid` present locally.
- Blame output: create a small repo, write commit `branch-id` and branch PR metadata, run `git meta blame --porcelain <path>`, assert grouped PR output.
- Help tests: verify `import gh` and `blame` help are invokable and that visible/hidden help lists are updated intentionally.

Fixtures:

- Store GitHub API fixture JSON under `crates/git-meta-cli/tests/fixtures/gh/`.
- Keep fixtures small and hand-edited so expected metadata is obvious.
- Use a fake shell script for `gh` rather than network calls.

## Suggested implementation phases

1. Refactor import CLI dispatch just enough to support `git meta import gh`.
2. Add typed GitHub API process wrapper and fixture-driven deserialization tests.
3. Implement PR metadata writes without comments or tags.
4. Add PR comments, reviews, approvals, and issue references.
5. Add idempotency state and dry-run summaries.
6. Add release tag mapping.
7. Add non-interactive `git meta blame --porcelain` and text output.
8. Add TUI once the data model and grouping behavior are stable.

## Open questions

- Should imported GitHub state be serialized and shared as project metadata, or kept local-only under `meta:local:*` keys?
- Should `review:comment` list entries be raw markdown strings or structured JSON objects with author, URL, timestamp, and type?
- Should PR reviewers be stored as GitHub logins, display names, emails when available, or structured JSON?
- Should `git meta blame` default to TUI on TTY, or should TUI require an explicit `--tui` flag until it stabilizes?
- Should `git meta import gh` remain hidden until the importer has a full fixture-backed test matrix?
