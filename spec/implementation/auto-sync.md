# Syncing with normal Git

This document describes how an end user can set up a normal Git repository to automatically push and pull metadata whenever they push and pull code, using only commands the reference CLI already provides (`git meta push`, `git meta pull`, `git meta show`, `git meta log`) and Git's existing hook and alias mechanisms.

It is intentionally scoped to what works today with stock Git and the published reference CLI. Higher-level integrations that hide these moves entirely are covered in the [Workflow](workflow.md) document.

## What we are wiring up

Two operations need to happen alongside normal Git for metadata to feel "automatic":

- on push: after (or before) the code push, run `git meta push` so the local metadata commits travel to the remote
- on pull / fetch: after new remote refs land, run `git meta pull` so the local metadata database materializes the new metadata commits

Git provides hooks for some of these moments and not others. The recommended setup uses the hooks Git does provide and falls back to a small set of aliases for the cases it does not.

## Push side: `pre-push`

Git's `pre-push` hook runs on the client immediately before refs are sent. It receives the remote name and URL as arguments and the list of local-to-remote ref pairs on stdin. This is the natural place to push metadata refs alongside the code push.

Save the following as `.git/hooks/pre-push` and make it executable (`chmod +x`):

```sh
#!/usr/bin/env sh
remote_name="$1"

# Forward metadata to the same remote the user is pushing to.
# The metadata remote should already be configured via `git meta remote add`.
git meta push "$remote_name" || {
    echo "git meta push to '$remote_name' failed; aborting git push" >&2
    exit 1
}
```

Behavior notes:

- `pre-push` runs once per `git push` invocation regardless of how many refs are being pushed, which matches `git meta push`'s once-per-invocation semantics.
- Returning a non-zero exit code from `pre-push` aborts the entire `git push`. If you would rather the code push always succeed and metadata sync be best-effort, replace the `exit 1` with `:` (no-op) so failures are reported but do not block.
- If `git meta` is not aware of the remote name passed to the hook, it falls back to the first configured meta remote.

> [!NOTE]
> There is no `post-push` hook in core Git. `pre-push` is the only client-side hook in the push lifecycle.

## Pull side: `post-merge`

Git's `post-merge` hook runs after a successful merge, including the merge phase of `git pull`. This is the simplest way to materialize newly fetched metadata into the local database.

Save the following as `.git/hooks/post-merge` and make it executable:

```sh
#!/usr/bin/env sh
git meta pull || true
```

The trailing `|| true` keeps a metadata sync failure from interrupting the rest of the merge workflow. Drop it if you want metadata problems to surface loudly.

Behavior notes:

- `post-merge` only fires when `git pull` actually merges. If the pull is a no-op (already up to date), the hook does not run, and there is nothing new to materialize anyway.
- `post-merge` does not fire for `git pull --rebase` or `git rebase`. If you rebase by default, see the `reference-transaction` recipe below or use the alias approach.

## Fetch side: `reference-transaction`

Core Git does not provide a `pre-fetch` or `post-fetch` hook. The closest mechanism is `reference-transaction`, available since Git 2.28, which fires for every ref-update transaction including the one written by `git fetch`.

Save the following as `.git/hooks/reference-transaction` and make it executable:

```sh
#!/usr/bin/env sh
# This hook runs three times per transaction (`prepared`, `committed`, `aborted`).
# Only act once, after the transaction commits.
[ "$1" = "committed" ] || exit 0

# Read the ref updates from stdin and only react when remote-tracking refs
# changed (which is what `git fetch` writes). Skip otherwise to avoid
# materializing on every local commit, branch, or tag write.
if grep -q "	refs/remotes/" ; then
    git meta pull >/dev/null 2>&1 &
fi
```

Behavior notes:

- This hook is on the hot path for *every* ref update Git makes (including `git commit` writing `HEAD`). The `refs/remotes/` filter scopes the work to fetches.
- The `&` runs `git meta pull` in the background so it does not block the calling Git command. If you want synchronous behavior, drop the `&`.
- This hook makes `post-merge` redundant for users who only fetch via the same Git invocation. Pick one or the other unless you have a reason to layer both.

> [!WARNING]
> `reference-transaction` is a relatively low-level mechanism. If a script in this hook fails noisily it can disrupt unrelated Git operations. The redirects to `/dev/null` and the background `&` above are deliberate. Test the hook with `git fetch -v` before relying on it.

## Aliases as a simpler alternative

For users who prefer to avoid hooks entirely, Git aliases that combine the code and metadata operations work just as well. Add to `~/.gitconfig` (global) or `.git/config` (per-repo):

```ini
[alias]
    pushm  = "!git push \"$@\" && git meta push"
    pullm  = "!git pull \"$@\" && git meta pull"
    fetchm = "!git fetch \"$@\" && git meta pull"
```

Users then run `git pushm`, `git pullm`, or `git fetchm` instead of the bare commands. The advantages over hooks are:

- portable across machines without copying hook scripts
- explicit: only runs when the user opts in
- easy to debug because failures appear inline in the terminal

The disadvantage is that someone running plain `git push` or `git pull` will not get metadata sync. Hooks are appropriate when "always on" is the desired behavior.

## Reading metadata alongside code history

Core Git also does not expose hooks for `git log` or `git show`, and Git aliases cannot override built-in commands. The reference CLI ships two existing read commands that are intended to be used directly instead:

- `git meta show <commit>` displays the commit details and all metadata attached to that commit
- `git meta log [<ref>]` walks the commit log and shows metadata for each commit, with `--mo` to limit output to commits that actually have metadata

The recommended pattern is to use these as drop-in companions to `git show` and `git log` rather than trying to splice metadata into the built-in commands' output.

If you prefer the verbs `show` and `log` to live under your normal Git invocation, alias them:

```ini
[alias]
    showm = "!f() { git show \"$@\" && git meta show \"${1:-HEAD}\" ; }; f"
    logm  = "!git meta log"
```

Now `git showm <sha>` prints the standard `git show` output followed by the metadata view, and `git logm` is a shortcut for `git meta log`.

## Recommended starter setup

For a single user on a single machine that wants the round trip to feel automatic with the least moving parts:

1. install the CLI: `cargo install git-meta-cli`
2. configure the metadata remote once: `git meta remote add <url>`
3. install the `pre-push` and `post-merge` hooks above
4. add the `showm` and `logm` aliases above for read access

This covers `git push`, `git pull`, and inspection without any custom code. For `git fetch` without a follow-up merge, fall back to either the `reference-transaction` hook or the `fetchm` alias.

## What this setup does not do

These recipes are deliberately thin. They do not:

- decide which metadata namespaces are eligible to publish (the meta remote configuration handles that)
- coordinate metadata pushes with multiple code remotes
- present unified `git log` / `git show` output that interleaves metadata
- handle conflicts beyond what `git meta pull` already does

A higher-level VCS that wants any of those behaviors should follow the [Workflow](workflow.md) document instead, which describes the integration model for tools like GitButler or Jujutsu.
