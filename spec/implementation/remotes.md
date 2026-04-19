# Remote Management

Some implementation examples of how we would ideally do remote mangement and serialization/materialization coordination.

## Adding a remote meta source

The workflow starts by adding a remote source. This could be automatically done by GitButler if we see it's setup (via local .gitmeta or something).

```
$ git meta remote add (url)
```

Could `ls-remote` the server and look for `refs/meta/*`, but should generally be `refs/meta/main`. Setup a fetch spec.

```
[remote "meta"]
        url = git@github.com:schacon/entire-meta.git
        fetch = +refs/meta/main:refs/meta/remote/main
        meta = true
        serialize = main[:refs/meta/main]
        promisor = true
        partialclonefilter = blob:none
```

We will use the fetchspec for mapping remote ref to local ref. If there are multiple fetchspecs, we have multiple sources on one remote (a history branch).

We'll add the `meta` boolean so it's easy to see which are our meta sources. The URL _could_ be the same as an "origin" remote, but we'll keep it separate anyhow.

We also add the `serialize = [name]` entry so we know where to write serialized values. If you have serialization filters writing to different destinations (ie, `internal` or `mine`), we will need this value to know to which remote to push them. Each serialized local ref head should have a remote entry with a matching value. This is also how we construct the push refspec.

After checking the source and setting up the Git remote, we do an initial blobless fetch of the ref.

```
git fetch --filter=blob:none meta refs/meta/main:refs/meta/remotes/main
```

Next we need to do the equivalent of a `git checkout` on that head, so Git will do the promisor remote "want" conversation to get everything in the tip tree.

The best way to do this that I can find is to get all the blobs with `ls-tree` and pipe the list into `fetch` (with some complicated options) which seems to do what we want.

```
git ls-tree -r --object-only meta/remotes/main | git -c fetch.negotiationAlgorithm=noop fetch origin --no-tags --no-write-fetch-head --recurse-submodules=no --filter=blob:none --stdin
```

Now we have the tip tree data and can do some fast metadata lookups for recent stuff. If we need to get other blobs, we can do the same basic trick - figure out the list of blobs you need from the commit tree history, run them through `fetch` to get a packfile of them.

It doesn't have to be a top level tree, we could look up any set of blob values we want and send them to `fetch` this way. Imagine wanting all the metadata for a range of commits - we walk the history past prune metadata commits to the last times we've seen any of these SHAs and walk the subtress to see what all blobs are referenced in any of them and then _just_ ask for those dozens of content blobs.

## Pushing and Pulling

Eventually we'll need to incorporate some automatic version of this into GitButler itself, but as a mid-level plumbing solution, we can do a `git meta push` and `git meta pull` that could be called by something else (like Git hooks or whatever).

### Pushing

So `git meta push` would rely on the `fetch` and `serialize` config values on the `meta` tagged remote (there should only be one, but fallback would be to choose the first one).

The simplest outcome of a `git meta push` would be to serialize a new tree and commit on the metadata history and push it upstream as a fast-forward.

The more complex case is that there is data we have not seen yet upstream, so we need to pull that down, serialize our own tree, merge the trees, materialize the outcome, then write a new tree and commit on top and try to push again. If we weren't fast enough and there is new data upstream again, we repeat. It should _always_ result in a single new commit written locally, even if we had to try several times.

### Pulling

A `git meta pull` should simply do the first part of the complex push process. Fetch the new data, serialize our side if we have new data and use Git to merge the trees with `ours` strategy, then materialize the new tree locally.

### Serializing for Push

We may want to keep our last serialized commit locally, but if we go to write another one and the last one was ours and unpushed, rewrite it. Always keep and push the minimum number of new commits necessary.

### Serialize Commit Messages

Serialization commits encode a diffstat of the changes they introduce. This allows fast key-list compilation from commit history without fetching any blob data — critical for blobless clones where the tree and commit objects are cheap but blob fetches are expensive. Walking the commit messages to reconstruct which keys exist is orders of magnitude faster than materializing trees.

There are two commit message formats:

**Normal** (up to 1000 changes):

```
git-meta: serialize (3 changes)

A	commit:abc123...	agent:model
M	commit:abc123...	agent:cost
D	project	meta:old-key
```

Each change line is: `A`/`M`/`D` (add/modify/delete), a tab, the target, a tab, the key.

**Large** (over 1000 changes):

```
git-meta: serialize (5432 changes)

changes-omitted: true
count: 5432
```

When the change count exceeds 1000, the individual lines are omitted to keep commit objects small. Consumers should fall back to tree diffing for these commits.

## Removing a remote meta source

A user may want to get rid of a meta source they are no longer using.

`git meta remote remove [name]`

It should remove the `.git/config` entry for that remote, any `refs/meta/local/*` and any `refs/meta/remote/*` pointers.
