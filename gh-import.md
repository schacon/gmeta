# GitHub Meta

## PR data importer

Add a new command, `git meta import gh` that uses the `gh` command line tool and github api to import GitHub Pull Request data into the git-meta format.

### Imported Keys

- `[branch] title` - PR title
- `[branch] description` - PR description
- `[branch] review:number` - PR number
- `[branch] review:url` - PR url
- `[branch] issue:id` - if there is a `closes` comment
- `[branch] review:comment` - PR comments
- `[branch] review:reviewed` - people who left feedback / reviews
- `[branch] review:approved` - people who signed off
- `[branch] released-in` - tag range that commit / branch was released in
- `[commit] branch-id` - branch id for PR (branch name + pr number)
- `[commit] conventional:type` - if there was a `feat:` starting the commit
- `[commit] released-in` - tag range that commit / branch was released in
- CI information?

Running `git meta import gh` should use `gh` (if it's authenticated) to query the merged pull requests on github to import pull request data into git-meta.

Run through the merged PRs in reverse order until you find one that is already imported and stop.

For each unimported merged PR, find and import all of the keys listed above.

Begin by constructing a branch-id. This should be the actual branch name plus the PR number (so multiple instances of the same branch names are unique).

Then import title, description, number and URL targeting that branch-id. If you see a `closes #25` type format, add `issue:id` as well.

Pull down all PR comments and approvals and add them as `review:comment`, `review:reviewed` and `review:approved` fields.

For all commits merged/rebased/squashed in that PR, add the `[commit] branch-id` to it. If the commit subject follows conventional commits, add the `conventional:type` as well.

If there is a new tag in the Git codebase that has not been seen yet, find all the PRs merged between this tag and the last imported tag (or new tags in between that) and add the `[branch] released-in` field to each branch merged in between.

Save in a simple database the PRs and tags imported. Should be indempotent - running it again with no changes will do nothing.

(todo) If there were multiple force-pushes, construct the evolog properly.

## git meta blame

Add a `git meta blame file.c` command that runs `git blame` but instead of blaming to a commit, blames to the pull request that introduced the commit. An interactive TUI that can step through changes to a file on a PR basis rather than a commit basis, using `git meta` metadata.

Start by running a `git blame --porcelain [file]`, which should look something like this:

```
❯ git blame --porcelain Cargo.toml
84a1d9b840d428fc523f6ffc1f8adfb43ab5918d 1 1 1
author Kiril Videlov
author-mail <kiril@videlov.com>
author-time 1775564269
author-tz +0200
committer Kiril Videlov
committer-mail <kiril@videlov.com>
committer-time 1775564269
committer-tz +0200
summary refactor: split single crate into gmeta-core/gmeta-cli workspace
previous 893900ae45e592b5cea95d3d884cbe489274980e Cargo.toml
filename Cargo.toml
        [workspace]
90f597909422b0c54e3bcf00cd67b706e500c20e 2 2 1
author Scott Chacon
author-mail <schacon@gmail.com>
author-time 1776689954
author-tz +0200
committer Scott Chacon
committer-mail <schacon@gmail.com>
committer-time 1776764792
committer-tz +0200
summary Rename CLI crate to git-meta-cli
previous e8fc832d459566e80a0114a2b0a7c3b632245b7a Cargo.toml
filename Cargo.toml
        members = ["crates/git-meta-lib", "crates/git-meta-cli"]
84a1d9b840d428fc523f6ffc1f8adfb43ab5918d 3 3 1
        resolver = "2"
```

We want to make a blame TUI that pulls in this information, pulls PR data connected to the change-ids that are attached to the commits referenced and provides an alternative blame UX that shows the PR that was introduced per block of lines rather than the commit.
