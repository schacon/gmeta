use predicates::prelude::*;

use crate::harness;

#[cfg(not(windows))]
#[test]
fn import_gh_imports_pr_metadata_and_is_idempotent() {
    use std::os::unix::fs::PermissionsExt;

    let (dir, sha) = harness::setup_repo();
    let fake_bin = tempfile::TempDir::new().unwrap();
    let gh_path = fake_bin.path().join("gh");
    let script = format!(
        r#"#!/bin/sh
if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "list" ]; then
  case "$*" in
    *closingIssuesReferences*|*comments*|*commits*|*reviews*)
      echo 'unexpected nested field in pr list' >&2
      exit 1
      ;;
  esac
  cat <<'JSON'
[{{"number":42,"title":"Add metadata import","body":"Closes #25","url":"https://github.com/owner/repo/pull/42","headRefName":"feature/import","baseRefName":"main","mergedAt":"2026-04-01T12:00:00Z","mergeCommit":{{"oid":"{sha}","messageHeadline":"feat(import): add GitHub import"}}}}]
JSON
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "view" ]; then
  cat <<'JSON'
{{"number":42,"title":"Add metadata import","body":"Closes #25","url":"https://github.com/owner/repo/pull/42","headRefName":"feature/import","baseRefName":"main","mergedAt":"2026-04-01T12:00:00Z","mergeCommit":{{"oid":"{sha}","messageHeadline":"feat(import): add GitHub import"}},"commits":[{{"oid":"{sha}","messageHeadline":"feat(import): add GitHub import"}}],"comments":[{{"author":{{"login":"alice"}},"body":"looks good","url":"https://github.com/owner/repo/pull/42#issuecomment-1","createdAt":"2026-04-01T11:00:00Z"}}],"reviews":[{{"author":{{"login":"bob"}},"state":"APPROVED","body":"approved","url":"https://github.com/owner/repo/pull/42#pullrequestreview-1","submittedAt":"2026-04-01T11:30:00Z"}}]}}
JSON
  exit 0
fi
echo "unexpected gh args: $@" >&2
exit 1
"#,
    );
    std::fs::write(&gh_path, script).unwrap();
    let mut permissions = std::fs::metadata(&gh_path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&gh_path, permissions).unwrap();

    let path = format!(
        "{}:{}",
        fake_bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    git(dir.path(), &["tag", "v1.0.0", &sha]);
    harness::git_meta(dir.path())
        .args(["set", &format!("commit:{sha}"), "change-id", "Iabcdef"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .env("PATH", &path)
        .args(["import", "gh", "--repo", "owner/repo", "--limit", "1"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "fetching up to 1 merged PRs with `gh pr list`",
        ))
        .stderr(predicate::str::contains("fetching PR #42 details"))
        .stderr(predicate::str::contains("imported 1 PRs"));

    harness::git_meta(dir.path())
        .args(["get", &format!("commit:{sha}"), "branch-id"])
        .assert()
        .success()
        .stdout(predicate::str::contains("feature-import#42"));

    harness::git_meta(dir.path())
        .args(["get", "branch:feature-import#42"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Add metadata import"))
        .stdout(predicate::str::contains("review:approved"))
        .stdout(predicate::str::contains("bob"))
        .stdout(predicate::str::contains("commits:author"))
        .stdout(predicate::str::contains("Test User <test@example.com>"))
        .stdout(predicate::str::contains("commits:author-date"))
        .stdout(predicate::str::contains("946684800"))
        .stdout(predicate::str::contains("released-in"))
        .stdout(predicate::str::contains("v1.0.0"))
        .stdout(predicate::str::contains("issue:id"))
        .stdout(predicate::str::contains("#25"));

    harness::git_meta(dir.path())
        .args(["get", "change-id:Iabcdef", "released-in"])
        .assert()
        .success()
        .stdout(predicate::str::contains("v1.0.0"));

    harness::git_meta(dir.path())
        .env("PATH", path)
        .args(["import", "gh", "--repo", "owner/repo", "--limit", "1"])
        .assert()
        .success()
        .stderr(predicate::str::contains("imported 0 PRs"));

    let path = format!(
        "{}:{}",
        fake_bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    harness::git_meta(dir.path())
        .env("PATH", path)
        .args([
            "import",
            "gh",
            "--repo",
            "owner/repo",
            "--limit",
            "1",
            "--force",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("imported 1 PRs"));
}

#[cfg(not(windows))]
#[test]
fn import_gh_without_limit_fetches_all_pages() {
    use std::os::unix::fs::PermissionsExt;

    let (dir, _sha) = harness::setup_repo();
    let fake_bin = tempfile::TempDir::new().unwrap();
    let gh_path = fake_bin.path().join("gh");
    let script = r#"#!/bin/sh
if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  exit 0
fi
if [ "$1" = "api" ] && [ "$2" = "graphql" ]; then
  case "$*" in
    *cursor=CURSOR1*)
      cat <<'JSON'
{"data":{"repository":{"pullRequests":{"nodes":[{"number":100,"title":"Older PR","body":"","url":"https://github.com/owner/repo/pull/100","headRefName":"older","baseRefName":"main","mergedAt":"2026-03-31T12:00:00Z","mergeCommit":null}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}
JSON
      ;;
    *)
      cat <<'JSON'
{"data":{"repository":{"pullRequests":{"nodes":[{"number":101,"title":"Newer PR","body":"","url":"https://github.com/owner/repo/pull/101","headRefName":"newer","baseRefName":"main","mergedAt":"2026-04-01T12:00:00Z","mergeCommit":null}],"pageInfo":{"hasNextPage":true,"endCursor":"CURSOR1"}}}}}
JSON
      ;;
  esac
  exit 0
fi
if [ "$1" = "pr" ] && [ "$2" = "view" ]; then
  if [ "$3" = "101" ]; then
    cat <<'JSON'
{"number":101,"title":"Newer PR","body":"","url":"https://github.com/owner/repo/pull/101","headRefName":"newer","baseRefName":"main","mergedAt":"2026-04-01T12:00:00Z","mergeCommit":null,"commits":[],"comments":[],"reviews":[]}
JSON
    exit 0
  fi
  if [ "$3" = "100" ]; then
    cat <<'JSON'
{"number":100,"title":"Older PR","body":"","url":"https://github.com/owner/repo/pull/100","headRefName":"older","baseRefName":"main","mergedAt":"2026-03-31T12:00:00Z","mergeCommit":null,"commits":[],"comments":[],"reviews":[]}
JSON
    exit 0
  fi
fi
echo "unexpected gh args: $@" >&2
exit 1
"#;
    std::fs::write(&gh_path, script).unwrap();
    let mut permissions = std::fs::metadata(&gh_path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&gh_path, permissions).unwrap();

    let path = format!(
        "{}:{}",
        fake_bin.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    harness::git_meta(dir.path())
        .env("PATH", path)
        .args([
            "import",
            "gh",
            "--repo",
            "owner/repo",
            "--dry-run",
            "--no-tags",
        ])
        .assert()
        .success()
        .stderr(predicate::str::contains("fetching merged PR page 1"))
        .stderr(predicate::str::contains(
            "fetched merged PR page 2: 1 PRs (2 total)",
        ))
        .stderr(predicate::str::contains("fetched 2 merged PR summaries"))
        .stderr(predicate::str::contains("importing PR #101: Newer PR"))
        .stderr(predicate::str::contains("importing PR #100: Older PR"))
        .stderr(predicate::str::contains(
            "dry-run: would import 2 PRs (2 fetched",
        ));
}

#[test]
fn blame_groups_lines_by_branch_metadata() {
    let (dir, _sha) = harness::setup_repo();
    std::fs::write(dir.path().join("file.txt"), "one\n").unwrap();
    git(dir.path(), &["add", "file.txt"]);
    git(dir.path(), &["commit", "-m", "feat: first"]);
    let first = git_stdout(dir.path(), &["rev-parse", "HEAD"]);

    std::fs::write(dir.path().join("file.txt"), "one\ntwo\n").unwrap();
    git(dir.path(), &["add", "file.txt"]);
    git(dir.path(), &["commit", "-m", "fix: second"]);
    let second = git_stdout(dir.path(), &["rev-parse", "HEAD"]);

    harness::git_meta(dir.path())
        .args(["set", &format!("commit:{first}"), "branch-id", "feature#1"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "branch:feature#1", "title", "Add first line"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "branch:feature#1", "review:number", "1"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args([
            "set",
            "branch:feature#1",
            "review:url",
            "https://github.com/owner/repo/pull/1",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args([
            "set:add",
            "branch:feature#1",
            "commits:author",
            "Alice <alice@example.com>",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args([
            "set:add",
            "branch:feature#1",
            "commits:author",
            "Bob <bob@example.com>",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args([
            "set:add",
            "branch:feature#1",
            "commits:author-date",
            "1775001600",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args([
            "set:add",
            "branch:feature#1",
            "commits:author-date",
            "1775174400",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", &format!("commit:{second}"), "branch-id", "fix#2"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "branch:fix#2", "title", "Add second line"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "branch:fix#2", "review:number", "2"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["blame", "--porcelain", "file.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"branch_id\": \"feature#1\""))
        .stdout(predicate::str::contains("\"branch_id\": \"fix#2\""))
        .stdout(predicate::str::contains("Add first line"))
        .stdout(predicate::str::contains("Add second line"));

    harness::git_meta(dir.path())
        .args(["blame", "--json", "file.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"start_line\": 1"))
        .stdout(predicate::str::contains("\"end_line\": 1"))
        .stdout(predicate::str::contains("\"number\": \"1\""))
        .stdout(predicate::str::contains("\"title\": \"Add first line\""))
        .stdout(predicate::str::contains(
            "\"url\": \"https://github.com/owner/repo/pull/1\"",
        ))
        .stdout(predicate::str::contains("\"commit_authors\": ["))
        .stdout(predicate::str::contains("Alice <alice@example.com>"))
        .stdout(predicate::str::contains("Bob <bob@example.com>"))
        .stdout(predicate::str::contains("\"commit_author_dates\": ["))
        .stdout(predicate::str::contains("\"1775001600\""))
        .stdout(predicate::str::contains("\"1775174400\""))
        .stdout(predicate::str::contains(
            "\"commit_author_date_range\": \"2026-04-01..2026-04-03\"",
        ))
        .stdout(predicate::str::contains("\"lines\"").not());

    harness::git_meta(dir.path())
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .env("COLUMNS", "80")
        .args(["blame", "file.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\x1b["))
        .stdout(predicate::str::contains(
            "https://github.com/owner/repo/pull/1",
        ))
        .stdout(predicate::str::contains(
            "alice@example.com, bob@example.com",
        ))
        .stdout(predicate::str::contains("dates:"))
        .stdout(predicate::str::contains("2026-04-01..2026-04-03"));
}

#[test]
fn blame_without_branch_metadata_shows_commit_details() {
    let (dir, _sha) = harness::setup_repo();
    std::fs::write(dir.path().join("file.txt"), "one\n").unwrap();
    git(dir.path(), &["add", "file.txt"]);
    git(dir.path(), &["commit", "-m", "feat: no pr"]);
    let commit = git_stdout(dir.path(), &["rev-parse", "--short=8", "HEAD"]);

    harness::git_meta(dir.path())
        .env_remove("NO_COLOR")
        .env("NO_COLOR", "1")
        .env("COLUMNS", "80")
        .args(["blame", "file.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("feat: no pr"))
        .stdout(predicate::str::contains(format!("commit: {commit}")))
        .stdout(predicate::str::contains("author: Test User"))
        .stdout(predicate::str::contains("date:"))
        .stdout(predicate::str::contains("http").not())
        .stderr(predicate::str::contains("No PR metadata found").not());
}

fn git(repo: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(["-C", &repo.to_string_lossy()])
        .args(args)
        .env("GIT_AUTHOR_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_stdout(repo: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(["-C", &repo.to_string_lossy()])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}
