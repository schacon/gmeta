use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use std::collections::{BTreeSet, HashSet};
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::context::CommandContext;
use git_meta_lib::db::Store;
use git_meta_lib::types::{Target, TargetType, ValueType, GIT_REF_THRESHOLD};
use git_meta_lib::MetaValue;

/// Supported import source formats.
#[derive(Debug, Clone, PartialEq)]
pub enum ImportFormat {
    /// Import the entire git history.
    Entire,
    /// Import from git-ai format.
    GitAi,
}

impl ImportFormat {
    /// Parse an import format string.
    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "entire" => Ok(ImportFormat::Entire),
            "git-ai" => Ok(ImportFormat::GitAi),
            other => bail!("unsupported import format: {other}"),
        }
    }
}

pub fn run(format: ImportFormat, dry_run: bool, since: Option<&str>) -> Result<()> {
    let since_epoch = parse_since_epoch(since)?;

    match format {
        ImportFormat::Entire => run_entire(dry_run, since_epoch),
        ImportFormat::GitAi => run_git_ai(dry_run, since_epoch),
    }
}

/// Import merged GitHub pull request metadata using the `gh` CLI.
///
/// # Parameters
///
/// - `dry_run`: print planned metadata writes without mutating the store.
/// - `limit`: maximum number of merged pull requests to fetch.
/// - `since`: optional lower bound for merged PRs, formatted as `YYYY-MM-DD`.
/// - `repo`: optional GitHub repository in `OWNER/NAME` form.
/// - `include_comments`: whether to import PR comments and review bodies.
/// - `no_tags`: reserved for release tag mapping; currently skips that phase.
///
/// # Errors
///
/// Returns an error if `gh` is missing or unauthenticated, GitHub output cannot
/// be parsed, or the metadata store fails to write an imported value.
pub fn run_gh(
    dry_run: bool,
    limit: Option<usize>,
    since: Option<&str>,
    repo: Option<&str>,
    include_comments: bool,
    no_tags: bool,
) -> Result<()> {
    let since_epoch = parse_since_epoch(since)?;
    ensure_gh_auth()?;

    let ctx = CommandContext::open(None)?;
    let repo_name = match repo {
        Some(repo) => repo.to_string(),
        None => resolve_gh_repo()?,
    };
    let prs = fetch_merged_prs(&repo_name, limit.unwrap_or(100), include_comments)?;
    let imported = imported_pr_numbers(ctx.session.store())?;

    let mut summary = GhImportSummary::default();
    for pr in prs {
        summary.fetched += 1;
        if let Some(cutoff) = since_epoch {
            if pr.merged_timestamp_seconds().unwrap_or_default() < cutoff {
                summary.skipped += 1;
                continue;
            }
        }
        if imported.contains(&pr.number.to_string()) {
            summary.skipped += 1;
            if since_epoch.is_none() && limit.is_none() {
                break;
            }
            continue;
        }

        let pr = fetch_pr_detail(&repo_name, pr.number, include_comments)?;
        let imported_pr = GitHubPullRequestImport::from_pr(pr);
        summary.comments += imported_pr.comments.len() as u64;
        eprintln!(
            "importing PR #{}: {}",
            imported_pr.number, imported_pr.title
        );
        summary.writes += apply_gh_import(
            &ctx,
            &repo_name,
            &imported_pr,
            dry_run,
            &mut summary.missing_commits,
        )?;
        summary.imported += 1;
    }

    if !no_tags {
        eprintln!("release tag mapping is not implemented yet; skipping tags");
    }

    if dry_run {
        eprintln!(
            "dry-run: would import {} PRs ({} fetched, {} skipped, {} comments, {} missing commits, {} writes)",
            summary.imported,
            summary.fetched,
            summary.skipped,
            summary.comments,
            summary.missing_commits,
            summary.writes,
        );
    } else {
        eprintln!(
            "imported {} PRs ({} fetched, {} skipped, {} comments, {} missing commits, {} writes)",
            summary.imported,
            summary.fetched,
            summary.skipped,
            summary.comments,
            summary.missing_commits,
            summary.writes,
        );
    }

    Ok(())
}

fn parse_since_epoch(since: Option<&str>) -> Result<Option<i64>> {
    match since {
        Some(date_str) => {
            let date_fmt =
                time::format_description::parse("[year]-[month]-[day]").unwrap_or_default();
            let date = time::Date::parse(date_str, &date_fmt).with_context(|| {
                format!("invalid --since date '{date_str}', expected YYYY-MM-DD")
            })?;
            let odt = time::OffsetDateTime::new_utc(date, time::Time::MIDNIGHT);
            Ok(Some(odt.unix_timestamp()))
        }
        None => Ok(None),
    }
}

#[derive(Default)]
struct GhImportSummary {
    fetched: u64,
    imported: u64,
    skipped: u64,
    comments: u64,
    missing_commits: u64,
    writes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhRepoView {
    name: String,
    owner: GhOwner,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum GhOwner {
    Login { login: String },
    Name(String),
}

impl GhOwner {
    fn login(&self) -> &str {
        match self {
            GhOwner::Login { login } => login,
            GhOwner::Name(name) => name,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhPullRequest {
    number: u64,
    title: String,
    #[serde(default)]
    body: Option<String>,
    url: String,
    #[serde(default)]
    head_ref_name: Option<String>,
    #[serde(default)]
    base_ref_name: Option<String>,
    #[serde(default)]
    merged_at: Option<String>,
    #[serde(default)]
    merge_commit: Option<GhCommit>,
    #[serde(default)]
    commits: Vec<GhCommit>,
    #[serde(default)]
    comments: Vec<GhComment>,
    #[serde(default)]
    reviews: Vec<GhReview>,
    #[serde(default, alias = "closingIssuesReferences")]
    closing_issues: Vec<GhIssue>,
}

impl GhPullRequest {
    fn merged_timestamp_seconds(&self) -> Option<i64> {
        self.merged_at
            .as_deref()
            .and_then(|value| parse_rfc3339_seconds(value).ok())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhCommit {
    oid: String,
    #[serde(default, alias = "messageHeadline")]
    message_headline: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhComment {
    #[serde(default)]
    author: Option<GhAuthor>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhReview {
    #[serde(default)]
    author: Option<GhAuthor>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    submitted_at: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhIssue {
    #[serde(default)]
    number: Option<u64>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhAuthor {
    #[serde(default)]
    login: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

impl GhAuthor {
    fn identity(&self) -> Option<String> {
        self.login
            .as_ref()
            .or(self.name.as_ref())
            .filter(|value| !value.trim().is_empty())
            .cloned()
    }
}

#[derive(Debug)]
struct GitHubPullRequestImport {
    number: u64,
    branch_id: String,
    title: String,
    description: String,
    url: String,
    head_ref: String,
    base_ref: Option<String>,
    merged_timestamp_ms: i64,
    issues: BTreeSet<String>,
    issue_urls: BTreeSet<String>,
    commits: Vec<GitHubCommitImport>,
    comments: Vec<GitHubCommentImport>,
    reviewed_by: BTreeSet<String>,
    approved_by: BTreeSet<String>,
}

impl GitHubPullRequestImport {
    fn from_pr(pr: GhPullRequest) -> Self {
        let head_ref = pr.head_ref_name.unwrap_or_else(|| "unknown".to_string());
        let branch_id = branch_id(&head_ref, pr.number);
        let mut issues = pr
            .closing_issues
            .iter()
            .filter_map(|issue| issue.number.map(|number| format!("#{number}")))
            .collect::<BTreeSet<_>>();
        let issue_urls = pr
            .closing_issues
            .iter()
            .filter_map(|issue| issue.url.clone())
            .collect::<BTreeSet<_>>();
        if let Some(body) = &pr.body {
            issues.extend(extract_closing_issue_ids(body));
        }

        let mut commits = pr
            .commits
            .into_iter()
            .map(GitHubCommitImport::from)
            .collect::<Vec<_>>();
        if let Some(merge_commit) = pr.merge_commit {
            let merge_import = GitHubCommitImport::from(merge_commit);
            if !commits.iter().any(|commit| commit.oid == merge_import.oid) {
                commits.push(merge_import);
            }
        }

        let mut comments = pr
            .comments
            .into_iter()
            .filter_map(GitHubCommentImport::from_comment)
            .collect::<Vec<_>>();
        let mut reviewed_by = BTreeSet::new();
        let mut approved_by = BTreeSet::new();
        for review in pr.reviews {
            if let Some(author) = review.author.as_ref().and_then(GhAuthor::identity) {
                reviewed_by.insert(author.clone());
                if review
                    .state
                    .as_deref()
                    .is_some_and(|state| state.eq_ignore_ascii_case("APPROVED"))
                {
                    approved_by.insert(author);
                }
            }
            if let Some(comment) = GitHubCommentImport::from_review(review) {
                comments.push(comment);
            }
        }

        Self {
            number: pr.number,
            branch_id,
            title: pr.title,
            description: pr.body.unwrap_or_default(),
            url: pr.url,
            head_ref,
            base_ref: pr.base_ref_name,
            merged_timestamp_ms: pr
                .merged_at
                .as_deref()
                .and_then(|value| parse_rfc3339_seconds(value).ok())
                .unwrap_or_default()
                * 1000,
            issues,
            issue_urls,
            commits,
            comments,
            reviewed_by,
            approved_by,
        }
    }
}

#[derive(Debug)]
struct GitHubCommitImport {
    oid: String,
    subject: String,
}

impl From<GhCommit> for GitHubCommitImport {
    fn from(commit: GhCommit) -> Self {
        Self {
            oid: commit.oid,
            subject: commit.message_headline.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Serialize)]
struct GitHubCommentImport {
    kind: &'static str,
    author: Option<String>,
    body: String,
    url: Option<String>,
    created_at: Option<String>,
}

impl GitHubCommentImport {
    fn from_comment(comment: GhComment) -> Option<Self> {
        let body = comment.body.unwrap_or_default();
        if body.trim().is_empty() {
            return None;
        }
        Some(Self {
            kind: "comment",
            author: comment.author.as_ref().and_then(GhAuthor::identity),
            body,
            url: comment.url,
            created_at: comment.created_at,
        })
    }

    fn from_review(review: GhReview) -> Option<Self> {
        let body = review.body.unwrap_or_default();
        if body.trim().is_empty() {
            return None;
        }
        Some(Self {
            kind: "review",
            author: review.author.as_ref().and_then(GhAuthor::identity),
            body,
            url: review.url,
            created_at: review.submitted_at,
        })
    }
}

fn ensure_gh_auth() -> Result<()> {
    let output = Command::new("gh")
        .args(["auth", "status"])
        .output()
        .context("git meta import gh requires the GitHub CLI ('gh')")?;
    if !output.status.success() {
        bail!("GitHub CLI is not authenticated; run `gh auth login` before `git meta import gh`");
    }
    Ok(())
}

fn resolve_gh_repo() -> Result<String> {
    let output = Command::new("gh")
        .args(["repo", "view", "--json", "owner,name"])
        .output()
        .context("failed to run `gh repo view`")?;
    if !output.status.success() {
        bail!("could not infer GitHub repository; pass --repo OWNER/NAME");
    }
    let view: GhRepoView =
        serde_json::from_slice(&output.stdout).context("parsing `gh repo view` JSON")?;
    Ok(format!("{}/{}", view.owner.login(), view.name))
}

fn fetch_merged_prs(
    repo: &str,
    limit: usize,
    _include_comments: bool,
) -> Result<Vec<GhPullRequest>> {
    let fields = [
        "number",
        "title",
        "body",
        "url",
        "headRefName",
        "baseRefName",
        "mergedAt",
        "mergeCommit",
    ];

    let output = Command::new("gh")
        .args([
            "pr",
            "list",
            "--state",
            "merged",
            "--limit",
            &limit.to_string(),
            "--repo",
            repo,
            "--json",
            &fields.join(","),
        ])
        .output()
        .context("failed to run `gh pr list`")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("failed to fetch merged GitHub PRs: {}", stderr.trim());
    }

    serde_json::from_slice(&output.stdout).context("parsing `gh pr list` JSON")
}

fn fetch_pr_detail(repo: &str, number: u64, include_comments: bool) -> Result<GhPullRequest> {
    let mut fields = vec![
        "number",
        "title",
        "body",
        "url",
        "headRefName",
        "baseRefName",
        "mergedAt",
        "mergeCommit",
        "commits",
        "reviews",
    ];
    if include_comments {
        fields.push("comments");
    }

    let output = Command::new("gh")
        .args([
            "pr",
            "view",
            &number.to_string(),
            "--repo",
            repo,
            "--json",
            &fields.join(","),
        ])
        .output()
        .with_context(|| format!("failed to run `gh pr view {number}`"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("failed to fetch GitHub PR #{number}: {}", stderr.trim());
    }

    serde_json::from_slice(&output.stdout).with_context(|| format!("parsing PR #{number} JSON"))
}

fn imported_pr_numbers(db: &Store) -> Result<HashSet<String>> {
    let target = Target::project();
    match db.get_value(&target, "github:imported-pr")? {
        Some(MetaValue::Set(values)) => Ok(values.into_iter().collect()),
        _ => Ok(HashSet::new()),
    }
}

fn apply_gh_import(
    ctx: &CommandContext,
    repo_name: &str,
    pr: &GitHubPullRequestImport,
    dry_run: bool,
    missing_commits: &mut u64,
) -> Result<u64> {
    let mut writes = 0u64;
    let db = ctx.session.store();
    let repo = ctx.session.repo();
    let email = ctx.session.email();
    let branch_target = Target::branch(&pr.branch_id);
    let project_target = Target::project();
    let mut ts = pr.merged_timestamp_ms;

    writes += set_import_string(
        db,
        repo,
        dry_run,
        &branch_target,
        "title",
        &pr.title,
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_string(
        db,
        repo,
        dry_run,
        &branch_target,
        "description",
        &pr.description,
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_string(
        db,
        repo,
        dry_run,
        &branch_target,
        "review:number",
        &pr.number.to_string(),
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_string(
        db,
        repo,
        dry_run,
        &branch_target,
        "review:url",
        &pr.url,
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_string(
        db,
        repo,
        dry_run,
        &branch_target,
        "github:head-ref",
        &pr.head_ref,
        email,
        ts,
    )?;
    ts += 1;
    if let Some(base_ref) = &pr.base_ref {
        writes += set_import_string(
            db,
            repo,
            dry_run,
            &branch_target,
            "github:base-ref",
            base_ref,
            email,
            ts,
        )?;
        ts += 1;
    }

    for issue in &pr.issues {
        writes += set_import_member(db, dry_run, &branch_target, "issue:id", issue, email, ts)?;
        ts += 1;
    }
    for issue_url in &pr.issue_urls {
        writes += set_import_member(
            db,
            dry_run,
            &branch_target,
            "issue:url",
            issue_url,
            email,
            ts,
        )?;
        ts += 1;
    }
    for reviewer in &pr.reviewed_by {
        writes += set_import_member(
            db,
            dry_run,
            &branch_target,
            "review:reviewed",
            reviewer,
            email,
            ts,
        )?;
        ts += 1;
    }
    for approver in &pr.approved_by {
        writes += set_import_member(
            db,
            dry_run,
            &branch_target,
            "review:approved",
            approver,
            email,
            ts,
        )?;
        ts += 1;
    }
    for comment in &pr.comments {
        let value = serde_json::to_string(comment)?;
        writes += push_import_list(
            db,
            repo,
            dry_run,
            &branch_target,
            "review:comment",
            &value,
            email,
            ts,
        )?;
        ts += 1;
    }

    for commit in &pr.commits {
        if !commit_exists(repo, &commit.oid) {
            *missing_commits += 1;
            continue;
        }
        let commit_target = Target::from_parts(TargetType::Commit, Some(commit.oid.clone()));
        writes += set_import_string(
            db,
            repo,
            dry_run,
            &commit_target,
            "branch-id",
            &pr.branch_id,
            email,
            ts,
        )?;
        ts += 1;
        if let Some(conventional) = ConventionalType::parse(&commit.subject) {
            writes += set_import_member(
                db,
                dry_run,
                &commit_target,
                "conventional:type",
                &conventional.kind,
                email,
                ts,
            )?;
            ts += 1;
            if conventional.breaking {
                writes += set_import_member(
                    db,
                    dry_run,
                    &commit_target,
                    "conventional:type",
                    "breaking",
                    email,
                    ts,
                )?;
                ts += 1;
            }
        }
    }

    writes += set_import_string(
        db,
        repo,
        dry_run,
        &project_target,
        "github:repo",
        repo_name,
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_string(
        db,
        repo,
        dry_run,
        &project_target,
        "github:last-imported-merged-at",
        &pr.merged_timestamp_ms.to_string(),
        email,
        ts,
    )?;
    ts += 1;
    writes += set_import_member(
        db,
        dry_run,
        &project_target,
        "github:imported-pr",
        &pr.number.to_string(),
        email,
        ts,
    )?;

    Ok(writes)
}

fn set_import_string(
    db: &Store,
    repo: &gix::Repository,
    dry_run: bool,
    target: &Target,
    key: &str,
    value: &str,
    email: &str,
    timestamp: i64,
) -> Result<u64> {
    let encoded = json_string(value);
    let use_git_ref = encoded.len() > GIT_REF_THRESHOLD;
    if dry_run {
        eprintln!(
            "    [dry-run] {} {} = {}{}",
            target,
            key,
            truncate(&encoded, 80),
            if use_git_ref { " [git-ref]" } else { "" },
        );
        return Ok(1);
    }
    if use_git_ref {
        let blob_oid: gix::ObjectId = repo.write_blob(encoded.as_bytes())?.into();
        db.set_with_git_ref(
            None,
            target,
            key,
            &blob_oid.to_string(),
            &ValueType::String,
            email,
            timestamp,
            true,
        )?;
    } else {
        db.set(target, key, &encoded, &ValueType::String, email, timestamp)?;
    }
    Ok(1)
}

fn set_import_member(
    db: &Store,
    dry_run: bool,
    target: &Target,
    key: &str,
    value: &str,
    email: &str,
    timestamp: i64,
) -> Result<u64> {
    if dry_run {
        eprintln!("    [dry-run] {target} {key} += {value}");
        return Ok(1);
    }
    db.set_add(target, key, value, email, timestamp)?;
    Ok(1)
}

fn push_import_list(
    db: &Store,
    repo: &gix::Repository,
    dry_run: bool,
    target: &Target,
    key: &str,
    value: &str,
    email: &str,
    timestamp: i64,
) -> Result<u64> {
    if dry_run {
        eprintln!("    [dry-run] {target} {key} << {}", truncate(value, 80));
        return Ok(1);
    }
    db.list_push_with_repo(Some(repo), target, key, value, email, timestamp)?;
    Ok(1)
}

fn commit_exists(repo: &gix::Repository, oid: &str) -> bool {
    let Ok(object_id) = gix::ObjectId::from_hex(oid.as_bytes()) else {
        return false;
    };
    object_id.attach(repo).object().is_ok()
}

fn branch_id(head_ref: &str, number: u64) -> String {
    let sanitized = head_ref
        .chars()
        .map(|c| match c {
            '/' | ':' | '\0' => '-',
            other => other,
        })
        .collect::<String>();
    format!("{sanitized}#{number}")
}

#[derive(Debug, PartialEq, Eq)]
struct ConventionalType {
    kind: String,
    breaking: bool,
}

impl ConventionalType {
    fn parse(subject: &str) -> Option<Self> {
        let (prefix, _) = subject.split_once(':')?;
        let mut prefix = prefix.trim();
        if prefix.is_empty() || prefix.contains(' ') {
            return None;
        }

        let breaking = prefix.ends_with('!');
        if breaking {
            prefix = &prefix[..prefix.len().saturating_sub(1)];
        }
        let kind = prefix
            .split_once('(')
            .map_or(prefix, |(kind, _scope)| kind)
            .trim();
        if kind.is_empty()
            || !kind
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return None;
        }

        Some(Self {
            kind: kind.to_string(),
            breaking,
        })
    }
}

fn extract_closing_issue_ids(body: &str) -> BTreeSet<String> {
    let closing_words = [
        "close", "closes", "closed", "fix", "fixes", "fixed", "resolve", "resolves", "resolved",
    ];
    let tokens = body.split_whitespace().collect::<Vec<_>>();
    let mut issues = BTreeSet::new();
    for pair in tokens.windows(2) {
        let keyword = pair[0]
            .trim_matches(|c: char| !c.is_ascii_alphabetic())
            .to_ascii_lowercase();
        if !closing_words.contains(&keyword.as_str()) {
            continue;
        }
        let issue = pair[1].trim_matches(|c: char| c == '.' || c == ',' || c == ')' || c == '(');
        if issue.starts_with('#') && issue[1..].chars().all(|c| c.is_ascii_digit()) {
            issues.insert(issue.to_string());
        }
    }
    issues
}

fn parse_rfc3339_seconds(value: &str) -> Result<i64> {
    let parsed = time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .with_context(|| format!("invalid RFC3339 timestamp: {value}"))?;
    Ok(parsed.unix_timestamp())
}

fn run_entire(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let email = ctx.session.email();
    let fallback_ts = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;

    let db = if dry_run {
        None
    } else {
        Some(ctx.session.store())
    };

    let mut imported_count = 0u64;

    // Resolve the checkpoints tree (local or remote)
    let checkpoints_tree_id = resolve_entire_ref(repo, "entire/checkpoints/v1")?;
    if checkpoints_tree_id.is_none() {
        eprintln!("No entire/checkpoints/v1 ref found (local or remote), skipping checkpoints");
    }

    // Step 1: Walk all commits looking for Entire-Checkpoint trailers.
    if let Some(cp_tree_id) = checkpoints_tree_id {
        if let Some(ts) = since_epoch {
            let date_str = time::OffsetDateTime::from_unix_timestamp(ts)
                .ok()
                .and_then(|d| {
                    d.format(
                        &time::format_description::parse("[year]-[month]-[day]")
                            .unwrap_or_default(),
                    )
                    .ok()
                })
                .unwrap_or_else(|| "unknown".to_string());
            eprintln!("Scanning commits for Entire-Checkpoint trailers (since {date_str})...");
        } else {
            eprintln!("Scanning commits for Entire-Checkpoint trailers...");
        }
        imported_count +=
            import_checkpoints_from_commits(repo, cp_tree_id, db, email, dry_run, since_epoch)?;
    }

    // Step 2: Import trails
    if let Some(tree_id) = resolve_entire_ref(repo, "entire/trails/v1")? {
        eprintln!("Processing entire/trails/v1...");
        imported_count += import_trails(repo, tree_id, db, email, fallback_ts, dry_run)?;
    } else {
        eprintln!("No entire/trails/v1 ref found, skipping trails");
    }

    if dry_run {
        eprintln!("Dry run: would have imported {imported_count} keys");
    } else {
        eprintln!("Imported {imported_count} keys");
    }

    Ok(())
}

/// Resolve an entire ref to the tree OID of its tip commit.
fn resolve_entire_ref(repo: &gix::Repository, refname: &str) -> Result<Option<gix::ObjectId>> {
    let reference = repo
        .find_reference(&format!("refs/heads/{refname}"))
        .or_else(|_| repo.find_reference(&format!("refs/remotes/origin/{refname}")))
        .or_else(|_| repo.find_reference(refname));

    match reference {
        Ok(r) => {
            let refname_used = r.name().as_bstr().to_string();
            eprintln!("  Resolved {refname} via {refname_used}");
            let commit_id = r.into_fully_peeled_id()?.detach();
            let commit_obj = commit_id.attach(repo).object()?.into_commit();
            let tree_id = commit_obj.tree_id()?.detach();
            Ok(Some(tree_id))
        }
        Err(_) => Ok(None),
    }
}

/// Walk all commits across all refs, find Entire-Checkpoint trailers,
/// look up each checkpoint in the checkpoints tree, and import it.
fn import_checkpoints_from_commits(
    repo: &gix::Repository,
    checkpoints_tree_id: gix::ObjectId,
    db: Option<&Store>,
    email: &str,
    dry_run: bool,
    since_epoch: Option<i64>,
) -> Result<u64> {
    let mut count = 0u64;
    let mut seen_commits: HashSet<gix::ObjectId> = HashSet::new();
    let mut found = 0u64;
    let mut skipped = 0u64;
    let mut missing = 0u64;

    // Collect all ref tips to walk
    let mut start_oids: Vec<gix::ObjectId> = Vec::new();
    let platform = repo.references()?;
    for r in platform.all()?.flatten() {
        let name = r.name().as_bstr().to_string();
        if name.contains("/entire/") {
            continue;
        }
        if let Ok(id) = r.into_fully_peeled_id() {
            start_oids.push(id.detach());
        }
    }

    // Walk each ref tip
    let mut scanned = 0u64;
    for start_oid in &start_oids {
        let walk = repo.rev_walk(Some(*start_oid));
        let Ok(iter) = walk.all() else {
            continue;
        };

        for info_result in iter {
            let Ok(info) = info_result else {
                continue;
            };
            let oid = info.id;
            if !seen_commits.insert(oid) {
                continue;
            }
            scanned += 1;

            let commit_obj = oid.attach(repo).object()?.into_commit();
            let decoded = commit_obj.decode()?;

            // Skip commits older than --since date
            if let Some(cutoff) = since_epoch {
                if {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } < cutoff
                {
                    continue;
                }
            }

            let msg = decoded.message.to_str_lossy().to_string();

            // Look for Entire-Checkpoint trailer(s)
            for line in msg.lines() {
                let line = line.trim();
                let Some(checkpoint_id) = line.strip_prefix("Entire-Checkpoint:") else {
                    continue;
                };
                let checkpoint_id = checkpoint_id.trim();
                if checkpoint_id.is_empty() {
                    continue;
                }

                let commit_sha = oid.to_string();

                // Skip if already imported
                if let Some(db) = db {
                    let commit_target = git_meta_lib::types::Target::from_parts(
                        TargetType::Commit,
                        Some(commit_sha.clone()),
                    );
                    if let Ok(Some(_mv)) = db.get(&commit_target, "agent:checkpoint-id") {
                        skipped += 1;
                        continue;
                    }
                }

                // Look up checkpoint in the sharded tree: first2/rest/
                let shard = &checkpoint_id[..2.min(checkpoint_id.len())];
                let rest = &checkpoint_id[2.min(checkpoint_id.len())..];

                let checkpoint_tree_id = (|| -> Result<Option<gix::ObjectId>> {
                    let Some(shard_id) = entry_to_tree_id(repo, checkpoints_tree_id, shard)? else {
                        return Ok(None);
                    };
                    entry_to_tree_id(repo, shard_id, rest)
                })()?;

                let Some(checkpoint_tree_id) = checkpoint_tree_id else {
                    missing += 1;
                    eprintln!(
                        "  Commit {} has Entire-Checkpoint: {} but checkpoint not found in tree",
                        &commit_sha[..7],
                        checkpoint_id
                    );
                    continue;
                };

                found += 1;
                eprintln!(
                    "  Commit {} <- checkpoint {}",
                    &commit_sha[..7],
                    checkpoint_id,
                );

                // Use the commit's author date as the metadata timestamp
                let mut ts = {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } * 1000;

                // Store checkpoint ID
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    &commit_sha,
                    "agent:checkpoint-id",
                    &json_string(checkpoint_id),
                    &ValueType::String,
                    email,
                    ts,
                )?;
                ts += 1;

                // Import top-level metadata.json (checkpoint summary)
                if let Some(content) = entry_to_blob(repo, checkpoint_tree_id, "metadata.json")? {
                    let meta: Value = serde_json::from_str(&content).unwrap_or(Value::Null);
                    let checkpoint_fields: &[(&str, &[&str])] = &[
                        ("strategy", &["strategy"]),
                        ("branch", &["branch"]),
                        ("files-changed", &["filesChanged", "files_changed"]),
                        ("token-usage", &["tokenUsage", "token_usage"]),
                    ];
                    for (gmeta_key, aliases) in checkpoint_fields {
                        if let Some(val) = aliases.iter().find_map(|a| meta.get(*a)) {
                            let key = format!("agent:{gmeta_key}");
                            let json_val = json_encode_value(val)?;
                            count += set_value(
                                repo,
                                db,
                                dry_run,
                                &TargetType::Commit,
                                &commit_sha,
                                &key,
                                &json_val,
                                &ValueType::String,
                                email,
                                ts,
                            )?;
                            ts += 1;
                        }
                    }
                }

                // Import session slots (0, 1, 2, ...)
                let mut session_idx = 0u32;
                loop {
                    let slot_name = session_idx.to_string();
                    let Some(session_tree_id) =
                        entry_to_tree_id(repo, checkpoint_tree_id, &slot_name)?
                    else {
                        break;
                    };

                    let key_prefix = if session_idx == 0 {
                        "agent".to_string()
                    } else {
                        format!("agent:session-{session_idx}")
                    };

                    count += import_session(
                        repo,
                        session_tree_id,
                        db,
                        &commit_sha,
                        &key_prefix,
                        email,
                        &mut ts,
                        dry_run,
                    )?;

                    session_idx += 1;
                }
            }
        }
    }

    eprintln!(
        "Scanned {scanned} commits: {found} checkpoints imported, {skipped} already present, {missing} not found in tree"
    );

    Ok(count)
}

/// Import a single session slot's data.
fn import_session(
    repo: &gix::Repository,
    session_tree_id: gix::ObjectId,
    db: Option<&Store>,
    commit_sha: &str,
    key_prefix: &str,
    email: &str,
    ts: &mut i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;

    // Session metadata.json
    if let Some(content) = entry_to_blob(repo, session_tree_id, "metadata.json")? {
        let meta: Value =
            serde_json::from_str(&content).context("parsing session metadata.json")?;

        let string_fields = [
            ("agent", "agent"),
            ("model", "model"),
            ("turnId", "turn-id"),
            ("turn_id", "turn-id"),
            ("sessionId", "session-id"),
            ("session_id", "session-id"),
        ];
        for (json_key, gmeta_key) in &string_fields {
            if let Some(val) = meta.get(json_key) {
                let key = format!("{key_prefix}:{gmeta_key}");
                let json_val = json_encode_value(val)?;
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_val,
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }
        }

        let object_fields = [
            ("attribution", "attribution"),
            ("summary", "summary"),
            ("tokenUsage", "token-usage"),
            ("token_usage", "token-usage"),
        ];
        for (json_key, gmeta_key) in &object_fields {
            if let Some(val) = meta.get(json_key) {
                let key = format!("{key_prefix}:{gmeta_key}");
                let json_val = json_encode_value(val)?;
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_val,
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }
        }
    }

    // prompt.txt
    if let Some(content) = entry_to_blob(repo, session_tree_id, "prompt.txt")? {
        let key = format!("{key_prefix}:prompt");
        count += set_value(
            repo,
            db,
            dry_run,
            &TargetType::Commit,
            commit_sha,
            &key,
            &json_string(&content),
            &ValueType::String,
            email,
            *ts,
        )?;
        *ts += 1;
    }

    // full.jsonl
    if let Some(content) = entry_to_blob(repo, session_tree_id, "full.jsonl")? {
        let key = format!("{key_prefix}:transcript");
        if !content.trim().is_empty() {
            let json_val = json_string(&content);
            count += set_value(
                repo,
                db,
                dry_run,
                &TargetType::Commit,
                commit_sha,
                &key,
                &json_val,
                &ValueType::String,
                email,
                *ts,
            )?;
            *ts += 1;
        }
    }

    // content_hash.txt
    if let Some(content) = entry_to_blob(repo, session_tree_id, "content_hash.txt")? {
        let key = format!("{key_prefix}:content-hash");
        count += set_value(
            repo,
            db,
            dry_run,
            &TargetType::Commit,
            commit_sha,
            &key,
            &json_string(content.trim()),
            &ValueType::String,
            email,
            *ts,
        )?;
        *ts += 1;
    }

    // tasks/ directory
    if let Some(tasks_tree_id) = entry_to_tree_id(repo, session_tree_id, "tasks")? {
        let tasks_tree = tasks_tree_id.attach(repo).object()?.into_tree();
        for task_entry_result in tasks_tree.iter() {
            let task_entry = task_entry_result?;
            let tool_use_id = task_entry.filename().to_str_lossy().to_string();
            if tool_use_id.is_empty() || !task_entry.mode().is_tree() {
                continue;
            }
            let task_tree_id = task_entry.object_id();

            if let Some(content) = entry_to_blob(repo, task_tree_id, "checkpoint.json")? {
                let key = format!(
                    "{}:tasks:{}:checkpoint",
                    key_prefix,
                    sanitize_key_segment(&tool_use_id)
                );
                count += set_value(
                    repo,
                    db,
                    dry_run,
                    &TargetType::Commit,
                    commit_sha,
                    &key,
                    &json_string(&content),
                    &ValueType::String,
                    email,
                    *ts,
                )?;
                *ts += 1;
            }

            let task_tree = task_tree_id.attach(repo).object()?.into_tree();
            for agent_entry_result in task_tree.iter() {
                let agent_entry = agent_entry_result?;
                let name = agent_entry.filename().to_str_lossy().to_string();
                if name.starts_with("agent-")
                    && name.ends_with(".jsonl")
                    && agent_entry.mode().is_blob()
                {
                    let blob = agent_entry.object_id().attach(repo).object()?.into_blob();
                    let content = String::from_utf8_lossy(&blob.data);
                    let agent_id = name
                        .strip_prefix("agent-")
                        .unwrap_or(&name)
                        .strip_suffix(".jsonl")
                        .unwrap_or(&name);
                    let key = format!(
                        "{}:tasks:{}:agent-{}",
                        key_prefix,
                        sanitize_key_segment(&tool_use_id),
                        sanitize_key_segment(agent_id),
                    );
                    let lines: Vec<&str> =
                        content.lines().filter(|l| !l.trim().is_empty()).collect();
                    if !lines.is_empty() {
                        let mut entries = Vec::new();
                        for (i, line) in lines.iter().enumerate() {
                            entries.push(git_meta_lib::ListEntry {
                                value: line.to_string(),
                                timestamp: *ts + i as i64,
                            });
                        }
                        let encoded = git_meta_lib::list_value::encode_entries(&entries)?;
                        count += set_value(
                            repo,
                            db,
                            dry_run,
                            &TargetType::Commit,
                            commit_sha,
                            &key,
                            &encoded,
                            &ValueType::List,
                            email,
                            *ts,
                        )?;
                        *ts += lines.len() as i64 + 1;
                    }
                }
            }
        }
    }

    Ok(count)
}

/// Read a blob from a tree entry by name.
fn entry_to_blob(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    name: &str,
) -> Result<Option<String>> {
    let tree = tree_id.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.filename().to_str_lossy() == name && entry.mode().is_blob() {
            let blob = entry.object_id().attach(repo).object()?.into_blob();
            return Ok(Some(String::from_utf8_lossy(&blob.data).to_string()));
        }
    }
    Ok(None)
}

/// Read a subtree OID from a tree entry by name.
fn entry_to_tree_id(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    name: &str,
) -> Result<Option<gix::ObjectId>> {
    let tree = tree_id.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.filename().to_str_lossy() == name && entry.mode().is_tree() {
            return Ok(Some(entry.object_id()));
        }
    }
    Ok(None)
}

/// Load the set of trail IDs that have already been imported.
fn load_imported_trail_ids(db: Option<&Store>) -> Result<HashSet<String>> {
    match db {
        Some(db) => Ok(db.imported_trail_ids()?),
        None => Ok(HashSet::new()),
    }
}

fn import_trails(
    repo: &gix::Repository,
    root_tree_id: gix::ObjectId,
    db: Option<&Store>,
    email: &str,
    base_ts: i64,
    dry_run: bool,
) -> Result<u64> {
    let mut count = 0u64;
    let mut ts = base_ts;
    let imported_trails = load_imported_trail_ids(db)?;

    let root_tree = root_tree_id.attach(repo).object()?.into_tree();
    for shard_entry_result in root_tree.iter() {
        let shard_entry = shard_entry_result?;
        let shard_name = shard_entry.filename().to_str_lossy().to_string();
        if shard_name.len() != 2 || !shard_entry.mode().is_tree() {
            continue;
        }

        let shard_tree = shard_entry.object_id().attach(repo).object()?.into_tree();
        for item_entry_result in shard_tree.iter() {
            let item_entry = item_entry_result?;
            let rest_name = item_entry.filename().to_str_lossy().to_string();
            let trail_id = format!("{shard_name}{rest_name}");

            if imported_trails.contains(&trail_id) {
                eprintln!("  Trail {trail_id} (already imported, skipping)");
                continue;
            }

            if !item_entry.mode().is_tree() {
                continue;
            }
            let item_tree_id = item_entry.object_id();

            let Some(meta_content) = entry_to_blob(repo, item_tree_id, "metadata.json")? else {
                eprintln!("  Skipping trail {trail_id} (no metadata.json)");
                continue;
            };
            let meta: Value =
                serde_json::from_str(&meta_content).context("parsing trail metadata.json")?;

            let branch_name = meta
                .get("branch")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let branch_uuid = format!(
                "{}-{}",
                branch_name,
                uuid::Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("0000")
            );
            eprintln!("  Trail {trail_id} (branch {branch_name}) -> branch:{branch_uuid}");

            count += set_value(
                repo,
                db,
                dry_run,
                &TargetType::Branch,
                &branch_uuid,
                "review:trail-id",
                &json_string(&trail_id),
                &ValueType::String,
                email,
                ts,
            )?;
            ts += 1;

            let string_fields = [
                "title", "body", "status", "type", "author", "priority", "base",
            ];
            for field in &string_fields {
                if let Some(val) = meta.get(field) {
                    let key = format!("review:{field}");
                    let json_val = json_encode_value(val)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        &key,
                        &json_val,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }

            let json_fields = ["assignees", "labels", "reviewers"];
            for field in &json_fields {
                if let Some(val) = meta.get(field) {
                    let key = format!("review:{field}");
                    let json_val = json_encode_value(val)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        &key,
                        &json_val,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }

            if let Some(content) = entry_to_blob(repo, item_tree_id, "checkpoints.json")? {
                let arr: Vec<Value> = serde_json::from_str(&content).unwrap_or_default();
                if !arr.is_empty() {
                    let mut entries = Vec::new();
                    for (i, item) in arr.iter().enumerate() {
                        entries.push(git_meta_lib::ListEntry {
                            value: serde_json::to_string(item)?,
                            timestamp: ts + i as i64,
                        });
                    }
                    let encoded = git_meta_lib::list_value::encode_entries(&entries)?;
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        "review:checkpoints",
                        &encoded,
                        &ValueType::List,
                        email,
                        ts,
                    )?;
                    ts += arr.len() as i64 + 1;
                }
            }

            if let Some(content) = entry_to_blob(repo, item_tree_id, "discussion.json")? {
                let disc: Value = serde_json::from_str(&content).unwrap_or(Value::Null);
                if disc != Value::Null {
                    count += set_value(
                        repo,
                        db,
                        dry_run,
                        &TargetType::Branch,
                        &branch_uuid,
                        "review:discussion",
                        &json_encode_value(&disc)?,
                        &ValueType::String,
                        email,
                        ts,
                    )?;
                    ts += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Store a value in the database (or just count it for dry run).
/// Large string values (> GIT_REF_THRESHOLD bytes) are stored as git blob refs.
fn set_value(
    repo: &gix::Repository,
    db: Option<&Store>,
    dry_run: bool,
    target_type: &TargetType,
    target_value: &str,
    key: &str,
    value: &str,
    value_type: &ValueType,
    email: &str,
    timestamp: i64,
) -> Result<u64> {
    let use_git_ref = *value_type == ValueType::String && value.len() > GIT_REF_THRESHOLD;

    if dry_run {
        eprintln!(
            "    [dry-run] {}:{} {} = {}{}",
            target_type.as_str(),
            &target_value[..7.min(target_value.len())],
            key,
            truncate(value, 80),
            if use_git_ref { " [git-ref]" } else { "" },
        );
        return Ok(1);
    }

    if let Some(db) = db {
        let target = if *target_type == TargetType::Project {
            git_meta_lib::types::Target::project()
        } else {
            git_meta_lib::types::Target::from_parts(
                target_type.clone(),
                Some(target_value.to_string()),
            )
        };
        if use_git_ref {
            let blob_oid: gix::ObjectId = repo.write_blob(value.as_bytes())?.into();
            db.set_with_git_ref(
                None,
                &target,
                key,
                &blob_oid.to_string(),
                value_type,
                email,
                timestamp,
                true,
            )?;
        } else {
            db.set(&target, key, value, value_type, email, timestamp)?;
        }
    }
    Ok(1)
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
}

fn json_encode_value(val: &Value) -> Result<String> {
    if let Some(s) = val.as_str() {
        Ok(json_string(s))
    } else {
        let serialized = serde_json::to_string(val)?;
        Ok(json_string(&serialized))
    }
}

fn sanitize_key_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c == '/' || c == '\0' || c == ':' {
                '-'
            } else {
                c
            }
        })
        .collect()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

const NOTES_REFS: &[&str] = &["refs/remotes/notes/ai", "refs/notes/ai"];

fn run_git_ai(dry_run: bool, since_epoch: Option<i64>) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let email = ctx.session.email();

    let db = if dry_run {
        None
    } else {
        Some(ctx.session.store())
    };

    // Locate the notes ref
    let notes_ref = NOTES_REFS
        .iter()
        .find(|&&r| repo.find_reference(r).is_ok())
        .copied();

    let Some(notes_ref) = notes_ref else {
        bail!(
            "no git-ai notes ref found; expected one of: {}",
            NOTES_REFS.join(", ")
        )
    };

    eprintln!("importing git-ai notes from {notes_ref}");

    // Resolve to the notes tree OID
    let notes_commit_id = repo
        .find_reference(notes_ref)?
        .into_fully_peeled_id()?
        .detach();
    let notes_commit = notes_commit_id.attach(repo).object()?.into_commit();
    let notes_tree_id = notes_commit.tree_id()?.detach();

    // Walk the two-level fanout tree
    let mut total = 0u64;
    let mut imported = 0u64;
    let mut skipped_date = 0u64;
    let mut skipped_exists = 0u64;
    let mut errors = 0u64;

    let notes_tree = notes_tree_id.attach(repo).object()?.into_tree();
    for shard_entry_result in notes_tree.iter() {
        let Ok(shard_entry) = shard_entry_result else {
            continue;
        };
        let shard_name = shard_entry.filename().to_str_lossy().to_string();
        // Only descend into two-char hex shard dirs.
        if shard_name.len() != 2 || !shard_name.chars().all(|c| c.is_ascii_hexdigit()) {
            continue;
        }
        if !shard_entry.mode().is_tree() {
            continue;
        }
        let Ok(shard_tree) = shard_entry.object_id().attach(repo).object() else {
            continue;
        };
        let shard_tree = shard_tree.into_tree();

        for note_entry_result in shard_tree.iter() {
            let Ok(note_entry) = note_entry_result else {
                continue;
            };
            let rest = note_entry.filename().to_str_lossy().to_string();
            let commit_sha = format!("{shard_name}{rest}");

            // Verify the annotated commit exists and is within --since range.
            let Ok(commit_oid) = gix::ObjectId::from_hex(commit_sha.as_bytes()) else {
                errors += 1;
                continue;
            };
            let Ok(annotated_commit) = commit_oid.attach(repo).object() else {
                errors += 1;
                continue;
            };
            let annotated_commit = annotated_commit.into_commit();
            let Ok(decoded) = annotated_commit.decode() else {
                errors += 1;
                continue;
            };
            if let Some(since) = since_epoch {
                if {
                    let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                    a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
                } < since
                {
                    skipped_date += 1;
                    continue;
                }
            }
            let commit_ts = {
                let a = decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;
                a.time().map_err(|e| anyhow::anyhow!("{e}"))?.seconds
            } * 1000;

            total += 1;

            // Read the note blob.
            let blob = match note_entry.object_id().attach(repo).object() {
                Ok(o) => o.into_blob(),
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };
            let note_text = match std::str::from_utf8(&blob.data) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    errors += 1;
                    continue;
                }
            };

            // Parse the note.
            let parsed = match parse_git_ai_note(&note_text) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!(
                        "  warning: could not parse note for {}: {}",
                        &commit_sha[..8],
                        e
                    );
                    errors += 1;
                    continue;
                }
            };

            // Check whether we already have data for this commit
            if let Some(db) = db {
                let commit_target = git_meta_lib::types::Target::from_parts(
                    TargetType::Commit,
                    Some(commit_sha.clone()),
                );
                if db.get(&commit_target, "agent.blame")?.is_some() {
                    skipped_exists += 1;
                    continue;
                }
            }

            eprintln!(
                "  commit {}  schema={}{}",
                &commit_sha[..8],
                parsed.schema_version,
                if parsed.model == "unknown" {
                    String::new()
                } else {
                    format!("  model={}", parsed.model)
                },
            );

            if let Some(db) = db {
                let commit_target = git_meta_lib::types::Target::from_parts(
                    TargetType::Commit,
                    Some(commit_sha.clone()),
                );
                // agent.blame -- store as git blob ref if large
                let (blame_val, is_ref) = if parsed.blame.len() > GIT_REF_THRESHOLD {
                    let oid: gix::ObjectId = repo.write_blob(parsed.blame.as_bytes())?.into();
                    (oid.to_string(), true)
                } else {
                    (json_string(&parsed.blame), false)
                };
                db.set_with_git_ref(
                    None,
                    &commit_target,
                    "agent.blame",
                    &blame_val,
                    &ValueType::String,
                    email,
                    commit_ts,
                    is_ref,
                )?;

                db.set(
                    &commit_target,
                    "agent.git-ai.schema-version",
                    &json_string(&parsed.schema_version),
                    &ValueType::String,
                    email,
                    commit_ts,
                )?;

                if let Some(ref ver) = parsed.git_ai_version {
                    db.set(
                        &commit_target,
                        "agent.git-ai.version",
                        &json_string(ver),
                        &ValueType::String,
                        email,
                        commit_ts,
                    )?;
                }

                if parsed.model != "unknown" {
                    db.set(
                        &commit_target,
                        "agent.model",
                        &json_string(&parsed.model),
                        &ValueType::String,
                        email,
                        commit_ts,
                    )?;
                }
            }

            imported += 1;
        }
    }

    eprintln!();
    if dry_run {
        eprintln!(
            "dry-run: would import {} commits ({} skipped: date filter={}, already exists={}; {} errors)",
            total.saturating_sub(skipped_date + skipped_exists + errors),
            skipped_date + skipped_exists,
            skipped_date,
            skipped_exists,
            errors,
        );
    } else {
        eprintln!(
            "imported {imported} commits  (skipped: date={skipped_date} already-exists={skipped_exists}  errors={errors})",
        );
    }

    Ok(())
}

struct GitAiNote {
    blame: String,
    schema_version: String,
    git_ai_version: Option<String>,
    model: String,
}

fn parse_git_ai_note(text: &str) -> Result<GitAiNote> {
    let (blame_raw, json_raw) = if let Some(rest) = text.strip_prefix("---\n") {
        ("", rest)
    } else {
        let sep = "\n---\n";
        match text.find(sep) {
            Some(pos) => (&text[..pos], &text[pos + sep.len()..]),
            None => bail!("no '---' separator found in note"),
        }
    };

    let blame = blame_raw.trim_end().to_string();

    let json: Value = serde_json::from_str(json_raw.trim()).context("failed to parse note JSON")?;

    let schema_version = json
        .get("schema_version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let git_ai_version = json
        .get("git_ai_version")
        .and_then(|v| v.as_str())
        .map(std::string::ToString::to_string);

    let mut models: Vec<String> = Vec::new();
    if let Some(prompts) = json.get("prompts").and_then(|v| v.as_object()) {
        for prompt in prompts.values() {
            if let Some(agent_id) = prompt.get("agent_id") {
                let tool = agent_id
                    .get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let model = agent_id
                    .get("model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let entry = format!("{tool}/{model}");
                if !models.contains(&entry) {
                    models.push(entry);
                }
            }
        }
    }
    let model = if models.is_empty() {
        "unknown".to_string()
    } else {
        models.join(", ")
    };

    Ok(GitAiNote {
        blame,
        schema_version,
        git_ai_version,
        model,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn branch_id_sanitizes_path_separators() {
        assert_eq!(branch_id("feature/import:gh", 42), "feature-import-gh#42");
    }

    #[test]
    fn conventional_parser_extracts_type_and_breaking_flag() {
        assert_eq!(
            ConventionalType::parse("feat(api)!: add import").unwrap(),
            ConventionalType {
                kind: "feat".to_string(),
                breaking: true,
            }
        );
        assert_eq!(
            ConventionalType::parse("refactor: simplify").unwrap(),
            ConventionalType {
                kind: "refactor".to_string(),
                breaking: false,
            }
        );
        assert!(ConventionalType::parse("Merge pull request #1").is_none());
    }

    #[test]
    fn extracts_closing_issue_ids_from_body() {
        let issues = extract_closing_issue_ids("This closes #25 and fixes #26.");
        assert!(issues.contains("#25"));
        assert!(issues.contains("#26"));
    }

    #[test]
    fn parses_gh_pr_json() {
        let json = r##"[{
            "number": 42,
            "title": "Add import",
            "body": "Closes #25",
            "url": "https://github.com/owner/repo/pull/42",
            "headRefName": "feature/import",
            "baseRefName": "main",
            "mergedAt": "2026-04-01T12:00:00Z",
            "mergeCommit": {
                "oid": "84a1d9b840d428fc523f6ffc1f8adfb43ab5918d",
                "messageHeadline": "feat: add import"
            },
            "commits": [],
            "comments": [],
            "reviews": [{"author": {"login": "bob"}, "state": "APPROVED"}],
            "closingIssuesReferences": [{
                "number": 25,
                "url": "https://github.com/owner/repo/issues/25"
            }]
        }]"##;
        let prs: Vec<GhPullRequest> = serde_json::from_str(json).unwrap();
        let imported = GitHubPullRequestImport::from_pr(prs.into_iter().next().unwrap());
        assert_eq!(imported.branch_id, "feature-import#42");
        assert!(imported.approved_by.contains("bob"));
        assert!(imported.issues.contains("#25"));
        assert!(imported
            .issue_urls
            .contains("https://github.com/owner/repo/issues/25"));
        assert_eq!(imported.commits.len(), 1);
    }
}
