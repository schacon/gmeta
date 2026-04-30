use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use terminal_size::{terminal_size, Width};

use crate::commands::hydrate::hydrate_promised_entries;
use crate::context::CommandContext;
use crate::pager::Pager;
use crate::style::Style;
use git_meta_lib::types::{Target, TargetType};
use git_meta_lib::MetaValue;

/// Run PR-oriented blame for `path`.
///
/// # Parameters
///
/// - `path`: the repository path to blame.
/// - `rev`: optional revision to pass to `git blame`.
/// - `porcelain`: when true, emit full JSON grouped blame data.
/// - `json`: when true, emit compact JSON with line ranges and PR metadata.
///
/// # Errors
///
/// Returns an error if `git blame` fails, porcelain output cannot be parsed, or
/// metadata lookup/output serialization fails.
pub fn run(path: &str, rev: Option<&str>, porcelain: bool, json: bool) -> Result<()> {
    if porcelain && json {
        bail!("cannot use --json and --porcelain together");
    }
    let ctx = CommandContext::open(None)?;
    let output = run_git_blame(path, rev)?;
    let lines = parse_porcelain(&output)?;
    hydrate_blame_metadata(&ctx.session, &lines)?;
    let groups = group_blame(ctx.session.store(), &lines)?;

    if porcelain {
        println!("{}", serde_json::to_string_pretty(&groups)?);
    } else if json {
        println!("{}", serde_json::to_string_pretty(&json_groups(&groups))?);
    } else {
        let mut out = Pager::start(Some(ctx.session.repo()));
        print_text(&mut out, &groups)?;
    }

    Ok(())
}

const COMMIT_BRANCH_KEY: &str = "branch-id";
const BRANCH_METADATA_KEYS: &[&str] = &[
    "title",
    "review:number",
    "review:url",
    "commits:author",
    "commits:author-date",
    "review:reviewed",
    "review:approved",
    "released-in",
];

fn run_git_blame(path: &str, rev: Option<&str>) -> Result<String> {
    let mut command = Command::new("git");
    command.args(["blame", "--porcelain"]);
    if let Some(rev) = rev {
        command.arg(rev);
    }
    command.args(["--", path]);
    let output = command
        .output()
        .with_context(|| format!("failed to run git blame for {path}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git blame failed for {path}: {}", stderr.trim());
    }
    String::from_utf8(output.stdout).context("git blame output was not valid UTF-8")
}

#[derive(Debug, Clone)]
struct BlameLine {
    commit: String,
    original_line: u32,
    final_line: u32,
    author: Option<String>,
    author_time: Option<String>,
    summary: Option<String>,
    previous: Option<String>,
    text: String,
}

#[derive(Default)]
struct PendingLine {
    commit: String,
    original_line: u32,
    final_line: u32,
    metadata: HashMap<String, String>,
}

fn parse_porcelain(output: &str) -> Result<Vec<BlameLine>> {
    let mut lines = Vec::new();
    let mut cached_metadata: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut pending: Option<PendingLine> = None;

    for line in output.lines() {
        if let Some(header) = parse_header(line) {
            pending = Some(header);
            continue;
        }

        if let Some(text) = line.strip_prefix('\t') {
            let Some(current) = pending.take() else {
                bail!("git blame porcelain source line appeared before a header");
            };
            let mut metadata = cached_metadata
                .get(&current.commit)
                .cloned()
                .unwrap_or_default();
            metadata.extend(current.metadata);
            cached_metadata.insert(current.commit.clone(), metadata.clone());
            lines.push(BlameLine {
                commit: current.commit,
                original_line: current.original_line,
                final_line: current.final_line,
                author: metadata.get("author").cloned(),
                author_time: metadata.get("author-time").cloned(),
                summary: metadata.get("summary").cloned(),
                previous: metadata.get("previous").cloned(),
                text: text.to_string(),
            });
            continue;
        }

        if let Some(current) = pending.as_mut() {
            if let Some((key, value)) = line.split_once(' ') {
                current.metadata.insert(key.to_string(), value.to_string());
            }
        }
    }

    Ok(lines)
}

fn parse_header(line: &str) -> Option<PendingLine> {
    let mut parts = line.split_whitespace();
    let commit = parts.next()?;
    if commit.len() < 40 || !commit.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let original_line = parts.next()?.parse::<u32>().ok()?;
    let final_line = parts.next()?.parse::<u32>().ok()?;
    Some(PendingLine {
        commit: commit.to_string(),
        original_line,
        final_line,
        metadata: HashMap::new(),
    })
}

#[derive(Debug, Clone, Default, Serialize)]
struct BranchMetadata {
    title: Option<String>,
    review_number: Option<String>,
    review_url: Option<String>,
    commit_authors: Vec<String>,
    commit_author_dates: Vec<String>,
    reviewed_by: Vec<String>,
    approved_by: Vec<String>,
    released_in: Vec<String>,
}

#[derive(Debug, Serialize)]
struct BlameGroup {
    start_line: u32,
    end_line: u32,
    commit: String,
    branch_id: Option<String>,
    branch: Option<BranchMetadata>,
    author: Option<String>,
    author_time: Option<String>,
    summary: Option<String>,
    previous: Option<String>,
    lines: Vec<GroupLine>,
}

#[derive(Debug, Serialize)]
struct GroupLine {
    line: u32,
    original_line: u32,
    text: String,
}

#[derive(Debug, Serialize)]
struct JsonBlameGroup {
    start_line: u32,
    end_line: u32,
    commit: String,
    branch_id: Option<String>,
    pr: Option<JsonPullRequest>,
    author: Option<String>,
    summary: Option<String>,
}

#[derive(Debug, Serialize)]
struct JsonPullRequest {
    number: Option<String>,
    title: Option<String>,
    url: Option<String>,
    commit_authors: Vec<String>,
    commit_author_dates: Vec<String>,
    commit_author_date_range: Option<String>,
    reviewed_by: Vec<String>,
    approved_by: Vec<String>,
    released_in: Vec<String>,
}

fn json_groups(groups: &[BlameGroup]) -> Vec<JsonBlameGroup> {
    groups
        .iter()
        .map(|group| JsonBlameGroup {
            start_line: group.start_line,
            end_line: group.end_line,
            commit: group.commit.clone(),
            branch_id: group.branch_id.clone(),
            pr: group.branch.as_ref().map(|branch| JsonPullRequest {
                number: branch.review_number.clone(),
                title: branch.title.clone(),
                url: branch.review_url.clone(),
                commit_authors: branch.commit_authors.clone(),
                commit_author_dates: branch.commit_author_dates.clone(),
                commit_author_date_range: author_date_range(&branch.commit_author_dates),
                reviewed_by: branch.reviewed_by.clone(),
                approved_by: branch.approved_by.clone(),
                released_in: branch.released_in.clone(),
            }),
            author: group.author.clone(),
            summary: group.summary.clone(),
        })
        .collect()
}

fn group_blame(db: &git_meta_lib::db::Store, lines: &[BlameLine]) -> Result<Vec<BlameGroup>> {
    let commit_branch_ids = load_commit_branch_ids(db, lines)?;

    let mut branch_metadata = BTreeMap::new();
    for branch_id in commit_branch_ids.values().filter_map(Option::as_ref) {
        if !branch_metadata.contains_key(branch_id) {
            branch_metadata.insert(branch_id.clone(), load_branch_metadata(db, branch_id)?);
        }
    }

    let mut groups = Vec::new();
    for line in lines {
        let branch_id = commit_branch_ids
            .get(&line.commit)
            .and_then(std::clone::Clone::clone);
        let identity = branch_id.clone().unwrap_or_else(|| line.commit.clone());
        let should_extend = groups.last().is_some_and(|group: &BlameGroup| {
            let group_identity = group
                .branch_id
                .clone()
                .unwrap_or_else(|| group.commit.clone());
            group_identity == identity && group.end_line + 1 == line.final_line
        });

        if should_extend {
            if let Some(group) = groups.last_mut() {
                group.end_line = line.final_line;
                group.lines.push(GroupLine {
                    line: line.final_line,
                    original_line: line.original_line,
                    text: line.text.clone(),
                });
            }
            continue;
        }

        let branch = branch_id
            .as_ref()
            .and_then(|id| branch_metadata.get(id))
            .cloned();
        groups.push(BlameGroup {
            start_line: line.final_line,
            end_line: line.final_line,
            commit: line.commit.clone(),
            branch_id,
            branch,
            author: line.author.clone(),
            author_time: line.author_time.clone(),
            summary: line.summary.clone(),
            previous: line.previous.clone(),
            lines: vec![GroupLine {
                line: line.final_line,
                original_line: line.original_line,
                text: line.text.clone(),
            }],
        });
    }

    Ok(groups)
}

fn hydrate_blame_metadata(session: &git_meta_lib::Session, lines: &[BlameLine]) -> Result<()> {
    let commits = lines
        .iter()
        .map(|line| line.commit.clone())
        .collect::<BTreeSet<_>>();
    let promised_commit_keys = promised_entries(
        session.store(),
        &TargetType::Commit,
        &commits,
        &[COMMIT_BRANCH_KEY],
    )?;
    if !promised_commit_keys.is_empty() {
        hydrate_promised_entries(session, &TargetType::Commit, &promised_commit_keys)?;
    }

    let branch_ids = load_commit_branch_ids(session.store(), lines)?
        .values()
        .filter_map(std::clone::Clone::clone)
        .collect::<BTreeSet<_>>();
    let promised_branch_keys = promised_entries(
        session.store(),
        &TargetType::Branch,
        &branch_ids,
        BRANCH_METADATA_KEYS,
    )?;
    if !promised_branch_keys.is_empty() {
        hydrate_promised_entries(session, &TargetType::Branch, &promised_branch_keys)?;
    }

    Ok(())
}

fn promised_entries(
    db: &git_meta_lib::db::Store,
    target_type: &TargetType,
    target_values: &BTreeSet<String>,
    keys: &[&str],
) -> Result<Vec<(String, String)>> {
    let keys = keys.iter().copied().collect::<HashSet<_>>();
    let entries = db
        .get_promised_keys()?
        .into_iter()
        .filter_map(|(target_type_str, target_value, key)| {
            let parsed_type = target_type_str.parse::<TargetType>().ok()?;
            (parsed_type == *target_type
                && target_values.contains(&target_value)
                && keys.contains(key.as_str()))
            .then_some((target_value, key))
        })
        .collect::<Vec<_>>();
    Ok(entries)
}

fn load_commit_branch_ids(
    db: &git_meta_lib::db::Store,
    lines: &[BlameLine],
) -> Result<BTreeMap<String, Option<String>>> {
    let mut commit_branch_ids = BTreeMap::new();
    for commit in lines
        .iter()
        .map(|line| line.commit.as_str())
        .collect::<HashSet<_>>()
    {
        let target = Target::from_parts(TargetType::Commit, Some(commit.to_string()));
        let branch_id = match db.get_value(&target, COMMIT_BRANCH_KEY)? {
            Some(MetaValue::String(value)) if !value.is_empty() => Some(value),
            _ => None,
        };
        commit_branch_ids.insert(commit.to_string(), branch_id);
    }
    Ok(commit_branch_ids)
}

fn load_branch_metadata(db: &git_meta_lib::db::Store, branch_id: &str) -> Result<BranchMetadata> {
    let target = Target::branch(branch_id);
    Ok(BranchMetadata {
        title: get_string(db, &target, "title")?,
        review_number: get_string(db, &target, "review:number")?,
        review_url: get_string(db, &target, "review:url")?,
        commit_authors: get_set(db, &target, "commits:author")?,
        commit_author_dates: get_set(db, &target, "commits:author-date")?,
        reviewed_by: get_set(db, &target, "review:reviewed")?,
        approved_by: get_set(db, &target, "review:approved")?,
        released_in: get_set(db, &target, "released-in")?,
    })
}

fn get_string(db: &git_meta_lib::db::Store, target: &Target, key: &str) -> Result<Option<String>> {
    match db.get_value(target, key)? {
        Some(MetaValue::String(value)) => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn get_set(db: &git_meta_lib::db::Store, target: &Target, key: &str) -> Result<Vec<String>> {
    match db.get_value(target, key)? {
        Some(MetaValue::Set(values)) => Ok(values.into_iter().collect()),
        _ => Ok(Vec::new()),
    }
}

fn print_text(out: &mut impl Write, groups: &[BlameGroup]) -> Result<()> {
    let style = Style::detect_stdout();
    let width = terminal_width();
    for group in groups {
        let range = format!("{}-{}", group.start_line, group.end_line);
        let pr_number = group
            .branch
            .as_ref()
            .and_then(|branch| branch.review_number.as_deref())
            .map(|number| format!("#{number}"));
        let description = group_description(group);
        let url = group
            .branch
            .as_ref()
            .and_then(|branch| branch.review_url.as_deref());
        let details = detail_lines(group);
        print_section_header(
            out,
            &style,
            &range,
            pr_number.as_deref(),
            &description,
            url,
            &details,
            width,
        )?;
        for line in &group.lines {
            let line_number = format!("{:>6}", line.line);
            let separator = " | ";
            let prefix_width = line_number.chars().count() + separator.chars().count();
            let text_width = width.saturating_sub(prefix_width);
            writeln!(
                out,
                "{}{}{}",
                ansi(&style, "36", &line_number),
                style.dim(separator),
                truncate_to_width(&line.text, text_width)
            )?;
        }
    }
    Ok(())
}

fn group_description(group: &BlameGroup) -> String {
    group
        .branch
        .as_ref()
        .and_then(|branch| branch.title.clone())
        .or_else(|| group.branch_id.clone())
        .or_else(|| group.summary.clone())
        .unwrap_or_else(|| group.commit[..8.min(group.commit.len())].to_string())
}

fn detail_lines(group: &BlameGroup) -> Vec<DetailLine> {
    if let Some(branch) = &group.branch {
        branch_detail_lines(branch)
    } else {
        commit_detail_lines(group)
    }
}

fn print_section_header(
    out: &mut impl Write,
    style: &Style,
    range: &str,
    pr_number: Option<&str>,
    description: &str,
    url: Option<&str>,
    details: &[DetailLine],
    width: usize,
) -> Result<()> {
    let max_header_width = width.saturating_sub(1).max(1);
    let header = visible_header(range, pr_number, description);
    let mut lines = vec![truncate_to_width(&header, max_header_width)];
    if let Some(url) = url {
        lines.push(truncate_to_width(url, max_header_width));
    }
    lines.extend(details.iter().map(|detail| {
        truncate_to_width(
            &format!("{}: {}", detail.label, detail.value),
            max_header_width,
        )
    }));
    let box_width = lines
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(1);
    let horizontal = "─".repeat(box_width);
    writeln!(out, "{}", ansi(style, "32", &format!("{horizontal}╮")))?;
    for (idx, line) in lines.iter().enumerate() {
        let padding = " ".repeat(box_width.saturating_sub(line.chars().count()));
        let detail_idx = idx.checked_sub(usize::from(url.is_some()) + 1);
        let styled_line = if idx == 0 {
            styled_header(style, range, pr_number, description, box_width)
        } else if let Some(detail_idx) = detail_idx {
            details.get(detail_idx).map_or_else(
                || format!("{}{}", style.dim(line), padding),
                |detail| styled_detail_line(style, detail, box_width),
            )
        } else {
            format!("{}{}", style.dim(line), padding)
        };
        writeln!(out, "{}{}", styled_line, ansi(style, "32", "│"))?;
    }
    writeln!(out, "{}", ansi(style, "32", &format!("{horizontal}╯")))?;
    Ok(())
}

struct DetailLine {
    label: &'static str,
    value: String,
}

fn branch_detail_lines(branch: &BranchMetadata) -> Vec<DetailLine> {
    let mut lines = Vec::new();
    if let Some(authors) = author_email_list(&branch.commit_authors) {
        lines.push(DetailLine {
            label: "authors",
            value: authors,
        });
    }
    if let Some(date_range) = author_date_range(&branch.commit_author_dates) {
        lines.push(DetailLine {
            label: "dates",
            value: date_range,
        });
    }
    lines
}

fn commit_detail_lines(group: &BlameGroup) -> Vec<DetailLine> {
    let mut lines = vec![DetailLine {
        label: "commit",
        value: group.commit[..8.min(group.commit.len())].to_string(),
    }];
    if let Some(author) = &group.author {
        lines.push(DetailLine {
            label: "author",
            value: author.clone(),
        });
    }
    if let Some(date) = group.author_time.as_deref().and_then(format_author_time) {
        lines.push(DetailLine {
            label: "date",
            value: date,
        });
    }
    lines
}

fn styled_detail_line(style: &Style, detail: &DetailLine, box_width: usize) -> String {
    let visible = format!("{}: {}", detail.label, detail.value);
    let visible = truncate_to_width(&visible, box_width);
    let label = format!("{}:", detail.label);
    if !visible.starts_with(&label) {
        let padding = " ".repeat(box_width.saturating_sub(visible.chars().count()));
        return format!("{}{}", style.dim(&visible), padding);
    }

    let value = visible[label.len()..].to_string();
    let padding = " ".repeat(box_width.saturating_sub(visible.chars().count()));
    format!(
        "{}{}{}",
        ansi(style, "34", &label),
        style.dim(&value),
        padding
    )
}

fn author_email_list(authors: &[String]) -> Option<String> {
    let emails = authors
        .iter()
        .map(|author| extract_email(author).unwrap_or_else(|| author.clone()))
        .filter(|author| !author.trim().is_empty())
        .collect::<Vec<_>>();
    (!emails.is_empty()).then(|| emails.join(", "))
}

fn extract_email(author: &str) -> Option<String> {
    let start = author.find('<')?;
    let end = author[start + 1..].find('>')? + start + 1;
    Some(author[start + 1..end].to_string())
}

fn author_date_range(author_dates: &[String]) -> Option<String> {
    let mut timestamps = author_dates
        .iter()
        .filter_map(|value| value.parse::<i64>().ok())
        .collect::<Vec<_>>();
    if timestamps.is_empty() {
        return None;
    }
    timestamps.sort_unstable();
    let min = format_author_date(*timestamps.first()?);
    let max = format_author_date(*timestamps.last()?);
    if min == max {
        Some(min)
    } else {
        Some(format!("{min}..{max}"))
    }
}

fn format_author_time(value: &str) -> Option<String> {
    value.parse::<i64>().ok().map(format_author_date)
}

fn format_author_date(seconds: i64) -> String {
    let format = time::macros::format_description!("[year]-[month]-[day]");
    time::OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .and_then(|date| date.format(format).ok())
        .unwrap_or_else(|| seconds.to_string())
}

fn visible_header(range: &str, pr_number: Option<&str>, description: &str) -> String {
    match pr_number {
        Some(pr_number) => format!("{range} {pr_number} {description}"),
        None => format!("{range} {description}"),
    }
}

fn styled_header(
    style: &Style,
    range: &str,
    pr_number: Option<&str>,
    description: &str,
    box_width: usize,
) -> String {
    let visible = visible_header(range, pr_number, description);
    let visible = truncate_to_width(&visible, box_width);
    let desc_start = match pr_number {
        Some(pr_number) => range.chars().count() + 1 + pr_number.chars().count() + 1,
        None => range.chars().count() + 1,
    };
    let range_len = range.chars().count().min(visible.chars().count());
    let pr_start = range_len + 1;
    let pr_len = pr_number
        .map(|number| number.chars().count())
        .unwrap_or_default();

    let mut out = String::new();
    let visible_chars = visible.chars().collect::<Vec<_>>();
    let range_text = visible_chars.iter().take(range_len).collect::<String>();
    out.push_str(&ansi(style, "36", &range_text));

    if let Some(pr_number) = pr_number {
        if visible_chars.len() > range_len {
            out.push(' ');
        }
        let available_pr = visible_chars.len().saturating_sub(pr_start).min(pr_len);
        if available_pr > 0 {
            let pr_text = visible_chars
                .iter()
                .skip(pr_start)
                .take(available_pr)
                .collect::<String>();
            out.push_str(&style.warn(&pr_text));
        } else if !pr_number.is_empty() {
            let _ = pr_number;
        }
    }

    if visible_chars.len() > desc_start {
        if !out.ends_with(' ') {
            out.push(' ');
        }
        let desc_text = visible_chars.iter().skip(desc_start).collect::<String>();
        out.push_str(&ansi(style, "1;34", &desc_text));
    }

    let padding = " ".repeat(box_width.saturating_sub(visible.chars().count()));
    out.push_str(&padding);
    out
}

fn ansi(style: &Style, code: &str, text: &str) -> String {
    if style.is_enabled() {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn terminal_width() -> usize {
    terminal_size()
        .map(|(Width(width), _)| usize::from(width))
        .or_else(|| {
            std::env::var("COLUMNS")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
        })
        .filter(|width| *width > 0)
        .unwrap_or(100)
}

fn truncate_to_width(value: &str, width: usize) -> String {
    const ELLIPSIS: &str = "...";
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width <= ELLIPSIS.len() {
        return ELLIPSIS[..width].to_string();
    }
    let keep = width - ELLIPSIS.len();
    let mut truncated = value.chars().take(keep).collect::<String>();
    truncated.push_str(ELLIPSIS);
    truncated
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_repeated_porcelain_metadata() {
        let input = "\
84a1d9b840d428fc523f6ffc1f8adfb43ab5918d 1 1 1
author Alice
author-time 1775001600
summary feat: add thing
filename file.txt
\tfirst
84a1d9b840d428fc523f6ffc1f8adfb43ab5918d 2 2 1
\tsecond
";

        let lines = parse_porcelain(input).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].author.as_deref(), Some("Alice"));
        assert_eq!(lines[1].author_time.as_deref(), Some("1775001600"));
        assert_eq!(lines[1].author.as_deref(), Some("Alice"));
        assert_eq!(lines[1].summary.as_deref(), Some("feat: add thing"));
    }

    #[test]
    fn truncates_to_requested_width() {
        assert_eq!(truncate_to_width("abcdef", 6), "abcdef");
        assert_eq!(truncate_to_width("abcdef", 5), "ab...");
        assert_eq!(truncate_to_width("abcdef", 2), "..");
    }
}
