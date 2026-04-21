//! `git meta setup` — alias for `git meta remote add <url> --init` that
//! reads the remote URL from a project-local `.git-meta` file.
//!
//! The intent is to let projects pin a recommended metadata remote in their
//! source tree so a fresh checkout can opt in to metadata exchange with a
//! single command instead of having to remember the URL.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

use crate::commands::remote;
use crate::context::CommandContext;

/// Filename, relative to the repo work tree, that holds the recommended
/// metadata remote URL.
const SETUP_FILE: &str = ".git-meta";

/// Default remote name used by `git meta setup`. Matches the default of
/// `git meta remote add --name`.
const DEFAULT_REMOTE_NAME: &str = "meta";

/// Run `git meta setup`.
///
/// Reads `.git-meta` from the repository work tree, resolves the metadata
/// remote URL from it, and delegates to [`remote::run_add`] with `--init`
/// enabled.
///
/// # Errors
///
/// Returns an error if the repository is bare, if `.git-meta` is missing or
/// contains no usable URL, or if the underlying `remote add` fails (e.g.
/// the remote name is already configured or the push to the new remote
/// fails).
pub fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();

    let workdir = repo
        .workdir()
        .ok_or_else(|| anyhow!("git meta setup requires a non-bare repository"))?;
    let setup_path = workdir.join(SETUP_FILE);

    let url = read_setup_url(&setup_path)?;

    eprintln!("Using metadata remote URL from {}", setup_path.display());
    remote::run_add(&url, DEFAULT_REMOTE_NAME, None, true)
}

/// Read the metadata remote URL from a `.git-meta` file.
///
/// The file format is intentionally minimal:
///
/// - Blank lines are ignored.
/// - Lines whose first non-whitespace character is `#` are treated as
///   comments and ignored.
/// - The first remaining line, with surrounding whitespace stripped, is
///   returned as the URL.
///
/// Anything after the first usable line is ignored, so projects can append
/// human-readable notes underneath the URL without breaking parsing.
///
/// # Errors
///
/// Returns an error if the file does not exist, cannot be read, or contains
/// only blank/comment lines.
fn read_setup_url(path: &Path) -> Result<String> {
    if !path.exists() {
        bail!(
            "no {SETUP_FILE} file found at {display}\n\n\
             Create one with the metadata remote URL on a single line, e.g.:\n  \
             echo 'git@github.com:org/project-meta.git' > {display}\n\n\
             Or run `git meta remote add <url> --init` directly to skip the alias.",
            display = path.display(),
        );
    }

    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read {SETUP_FILE} at {}", path.display()))?;
    parse_setup_url(&raw)
        .ok_or_else(|| {
            anyhow!(
                "{} is empty or contains no metadata remote URL\n\n\
                 Add a single non-comment line with the URL, for example:\n  \
                 git@github.com:org/project-meta.git",
                path.display(),
            )
        })
        .map(str::to_string)
        .map(strip_optional_trailing_slash_owned)
}

/// Pure parser used by [`read_setup_url`] and unit-tested in isolation.
///
/// Returns the first non-blank, non-comment line trimmed of surrounding
/// whitespace, or `None` if the input has no such line.
fn parse_setup_url(contents: &str) -> Option<&str> {
    contents.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            None
        } else {
            Some(trimmed)
        }
    })
}

/// Trim a single trailing slash from the URL, so users can paste either
/// `https://example.com/foo/bar` or `https://example.com/foo/bar/` and end
/// up with the same configured remote URL.
fn strip_optional_trailing_slash_owned(url: String) -> String {
    if url.len() > 1 && url.ends_with('/') {
        let mut s = url;
        s.pop();
        s
    } else {
        url
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_line_url() {
        assert_eq!(
            parse_setup_url("git@github.com:org/repo.git\n"),
            Some("git@github.com:org/repo.git"),
        );
    }

    #[test]
    fn ignores_blank_and_comment_lines() {
        let input = "\n\
                     # this is a comment\n\
                     \n\
                        # indented comment\n\
                     git@github.com:org/repo.git\n\
                     # trailing notes ignored\n";
        assert_eq!(parse_setup_url(input), Some("git@github.com:org/repo.git"));
    }

    #[test]
    fn returns_none_for_empty_input() {
        assert_eq!(parse_setup_url(""), None);
    }

    #[test]
    fn returns_none_for_only_comments() {
        assert_eq!(parse_setup_url("# only a comment\n\n# another\n"), None);
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(
            parse_setup_url("   git@github.com:org/repo.git   \n"),
            Some("git@github.com:org/repo.git"),
        );
    }

    #[test]
    fn first_url_wins_when_multiple_lines() {
        let input = "\
                     https://example.com/first.git\n\
                     https://example.com/second.git\n";
        assert_eq!(parse_setup_url(input), Some("https://example.com/first.git"));
    }

    #[test]
    fn strips_single_trailing_slash() {
        assert_eq!(
            strip_optional_trailing_slash_owned("https://example.com/foo/".to_string()),
            "https://example.com/foo"
        );
    }

    #[test]
    fn keeps_lone_slash() {
        assert_eq!(
            strip_optional_trailing_slash_owned("/".to_string()),
            "/"
        );
    }

    #[test]
    fn read_setup_url_missing_file_errors_helpfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".git-meta");
        let err = read_setup_url(&path).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("no .git-meta file found"), "got: {msg}");
        assert!(msg.contains("--init"), "got: {msg}");
    }

    #[test]
    fn read_setup_url_empty_file_errors_helpfully() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".git-meta");
        std::fs::write(&path, "# only a comment\n\n").unwrap();
        let err = read_setup_url(&path).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty or contains no metadata remote URL"), "got: {msg}");
    }
}
