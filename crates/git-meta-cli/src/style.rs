//! Shared ANSI styling helpers for terminal output.
//!
//! Two surfaces are exposed:
//!
//! - [`use_color_stdout`] / [`use_color_stderr`] — booleans that decide
//!   whether the next thing written to that stream should include SGR
//!   escape sequences.
//! - [`Style`] — a small palette of role-named helpers (`step`, `ok`,
//!   `warn`, `err`, `dim`, `bold`) that each return an owned `String`
//!   already wrapped in the correct escape codes (or no codes at all
//!   when color is disabled).
//!
//! The role helpers exist so progress output can stay free of
//! `if use_color { … } else { … }` branching at every call site:
//!
//! ```ignore
//! let s = git_meta_cli::style::Style::detect_stderr();
//! eprintln!("{} {}...", s.step("Checking"), url);
//! ```
//!
//! Color resolution order (first match wins) for both helpers:
//!
//! 1. `NO_COLOR` is set to any value → never color, per <https://no-color.org/>.
//! 2. `CLICOLOR_FORCE` is set to a non-empty value other than `"0"` →
//!    always color, even when the stream is not a TTY. This matches the
//!    convention used by `ls`, `grep`, and friends, and makes the
//!    colored path deterministically testable in `assert_cmd`.
//! 3. Otherwise, color iff the relevant stream is connected to a real
//!    terminal, so codes never leak into pipes, log files, or another
//!    program's input as raw escape garbage.

use std::io::IsTerminal;

/// Apply the env-only portion of the precedence rules.
///
/// Returns `Some(true)` / `Some(false)` if the environment forces a
/// decision, or `None` if the caller should fall back to TTY detection
/// for its own stream.
fn env_decision() -> Option<bool> {
    if std::env::var_os("NO_COLOR").is_some() {
        return Some(false);
    }
    if let Some(v) = std::env::var_os("CLICOLOR_FORCE") {
        if !v.is_empty() && v != "0" {
            return Some(true);
        }
    }
    None
}

/// Whether stdout output should include ANSI color escape sequences.
#[must_use]
pub fn use_color_stdout() -> bool {
    env_decision().unwrap_or_else(|| std::io::stdout().is_terminal())
}

/// Whether stderr output should include ANSI color escape sequences.
#[must_use]
pub fn use_color_stderr() -> bool {
    env_decision().unwrap_or_else(|| std::io::stderr().is_terminal())
}

/// Role-based ANSI styling palette.
///
/// Constructed once per call site via [`Style::detect_stderr`] /
/// [`Style::detect_stdout`] (or [`Style::always`] / [`Style::never`] in
/// tests), then reused for every message written by that command.
///
/// When color is disabled the wrap functions return their input verbatim
/// (no allocation surprises beyond the unavoidable owned `String`),
/// keeping colored and plain output a single code path.
pub struct Style {
    enabled: bool,
}

impl Style {
    /// Build a [`Style`] for messages written to stderr.
    #[must_use]
    pub fn detect_stderr() -> Self {
        Self {
            enabled: use_color_stderr(),
        }
    }

    /// Build a [`Style`] for messages written to stdout.
    #[must_use]
    pub fn detect_stdout() -> Self {
        Self {
            enabled: use_color_stdout(),
        }
    }

    /// Build a [`Style`] that always emits color, regardless of
    /// environment or TTY state. Intended for tests.
    #[cfg(test)]
    #[must_use]
    pub fn always() -> Self {
        Self { enabled: true }
    }

    /// Build a [`Style`] that never emits color. Intended for tests and
    /// for places that want explicit plain output.
    #[cfg(test)]
    #[must_use]
    pub fn never() -> Self {
        Self { enabled: false }
    }

    /// Whether this palette will emit ANSI escape codes. Useful for
    /// callers that need to branch on color state for layout (e.g.
    /// padding that should account for zero-width SGR sequences).
    #[allow(dead_code)]
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn wrap(&self, codes: &str, text: &str) -> String {
        if self.enabled {
            format!("\x1b[{codes}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    /// Style `text` as a "step in progress" label (bold cyan).
    ///
    /// Use for the leading verb of a progress line: `Checking`,
    /// `Fetching`, `Hydrating`, `Initializing`, etc.
    #[must_use]
    pub fn step(&self, text: &str) -> String {
        self.wrap("1;36", text)
    }

    /// Style `text` as a "success" label (bold green).
    ///
    /// Use for terminal status of a finished step (`done.`) and for
    /// post-action confirmations (`Added`, `Created`, `Reusing`,
    /// `Indexed`).
    #[must_use]
    pub fn ok(&self, text: &str) -> String {
        self.wrap("1;32", text)
    }

    /// Style `text` as a warning label (bold yellow).
    #[must_use]
    pub fn warn(&self, text: &str) -> String {
        self.wrap("1;33", text)
    }

    /// Style `text` as an error label (bold red).
    #[must_use]
    pub fn err(&self, text: &str) -> String {
        self.wrap("1;31", text)
    }

    /// Style `text` as a de-emphasized detail (dim).
    #[must_use]
    pub fn dim(&self, text: &str) -> String {
        self.wrap("2", text)
    }

    /// Style `text` as bold without changing color. Kept for symmetry
    /// with the colored helpers so role expansion (e.g. a future
    /// "highlight" tier) doesn't require a second helper module.
    #[allow(dead_code)]
    #[must_use]
    pub fn bold(&self, text: &str) -> String {
        self.wrap("1", text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn always_palette_wraps_with_sgr_codes() {
        let s = Style::always();
        assert_eq!(s.step("Checking"), "\x1b[1;36mChecking\x1b[0m");
        assert_eq!(s.ok("done."), "\x1b[1;32mdone.\x1b[0m");
        assert_eq!(s.warn("warning"), "\x1b[1;33mwarning\x1b[0m");
        assert_eq!(s.err("oops"), "\x1b[1;31moops\x1b[0m");
        assert_eq!(s.dim("hint"), "\x1b[2mhint\x1b[0m");
        assert_eq!(s.bold("strong"), "\x1b[1mstrong\x1b[0m");
    }

    #[test]
    fn never_palette_returns_plain_text() {
        let s = Style::never();
        assert_eq!(s.step("Checking"), "Checking");
        assert_eq!(s.ok("done."), "done.");
        assert_eq!(s.warn("warning"), "warning");
        assert_eq!(s.err("oops"), "oops");
        assert_eq!(s.dim("hint"), "hint");
        assert_eq!(s.bold("strong"), "strong");
    }

    #[test]
    fn is_enabled_reflects_constructor() {
        assert!(Style::always().is_enabled());
        assert!(!Style::never().is_enabled());
    }
}
