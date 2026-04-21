//! Optional pager for command output, modelled on `git`'s pager logic.
//!
//! The intent is parity with `git log` so users get familiar behaviour:
//!
//! 1. Pager is only used when stdout is a terminal — output piped to
//!    another program or redirected to a file is never run through a
//!    pager.
//! 2. The pager program is resolved in the same priority order as
//!    `git`: `GIT_PAGER` → `core.pager` (from the open repo) → `PAGER`
//!    → `less`. An explicit empty string or the literal `cat` disables
//!    paging at any link in the chain.
//! 3. The pager is invoked through `sh -c "<pager>"` so users can keep
//!    arguments inline (e.g. `PAGER='less -SR'`).
//! 4. Pager-friendly defaults are exported into the child env iff the
//!    user has not already set them: `LESS=FRX`, `LV=-c`, `MORE=FRX`.
//!    These match git's `PAGER_ENV` build constant.
//! 5. `GIT_META_PAGER_IN_USE=1` is set in the child so nested
//!    invocations can detect they are already paginating.
//!
//! On the Rust side, [`Pager`] implements [`std::io::Write`]. Writes
//! after the user closes the pager (e.g. `q` in `less`) are silently
//! dropped instead of erroring with `BrokenPipe`, so the calling
//! command never has to special-case "user quit early". Dropping the
//! [`Pager`] closes the pipe and waits for the pager to exit.

use std::env;
use std::io::{self, IsTerminal, Write};
use std::process::{Child, ChildStdin, Command, Stdio};

/// Output sink for a command that wants to pipe stdout through a pager
/// when running in a terminal, falling back to plain stdout otherwise.
///
/// Construct with [`Pager::start`]. The returned value implements
/// [`std::io::Write`]; commands should funnel all user-facing output
/// through it instead of `println!`/`print!` so the pager actually
/// receives the bytes.
///
/// On `Drop`, the pipe to the pager is closed (signalling EOF) and the
/// child process is reaped. Calling code does not need to invoke any
/// explicit shutdown method.
pub struct Pager {
    inner: Inner,
}

enum Inner {
    /// No paging — writes go directly to stdout.
    Stdout,
    /// Output is being piped through a child pager process.
    Pager {
        /// Pipe to the pager. Wrapped in `Option` so [`Drop`] can take
        /// it and drop it *before* `wait()`-ing the child, which is
        /// required for the pager to ever observe EOF on stdin.
        stdin: Option<ChildStdin>,
        /// Handle to the child pager process; reaped in [`Drop`].
        child: Child,
        /// Once a write has returned `BrokenPipe` (the user pressed
        /// `q`, the pager crashed, …) further writes become no-ops so
        /// the calling command can finish its normal control flow
        /// without seeing the error.
        broken: bool,
    },
}

impl Pager {
    /// Start a pager for command output, mirroring `git`'s behaviour.
    ///
    /// `repo` is consulted for the `core.pager` git config entry; pass
    /// `None` to skip the config lookup (useful when no repo is open).
    /// If stdout is not a TTY, or the resolved pager is empty / `cat`,
    /// or spawning the pager fails for any reason, this returns a
    /// `Pager` that writes directly to stdout — the caller never has
    /// to branch on whether paging is active.
    pub fn start(repo: Option<&gix::Repository>) -> Self {
        let pager = resolve_pager(
            io::stdout().is_terminal(),
            |key| std::env::var(key),
            repo.and_then(read_core_pager),
        );

        match pager.and_then(|p| spawn(&p)) {
            Some((stdin, child)) => Self {
                inner: Inner::Pager {
                    stdin: Some(stdin),
                    child,
                    broken: false,
                },
            },
            None => Self {
                inner: Inner::Stdout,
            },
        }
    }
}

/// Resolve which pager program to run, given the current environment
/// and (optionally) the value of `core.pager` from git config.
///
/// Pure function for testability — no environment or process I/O is
/// performed inside. Mirrors `git_pager()` in `git/pager.c`.
///
/// Returns `Some(program)` if and only if a real pager should be
/// spawned. Returns `None` when stdout is not a terminal, the
/// resolution chain ends in an empty string or the literal `"cat"`, or
/// `GIT_META_PAGER_IN_USE` is already set (a parent already paginated).
fn resolve_pager(
    stdout_is_tty: bool,
    env_var: impl Fn(&str) -> Result<String, env::VarError>,
    core_pager: Option<String>,
) -> Option<String> {
    if !stdout_is_tty {
        return None;
    }
    if env_var("GIT_META_PAGER_IN_USE").is_ok() {
        return None;
    }

    // Treat empty strings the same as "unset" so something like
    // `PAGER='' git meta log` falls through to the next link in the
    // chain instead of trying to spawn an empty command.
    let from_env = |key: &str| env_var(key).ok().filter(|s| !s.is_empty());

    let pager = from_env("GIT_PAGER")
        .or_else(|| core_pager.filter(|s| !s.is_empty()))
        .or_else(|| from_env("PAGER"))
        .unwrap_or_else(|| "less".to_string());

    if pager.is_empty() || pager == "cat" {
        return None;
    }
    Some(pager)
}

/// Read `core.pager` from the open repository's config snapshot.
///
/// Returns `None` if the key is unset or the value is empty after
/// conversion to `String`.
fn read_core_pager(repo: &gix::Repository) -> Option<String> {
    let config = repo.config_snapshot();
    config
        .string("core.pager")
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
}

/// Spawn the pager command via `sh -c` so the caller can include
/// arguments inline (e.g. `PAGER='less -SR'`).
///
/// Pager-friendly defaults (`LESS=FRX`, `LV=-c`, `MORE=FRX`) are added
/// to the child environment only when the user has not already set
/// them, matching git's `setup_pager_env` semantics. Returns `None` if
/// the spawn itself fails or the child somehow has no stdin pipe.
fn spawn(pager: &str) -> Option<(ChildStdin, Child)> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(pager);
    cmd.stdin(Stdio::piped());

    // Match git's PAGER_ENV: `LESS=FRX LV=-c MORE=FRX`. `F` quits
    // less if the output fits on one screen, `R` lets ANSI colour
    // codes pass through unmangled, and `X` skips the screen
    // init/deinit so quitting doesn't wipe the output.
    if env::var_os("LESS").is_none() {
        cmd.env("LESS", "FRX");
    }
    if env::var_os("LV").is_none() {
        cmd.env("LV", "-c");
    }
    if env::var_os("MORE").is_none() {
        cmd.env("MORE", "FRX");
    }

    cmd.env("GIT_META_PAGER_IN_USE", "1");

    let mut child = cmd.spawn().ok()?;
    let stdin = child.stdin.take()?;
    Some((stdin, child))
}

impl Write for Pager {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = match &mut self.inner {
            Inner::Stdout => io::stdout().write(buf),
            Inner::Pager {
                stdin: Some(stdin),
                broken: false,
                ..
            } => stdin.write(buf),
            // Pager already detached or marked broken — silently drop.
            _ => return Ok(buf.len()),
        };
        match result {
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                if let Inner::Pager { broken, .. } = &mut self.inner {
                    *broken = true;
                }
                Ok(buf.len())
            }
            other => other,
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let result = match &mut self.inner {
            Inner::Stdout => io::stdout().flush(),
            Inner::Pager {
                stdin: Some(stdin),
                broken: false,
                ..
            } => stdin.flush(),
            _ => return Ok(()),
        };
        match result {
            Err(e) if e.kind() == io::ErrorKind::BrokenPipe => {
                if let Inner::Pager { broken, .. } = &mut self.inner {
                    *broken = true;
                }
                Ok(())
            }
            other => other,
        }
    }
}

impl Drop for Pager {
    fn drop(&mut self) {
        if let Inner::Pager { stdin, child, .. } = &mut self.inner {
            // Drop the pipe handle so the pager observes EOF and
            // exits, then reap the child to avoid a zombie.
            *stdin = None;
            let _ = child.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a fake env lookup closure backed by a `HashMap` so each
    /// test can specify exactly the variables it cares about without
    /// touching the real process environment.
    fn fake_env<'a>(
        pairs: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Result<String, env::VarError> + 'a {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        move |k: &str| {
            map.get(k)
                .map(|v| (*v).to_string())
                .ok_or(env::VarError::NotPresent)
        }
    }

    #[test]
    fn returns_none_when_stdout_is_not_a_tty() {
        let pager = resolve_pager(false, fake_env(&[]), None);
        assert_eq!(pager, None);
    }

    #[test]
    fn defaults_to_less_when_nothing_is_configured() {
        let pager = resolve_pager(true, fake_env(&[]), None);
        assert_eq!(pager.as_deref(), Some("less"));
    }

    #[test]
    fn git_pager_env_takes_precedence_over_everything_else() {
        let pager = resolve_pager(
            true,
            fake_env(&[("GIT_PAGER", "delta"), ("PAGER", "more")]),
            Some("most".to_string()),
        );
        assert_eq!(pager.as_deref(), Some("delta"));
    }

    #[test]
    fn core_pager_config_beats_pager_env() {
        let pager = resolve_pager(
            true,
            fake_env(&[("PAGER", "more")]),
            Some("delta".to_string()),
        );
        assert_eq!(pager.as_deref(), Some("delta"));
    }

    #[test]
    fn pager_env_used_when_no_higher_priority_value() {
        let pager = resolve_pager(true, fake_env(&[("PAGER", "bat")]), None);
        assert_eq!(pager.as_deref(), Some("bat"));
    }

    #[test]
    fn empty_string_in_chain_falls_through_to_next_link() {
        // GIT_PAGER explicitly empty → should not stop the chain at
        // GIT_PAGER, should fall through to core.pager.
        let pager = resolve_pager(
            true,
            fake_env(&[("GIT_PAGER", ""), ("PAGER", "more")]),
            Some("delta".to_string()),
        );
        assert_eq!(pager.as_deref(), Some("delta"));
    }

    #[test]
    fn cat_disables_paging_entirely() {
        let pager = resolve_pager(true, fake_env(&[("GIT_PAGER", "cat")]), None);
        assert_eq!(pager, None);
    }

    #[test]
    fn nested_invocation_skips_paging() {
        // If a parent process already set GIT_META_PAGER_IN_USE we are
        // already paginating and should not spawn a second pager.
        let pager = resolve_pager(true, fake_env(&[("GIT_META_PAGER_IN_USE", "1")]), None);
        assert_eq!(pager, None);
    }

    #[test]
    fn multi_word_pager_command_is_preserved_for_shell() {
        // Pager strings frequently contain arguments; the resolver
        // must preserve them verbatim so `sh -c` can run them.
        let pager = resolve_pager(true, fake_env(&[("PAGER", "less -SR")]), None);
        assert_eq!(pager.as_deref(), Some("less -SR"));
    }
}
