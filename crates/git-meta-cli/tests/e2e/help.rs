use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness;

const GROUP_HEADINGS: &[&str] = &[
    "read and write data",
    "browse and exchange (porcelain)",
    "low-level git ref operations (plumbing)",
    "setup and configuration",
];

/// Sub-section labels rendered under the "read and write data" group,
/// splitting it by the kind of value each command operates on.
const SUBGROUP_LABELS: &[&str] = &["(strings)", "(lists)", "(sets)"];

const VISIBLE_COMMANDS: &[&str] = &[
    "set",
    "get",
    "rm",
    "list:push",
    "set:add",
    "show",
    "inspect",
    "log",
    "blame",
    "stats",
    "push",
    "pull",
    "sync",
    "serialize",
    "materialize",
    "remote",
    "config",
    "teardown",
];

const HIDDEN_COMMANDS: &[&str] = &[
    "import",
    "watch",
    "promisor",
    "prune",
    "local-prune",
    "config:prune",
];

/// All four ways of asking for top-level help — bare invocation, `-h`,
/// `--help`, and the `help` pseudo-subcommand — must print the curated
/// help with every group heading and every visible command name present,
/// and with every hidden command name absent.
#[test]
fn top_level_help_is_curated_for_every_invocation() {
    let dir = TempDir::new().unwrap();

    for invocation in [&[][..], &["-h"], &["--help"], &["help"]] {
        let mut cmd = harness::git_meta(dir.path());
        let mut assertion = cmd.args(invocation).assert().success();

        for heading in GROUP_HEADINGS {
            assertion = assertion.stdout(predicate::str::contains(*heading));
        }
        for label in SUBGROUP_LABELS {
            assertion = assertion.stdout(predicate::str::contains(*label));
        }
        for name in VISIBLE_COMMANDS {
            assertion = assertion.stdout(predicate::str::contains(*name));
        }
        for name in HIDDEN_COMMANDS {
            assertion = assertion.stdout(predicate::str::contains(*name).not());
        }
    }
}

/// Within the porcelain group, the daily-use sync commands `push`,
/// `pull`, and `sync` come first as their own block, separated from the read-only
/// inspection commands (`show`, `inspect`, `log`, `stats`) by a blank
/// line. We verify both the order and the blank-line separator by
/// matching a multi-line snippet of stdout.
#[test]
fn porcelain_group_lists_exchange_commands_before_inspection_commands() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .assert()
        .success()
        .stdout(predicate::function(|out: &str| {
            let push = out.find("   push ").expect("push line missing");
            let pull = out.find("   pull ").expect("pull line missing");
            let sync = out.find("   sync ").expect("sync line missing");
            let show = out.find("   show ").expect("show line missing");
            let stats = out.find("   stats ").expect("stats line missing");
            push < pull && pull < sync && sync < show && show < stats
        }))
        .stdout(predicate::str::contains(
            "Pull, merge, rewrite if needed, and push metadata\n\n   show",
        ));
}

/// Hiding a command from the curated help must not disable it. Hidden
/// commands like `import` should still be invokable directly and produce
/// their own clap-generated per-subcommand help.
#[test]
fn hidden_commands_remain_invokable() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .args(["import", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Import metadata"));
}

/// `git meta <subcommand> --help` must continue to fall through to
/// clap's per-subcommand help; only the *top-level* `--help` is
/// intercepted by the custom help printer.
#[test]
fn subcommand_help_still_uses_clap() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .args(["set", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: git-meta set"))
        .stdout(predicate::str::contains("<TARGET>"));
}

/// By default — under `assert_cmd`, stdout is captured (not a TTY) — the
/// curated help must be plain ASCII with no ANSI escape sequences.
/// Embedding raw escape codes in pipes, log files, or another program's
/// input is exactly the failure mode the TTY check is designed to
/// prevent.
#[test]
fn top_level_help_omits_color_when_not_a_tty() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .env_remove("CLICOLOR_FORCE")
        .assert()
        .success()
        .stdout(predicate::str::contains("\x1b[").not());
}

/// `CLICOLOR_FORCE=1` must force ANSI styling on regardless of TTY
/// detection, matching the convention used by `ls`, `grep`, etc. We
/// assert the specific SGR sequences the palette emits for each role:
/// bold for `usage:`, bold + yellow for group headings, dim for
/// `(strings)` / `(lists)` / `(sets)` sub-labels, green for command
/// names, and dim for the footer hints.
#[test]
fn top_level_help_emits_color_when_forced() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .env_remove("NO_COLOR")
        .env("CLICOLOR_FORCE", "1")
        .assert()
        .success()
        .stdout(predicate::str::contains("\x1b[1musage:\x1b[0m"))
        .stdout(predicate::str::contains(
            "\x1b[1m\x1b[33mread and write data\x1b[0m",
        ))
        .stdout(predicate::str::contains("\x1b[2m(strings)\x1b[0m"))
        .stdout(predicate::str::contains("\x1b[2m(lists)\x1b[0m"))
        .stdout(predicate::str::contains("\x1b[2m(sets)\x1b[0m"))
        .stdout(predicate::str::contains("\x1b[32mset"))
        .stdout(predicate::str::contains("\x1b[2mRun 'git meta"));
}

/// `NO_COLOR` must win over `CLICOLOR_FORCE` (per <https://no-color.org/>),
/// so an environment that sets both still produces plain output.
#[test]
fn no_color_overrides_clicolor_force() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .env("NO_COLOR", "1")
        .env("CLICOLOR_FORCE", "1")
        .assert()
        .success()
        .stdout(predicate::str::contains("\x1b[").not());
}
