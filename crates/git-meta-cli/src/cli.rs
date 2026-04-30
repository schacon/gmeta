use clap::{ArgAction, Args, Parser, Subcommand};

/// Top-level command-line interface for the `git-meta` binary.
//
// `version` (no value) instructs clap to derive `--version` / `-V` from
// `CARGO_PKG_VERSION`, which is kept in sync with the workspace package
// version, so the flag always reflects the version of the installed
// binary.
#[derive(Parser)]
#[command(
    name = "git-meta",
    version,
    about = "Structured metadata for Git data",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set a string metadata value
    #[command(display_order = 10)]
    Set {
        /// Read value from file
        #[arg(short = 'F', long = "file")]
        file: Option<String>,

        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Override timestamp (milliseconds since epoch, for imports)
        #[arg(long)]
        timestamp: Option<i64>,

        /// Target in type:value format (e.g. commit:abc123)
        target: String,

        /// Key (can be namespaced with colons, e.g. agent:model)
        key: String,

        /// String value (omit when reading from --file)
        value: Option<String>,
    },

    /// Get string metadata value(s)
    #[command(display_order = 11)]
    Get {
        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Include authorship info (requires --json)
        #[arg(long = "with-authorship")]
        with_authorship: bool,

        /// Target in type:value format
        target: String,

        /// Key (optional, partial key matches)
        key: Option<String>,
    },

    /// Remove a string metadata key
    #[command(display_order = 12)]
    Rm {
        /// Target in type:value format
        target: String,

        /// Key to remove
        key: String,
    },

    /// Push a value onto a list
    #[command(name = "list:push", display_order = 13)]
    ListPush {
        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to push
        value: String,
    },

    /// Pop a value from a list
    #[command(name = "list:pop", display_order = 14)]
    ListPop {
        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to pop
        value: String,
    },

    /// Show list entries with IDs, or remove one by index
    #[command(name = "list:rm", display_order = 15)]
    ListRm {
        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Index of the entry to remove (omit to list entries)
        index: Option<usize>,
    },

    /// Add a member to a set
    #[command(name = "set:add", display_order = 16)]
    SetAdd {
        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Override timestamp (milliseconds since epoch, for imports)
        #[arg(long)]
        timestamp: Option<i64>,

        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to add
        value: String,
    },

    /// Remove a member from a set
    #[command(name = "set:rm", display_order = 17)]
    SetRm {
        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Override timestamp (milliseconds since epoch, for imports)
        #[arg(long)]
        timestamp: Option<i64>,

        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to remove
        value: String,
    },

    /// Show commit details and associated metadata
    #[command(display_order = 20)]
    Show {
        /// Commit SHA or ref to show
        #[arg(value_name = "COMMIT")]
        commit: String,
    },

    /// Browse metadata keys and values
    #[command(display_order = 21)]
    Inspect {
        /// Target type to list (e.g. commit, change-id, branch, project)
        target_type: Option<String>,

        /// Fuzzy search term to filter keys/values
        term: Option<String>,

        /// Show a weekly timeline graph of entries
        #[arg(long)]
        timeline: bool,

        /// List only promisor (not-yet-fetched) keys
        #[arg(long)]
        promisor: bool,
    },

    /// Show metadata statistics
    #[command(display_order = 22)]
    Stats,

    /// Walk commit log and show metadata for each commit
    #[command(display_order = 23)]
    Log {
        /// Commit-ish to start from (default: HEAD)
        #[arg(value_name = "REF")]
        start_ref: Option<String>,

        /// Number of commits to show (default: 20)
        #[arg(short = 'n', default_value = "20")]
        count: usize,

        /// Only show commits that have metadata
        #[arg(long = "mo")]
        metadata_only: bool,
    },

    /// Serialize metadata to Git ref
    #[command(display_order = 30)]
    Serialize {
        /// Show detailed information about serialization decisions
        #[arg(short = 'v', long)]
        verbose: bool,

        /// Rebuild serialized refs from the full SQLite state
        #[arg(long = "force-full")]
        force_full: bool,
    },

    /// Materialize remote metadata into local SQLite
    #[command(display_order = 31)]
    Materialize {
        /// Remote name (optional, defaults to all remotes)
        remote: Option<String>,

        /// Show what would be changed without updating SQLite or refs
        #[arg(long = "dry-run")]
        dry_run: bool,

        /// Reindex promisor keys from the full remote metadata history
        #[arg(long = "force-full")]
        force_full: bool,

        /// Show detailed information about merge decisions and tree parsing
        #[arg(short = 'v', long)]
        verbose: bool,
    },

    /// Import metadata from another format
    #[command(display_order = 32, hide = true)]
    Import(ImportArgs),

    /// Show file blame grouped by pull request metadata
    #[command(display_order = 24)]
    Blame {
        /// Output compact JSON with line ranges and PR metadata
        #[arg(long)]
        json: bool,

        /// Output machine-readable JSON
        #[arg(long)]
        porcelain: bool,

        /// Revision to blame from (default: HEAD)
        #[arg(long)]
        rev: Option<String>,

        /// Path to blame
        path: String,
    },

    /// Initialize a metadata remote from a project-local `.git-meta` file
    ///
    /// Reads the remote URL from `.git-meta` at the repo root and then runs
    /// the equivalent of `git meta remote add <url> --init`. The file is YAML
    /// with a required `url` key; unknown keys are ignored for forward
    /// compatibility.
    #[command(display_order = 33)]
    Setup,

    /// Manage metadata remote sources
    #[command(display_order = 34)]
    Remote(RemoteArgs),

    /// Push local metadata to a remote
    #[command(display_order = 35)]
    Push {
        /// Remote name (defaults to the first meta remote)
        remote: Option<String>,

        /// Show detailed information about push decisions
        #[arg(short = 'v', long)]
        verbose: bool,

        /// Push a README to refs/heads/main on the meta remote (only if it doesn't already exist)
        #[arg(long)]
        readme: bool,
    },

    /// Pull remote metadata and merge into local database
    #[command(display_order = 36)]
    Pull {
        /// Remote name (defaults to the first meta remote)
        remote: Option<String>,

        /// Show detailed information about pull decisions
        #[arg(short = 'v', long)]
        verbose: bool,
    },

    /// Pull, merge, rewrite if needed, and push metadata
    #[command(display_order = 37)]
    Sync {
        /// Remote name (defaults to all meta remotes)
        remote: Option<String>,

        /// Show detailed information about sync decisions
        #[arg(short = 'v', long)]
        verbose: bool,
    },

    /// Walk remote history and index keys as promisor entries
    #[command(display_order = 38, hide = true)]
    Promisor,

    /// Watch agent transcripts and auto-attach to commits
    #[command(display_order = 33, hide = true)]
    Watch {
        /// Agent to watch (default: claude)
        #[arg(long, default_value = "claude")]
        agent: String,

        /// Seconds of inactivity before considering agent stopped
        #[arg(long, default_value = "30")]
        debounce: u64,
    },

    /// Get or set project configuration (meta:* keys)
    #[command(display_order = 40)]
    Config {
        /// List all config values
        #[arg(long)]
        list: bool,

        /// Remove a config key
        #[arg(long)]
        unset: bool,

        /// Config key (e.g. meta:prune:since)
        key: Option<String>,

        /// Config value (omit to read)
        value: Option<String>,
    },

    /// Interactively configure auto-prune rules
    #[command(name = "config:prune", display_order = 41, hide = true)]
    ConfigPrune,

    /// Prune the serialized git tree, dropping old entries
    #[command(display_order = 42, hide = true)]
    Prune {
        /// Show what would be pruned without committing
        #[arg(long = "dry-run")]
        dry_run: bool,
    },

    /// Prune old metadata from the local SQLite database
    #[command(name = "local-prune", display_order = 43, hide = true)]
    LocalPrune {
        /// Show what would be pruned without deleting anything
        #[arg(long = "dry-run")]
        dry_run: bool,

        /// Ignore the date rule and prune all non-project metadata
        #[arg(long = "skip-date")]
        skip_date: bool,
    },

    /// Remove the git meta database and all meta refs
    #[command(display_order = 44)]
    Teardown,
}

#[derive(Args)]
pub struct RemoteArgs {
    #[command(subcommand)]
    pub action: RemoteAction,
}

/// Arguments for importing metadata from external tools and services.
#[derive(Args)]
pub struct ImportArgs {
    /// Import source to use.
    #[command(subcommand)]
    pub action: Option<ImportAction>,

    /// Legacy source format: "entire" or "git-ai".
    #[arg(long, hide = true)]
    pub format: Option<String>,

    /// Show what would be imported without writing.
    #[arg(long = "dry-run", hide = true)]
    pub dry_run: bool,

    /// Only import metadata for commits on or after this date (YYYY-MM-DD).
    #[arg(long, hide = true)]
    pub since: Option<String>,
}

/// Supported import sources.
#[derive(Subcommand)]
pub enum ImportAction {
    /// Import merged pull request metadata from GitHub using gh
    Gh(GhImportArgs),
}

/// Arguments for `git meta import gh`.
#[derive(Args)]
pub struct GhImportArgs {
    /// Show what would be imported without writing
    #[arg(long = "dry-run")]
    pub dry_run: bool,

    /// Maximum number of merged PRs to fetch
    #[arg(long)]
    pub limit: Option<usize>,

    /// Only import PRs merged on or after this date (YYYY-MM-DD)
    #[arg(long)]
    pub since: Option<String>,

    /// GitHub repository in OWNER/NAME form
    #[arg(long)]
    pub repo: Option<String>,

    /// Import PR comments and review bodies
    #[arg(long, action = ArgAction::SetTrue, default_value_t = true)]
    pub include_comments: bool,

    /// Skip release tag mapping
    #[arg(long = "no-tags")]
    pub no_tags: bool,

    /// Reprocess PRs even when they were previously imported
    #[arg(long)]
    pub force: bool,
}

#[derive(Subcommand)]
pub enum RemoteAction {
    /// Add a metadata remote source
    Add {
        /// Remote URL (e.g. git@github.com:user/repo.git)
        url: String,

        /// Remote name (default: meta)
        #[arg(long, default_value = "meta")]
        name: String,

        /// Metadata namespace to use (default: from git config or "meta")
        #[arg(long)]
        namespace: Option<String>,

        /// Initialize the remote with a README commit on `refs/{namespace}/main`
        /// when no metadata refs exist there yet.
        ///
        /// On an interactive terminal, you will be prompted instead. Use this
        /// flag to skip the prompt (e.g. in CI).
        #[arg(long)]
        init: bool,
    },

    /// Remove a metadata remote source
    Remove {
        /// Remote name to remove
        name: String,
    },

    /// List configured metadata remotes
    List,
}

/// One curated group in the top-level help output.
///
/// Each group prints a bold heading followed by one or more
/// [`HelpSection`]s, allowing a single thematic group (like "read and
/// write data") to be visually subdivided by the kind of value it
/// operates on (strings vs lists vs sets).
struct HelpGroup {
    /// Heading text, rendered bold + yellow.
    heading: &'static str,
    /// Sub-sections, in display order. A group with a single
    /// label-less section renders identically to a flat group.
    sections: &'static [HelpSection],
}

/// One sub-section within a [`HelpGroup`].
///
/// When `label` is `Some`, it is rendered dim and indented just above
/// the commands (e.g. `(strings)`, `(lists)`, `(sets)`). When `label`
/// is `None`, the commands are listed directly under the group heading
/// with no extra label line.
struct HelpSection {
    /// Optional sub-label, e.g. `Some("(strings)")`.
    label: Option<&'static str>,
    /// Subcommand names to list, in display order. Names must match
    /// what clap resolves via `Command::find_subcommand` so the
    /// one-line `about` text can be pulled from the [`Commands`] enum.
    commands: &'static [&'static str],
}

/// Curated top-level help groups, shown by [`print_help`].
///
/// Order matters at every level: groups print top-to-bottom, sections
/// print top-to-bottom within each group, and command names print
/// top-to-bottom within each section. Anything not listed here is
/// hidden from this view; most of those entries also carry
/// `#[command(hide = true)]` so they stay out of clap's own help,
/// error suggestions, and shell completions.
const HELP_GROUPS: &[HelpGroup] = &[
    HelpGroup {
        heading: "read and write data",
        sections: &[
            HelpSection {
                label: Some("(strings)"),
                commands: &["set", "get", "rm"],
            },
            HelpSection {
                label: Some("(lists)"),
                commands: &["list:push", "list:pop", "list:rm"],
            },
            HelpSection {
                label: Some("(sets)"),
                commands: &["set:add", "set:rm"],
            },
        ],
    },
    HelpGroup {
        heading: "browse and exchange (porcelain)",
        sections: &[
            // push / pull are the two commands users reach for daily,
            // so they go first as their own block, separated by a
            // blank line from the read-only inspection commands below.
            HelpSection {
                label: None,
                commands: &["push", "pull", "sync"],
            },
            HelpSection {
                label: None,
                commands: &["show", "inspect", "log", "blame", "stats"],
            },
        ],
    },
    HelpGroup {
        heading: "low-level git ref operations (plumbing)",
        sections: &[HelpSection {
            label: None,
            commands: &["serialize", "materialize"],
        }],
    },
    HelpGroup {
        heading: "setup and configuration",
        sections: &[HelpSection {
            label: None,
            commands: &["setup", "remote", "config", "teardown"],
        }],
    },
];

/// Decide whether the curated help should emit ANSI color codes.
///
/// Delegates to [`crate::style::use_color_stdout`], which centralises the
/// `NO_COLOR` / `CLICOLOR_FORCE` / TTY precedence used everywhere in the
/// CLI. Wrapping it here keeps [`Palette::detect`] readable and lets the
/// help-specific call site stay decoupled from the lower-level helper.
fn use_color() -> bool {
    crate::style::use_color_stdout()
}

/// ANSI styling palette used by [`print_help`].
///
/// Constructed once per invocation by [`Palette::detect`] so every code
/// site can stay free of color/no-color branching. When color is
/// disabled every field is the empty string, making colored and plain
/// output a single code path.
struct Palette {
    /// Bold weight, used for the `usage:` label.
    bold: &'static str,
    /// Dim weight, used for the footer hints.
    dim: &'static str,
    /// Yellow foreground, combined with bold for group headings.
    yellow: &'static str,
    /// Green foreground, used for command names in each group.
    green: &'static str,
    /// SGR reset, terminating any styled run.
    reset: &'static str,
}

impl Palette {
    /// Build a palette honouring the environment: colored when stdout is
    /// a TTY and `NO_COLOR` is unset, otherwise an all-empty palette.
    fn detect() -> Self {
        if use_color() {
            Self {
                bold: "\x1b[1m",
                dim: "\x1b[2m",
                yellow: "\x1b[33m",
                green: "\x1b[32m",
                reset: "\x1b[0m",
            }
        } else {
            Self {
                bold: "",
                dim: "",
                yellow: "",
                green: "",
                reset: "",
            }
        }
    }
}

/// Print the structured top-level help that replaces clap's auto-generated
/// help for `git meta`, `git meta -h`, `git meta --help`, and `git meta
/// help`.
///
/// Subcommand one-line descriptions are pulled from the clap [`Command`]
/// tree at runtime so they always match the doc comments on each
/// [`Commands`] variant — there is no second source of truth to keep in
/// sync. Subcommand names not listed in [`HELP_GROUPS`] are intentionally
/// omitted.
///
/// Output goes to stdout. Group headings render bold yellow and command
/// names render green when stdout is a terminal and `NO_COLOR` is unset;
/// in pipes, logs, and dumb terminals the output is plain ASCII.
///
/// [`Command`]: clap::Command
pub fn print_help() {
    use clap::CommandFactory;
    let cmd = Cli::command();
    let p = Palette::detect();

    // Pad command names so the description column lines up across
    // every group and sub-section. ANSI codes are zero-width so this
    // width also lines up visually when colors are enabled.
    let pad = HELP_GROUPS
        .iter()
        .flat_map(|g| g.sections.iter())
        .flat_map(|s| s.commands.iter())
        .map(|n| n.len())
        .max()
        .unwrap_or(0)
        + 4;

    println!("{}usage:{} git meta <command> [options]", p.bold, p.reset);
    println!();
    println!("Structured metadata for Git data — attach values to commits, branches,");
    println!("paths, and projects, and exchange them over normal git transport.");
    println!();
    println!("These are the most commonly used git meta commands:");

    for group in HELP_GROUPS {
        println!();
        println!("{}{}{}{}", p.bold, p.yellow, group.heading, p.reset);
        for (idx, section) in group.sections.iter().enumerate() {
            // Adjacent sub-sections always get a blank line between
            // them so they read as distinct blocks — whether they're
            // sub-labelled (e.g. (strings) / (lists) / (sets)) or
            // unlabelled (e.g. push/pull above show/inspect/log/stats).
            // The first section always sits flush against the group
            // heading.
            if idx > 0 {
                println!();
            }
            if let Some(label) = section.label {
                println!("   {}{}{}", p.dim, label, p.reset);
            }
            for name in section.commands {
                let about = cmd
                    .find_subcommand(name)
                    .and_then(|c| c.get_about())
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                println!(
                    "   {green}{name:<pad$}{reset}{about}",
                    green = p.green,
                    reset = p.reset,
                );
            }
        }
    }

    println!();
    println!(
        "{dim}Run 'git meta <command> --help' for command-specific options.{reset}",
        dim = p.dim,
        reset = p.reset,
    );
    println!(
        "{dim}See https://git-meta.com for the spec and full docs.{reset}",
        dim = p.dim,
        reset = p.reset,
    );
}
