use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "git-meta",
    about = "Structured metadata for Git data",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set a metadata value
    #[command(display_order = 10)]
    Set {
        /// Value type: string (default) or list
        #[arg(short = 't', long = "type", default_value = "string")]
        value_type: String,

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

        /// Value (string or JSON array for lists)
        value: Option<String>,
    },

    /// Get metadata value(s)
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

    /// Remove a metadata key
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
    },

    /// Materialize remote metadata into local SQLite
    #[command(display_order = 31)]
    Materialize {
        /// Remote name (optional, defaults to all remotes)
        remote: Option<String>,

        /// Show what would be changed without updating SQLite or refs
        #[arg(long = "dry-run")]
        dry_run: bool,

        /// Show detailed information about merge decisions and tree parsing
        #[arg(short = 'v', long)]
        verbose: bool,
    },

    /// Import metadata from another format
    #[command(display_order = 32)]
    Import {
        /// Source format: "entire" or "git-ai"
        #[arg(long)]
        format: String,

        /// Show what would be imported without writing
        #[arg(long = "dry-run")]
        dry_run: bool,

        /// Only import metadata for commits on or after this date (YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
    },

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

    /// Walk remote history and index keys as promisor entries
    #[command(display_order = 37)]
    Promisor,

    /// Watch agent transcripts and auto-attach to commits
    #[command(display_order = 33)]
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
    #[command(name = "config:prune", display_order = 41)]
    ConfigPrune,

    /// Prune the serialized git tree, dropping old entries
    #[command(display_order = 42)]
    Prune {
        /// Show what would be pruned without committing
        #[arg(long = "dry-run")]
        dry_run: bool,
    },

    /// Prune old metadata from the local SQLite database
    #[command(name = "local-prune", display_order = 43)]
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

    /// Benchmark read performance across all stored keys
    #[cfg(feature = "bench")]
    Bench,

    /// Benchmark fanout schemes on a synthetic repo
    #[cfg(feature = "bench")]
    FanoutBench {
        /// Number of base objects to populate the tree with (default: 1_000_000)
        #[arg(long, default_value = "1000000")]
        objects: usize,
    },

    /// Benchmark history generation and full-history walk
    #[cfg(feature = "bench")]
    HistoryWalker {
        /// Number of meta commits to generate (default: 500)
        #[arg(long, default_value = "500")]
        commits: usize,
    },

    /// Benchmark serialize performance
    #[cfg(feature = "bench")]
    SerializeBench {
        /// Number of insert+serialize rounds (default: 10)
        #[arg(long, default_value = "10")]
        rounds: usize,
    },
}

#[derive(Args)]
pub struct RemoteArgs {
    #[command(subcommand)]
    pub action: RemoteAction,
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
    },

    /// Remove a metadata remote source
    Remove {
        /// Remote name to remove
        name: String,
    },

    /// List configured metadata remotes
    List,
}
