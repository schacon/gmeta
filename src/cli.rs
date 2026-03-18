use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "gmeta", about = "Structured metadata for Git data")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Set a metadata value
    Set {
        /// Value type: string (default) or list
        #[arg(short = 't', long = "type", default_value = "string")]
        value_type: String,

        /// Read value from file
        #[arg(short = 'F', long = "file")]
        file: Option<String>,

        /// Target in type:value format (e.g. commit:abc123)
        target: String,

        /// Key (can be namespaced with colons, e.g. agent:model)
        key: String,

        /// Value (string or JSON array for lists)
        value: Option<String>,
    },

    /// Get metadata value(s)
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
    Rm {
        /// Target in type:value format
        target: String,

        /// Key to remove
        key: String,
    },

    /// Push a value onto a list
    #[command(name = "list:push")]
    ListPush {
        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to push
        value: String,
    },

    /// Pop a value from a list
    #[command(name = "list:pop")]
    ListPop {
        /// Target in type:value format
        target: String,

        /// Key
        key: String,

        /// Value to pop
        value: String,
    },

    /// Serialize metadata to Git ref
    Serialize,

    /// Materialize remote metadata into local SQLite
    Materialize {
        /// Remote name (optional, defaults to all remotes)
        remote: Option<String>,

        /// Show what would be changed without updating SQLite or refs
        #[arg(long = "dry-run")]
        dry_run: bool,
    },

    /// Import metadata from another format
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

    /// Show metadata statistics
    Stats,

    /// Walk commit log and show metadata for each commit
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

    /// Benchmark read performance across all stored keys
    Bench,

    /// Benchmark fanout schemes (first2 vs first3 vs first2/next2) on a synthetic repo
    FanoutBench {
        /// Number of base objects to populate the tree with (default: 1_000_000)
        #[arg(long, default_value = "1000000")]
        objects: usize,
    },

    /// Remove the gmeta database and all meta refs
    Teardown,
}
