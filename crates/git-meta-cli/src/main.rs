#![allow(clippy::type_complexity, clippy::too_many_arguments)]

mod cli;
mod commands;
mod context;
mod pager;
mod style;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, ImportAction, RemoteAction};

/// Returns `true` when the user invoked `git meta` in a way that should
/// trigger our curated top-level help instead of clap's auto-generated
/// output.
///
/// The check is intentionally narrow — only the bare `git meta`,
/// `git meta -h`, `git meta --help`, and `git meta help` invocations are
/// intercepted. Anything with a real subcommand (e.g. `git meta set
/// --help`) falls through to clap so the per-subcommand help still works
/// as normal.
fn should_show_top_level_help() -> bool {
    let mut args = std::env::args().skip(1);
    match (args.next(), args.next()) {
        (None, _) => true,
        (Some(arg), None) => matches!(arg.as_str(), "-h" | "--help" | "help"),
        _ => false,
    }
}

fn main() -> Result<()> {
    if should_show_top_level_help() {
        cli::print_help();
        return Ok(());
    }

    let cli = Cli::parse();

    match cli.command {
        Commands::Set {
            file,
            json,
            timestamp,
            target,
            key,
            value,
        } => commands::set::run(
            &target,
            &key,
            value.as_deref(),
            file.as_deref(),
            json,
            timestamp,
        ),

        Commands::Get {
            json,
            with_authorship,
            target,
            key,
        } => commands::get::run(&target, key.as_deref(), json, with_authorship),

        Commands::Rm { target, key } => commands::rm::run(&target, &key),

        Commands::ListPush { target, key, value } => {
            commands::list::run_push(&target, &key, &value)
        }

        Commands::ListPop { target, key, value } => commands::list::run_pop(&target, &key, &value),

        Commands::ListRm { target, key, index } => commands::list::run_rm(&target, &key, index),

        Commands::SetAdd {
            json,
            timestamp,
            target,
            key,
            value,
        } => commands::set::run_add(&target, &key, &value, json, timestamp),

        Commands::SetRm {
            json,
            timestamp,
            target,
            key,
            value,
        } => commands::set::run_rm(&target, &key, &value, json, timestamp),

        Commands::Setup => commands::setup::run(),

        Commands::Remote(args) => match args.action {
            RemoteAction::Add {
                url,
                name,
                namespace,
                init,
            } => commands::remote::run_add(&url, &name, namespace.as_deref(), init),
            RemoteAction::Remove { name } => commands::remote::run_remove(&name),
            RemoteAction::List => commands::remote::run_list(),
        },

        Commands::Push {
            remote,
            verbose,
            readme,
        } => {
            if readme {
                commands::push::run_readme(remote.as_deref(), verbose)
            } else {
                commands::push::run(remote.as_deref(), verbose)
            }
        }
        Commands::Pull { remote, verbose } => commands::pull::run(remote.as_deref(), verbose),
        Commands::Sync { remote, verbose } => commands::sync::run(remote.as_deref(), verbose),

        Commands::Promisor => commands::promisor::run(),

        Commands::Serialize {
            verbose,
            force_full,
        } => commands::serialize::run(verbose, force_full),

        Commands::Materialize {
            remote,
            dry_run,
            force_full,
            verbose,
        } => commands::materialize::run(remote.as_deref(), dry_run, force_full, verbose),

        Commands::Import(args) => match args.action {
            Some(ImportAction::Gh(gh_args)) => commands::import::run_gh(
                gh_args.dry_run,
                gh_args.limit,
                gh_args.since.as_deref(),
                gh_args.repo.as_deref(),
                gh_args.include_comments,
                gh_args.no_tags,
                gh_args.force,
            ),
            None => {
                let format = args.format.ok_or_else(|| {
                    anyhow::anyhow!("missing import source; try `git meta import gh`")
                })?;
                let fmt = commands::import::ImportFormat::from_str(&format)?;
                commands::import::run(fmt, args.dry_run, args.since.as_deref())
            }
        },

        Commands::Show { commit } => commands::show::run(&commit),

        Commands::Inspect {
            target_type,
            term,
            timeline,
            promisor,
        } => commands::inspect::run(target_type.as_deref(), term.as_deref(), timeline, promisor),

        Commands::Stats => commands::stats::run(),

        Commands::Log {
            start_ref,
            count,
            metadata_only,
        } => commands::log::run(start_ref.as_deref(), count, metadata_only),

        Commands::Blame {
            json,
            porcelain,
            rev,
            path,
        } => commands::blame::run(&path, rev.as_deref(), porcelain, json),

        Commands::Config {
            list,
            unset,
            key,
            value,
        } => commands::config::run(list, unset, key.as_deref(), value.as_deref()),

        Commands::ConfigPrune => commands::prune::config::run(),

        Commands::Prune { dry_run } => commands::prune::tree::run(dry_run),

        Commands::LocalPrune { dry_run, skip_date } => {
            commands::prune::local::run(dry_run, skip_date)
        }

        Commands::Teardown => commands::teardown::run(),

        Commands::Watch { agent, debounce } => commands::watch::run(&agent, debounce),
    }
}
