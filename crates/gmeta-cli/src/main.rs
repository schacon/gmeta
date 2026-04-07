#![allow(clippy::type_complexity, clippy::too_many_arguments)]

mod cli;
mod commands;
mod context;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, RemoteAction};

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Set {
            value_type,
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
            &value_type,
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

        Commands::Remote(args) => match args.action {
            RemoteAction::Add {
                url,
                name,
                namespace,
            } => commands::remote::run_add(&url, &name, namespace.as_deref()),
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

        Commands::Promisor => commands::promisor::run(),

        Commands::Serialize { verbose } => commands::serialize::run(verbose),

        Commands::Materialize {
            remote,
            dry_run,
            verbose,
        } => commands::materialize::run(remote.as_deref(), dry_run, verbose),

        Commands::Import {
            format,
            dry_run,
            since,
        } => {
            let fmt = gmeta_core::types::ImportFormat::from_str(&format)?;
            commands::import::run(fmt, dry_run, since.as_deref())
        }

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

        Commands::Bench => commands::bench::db_bench::run(),

        Commands::FanoutBench { objects } => commands::bench::fanout_bench::run(objects),

        Commands::HistoryWalker { commits } => commands::bench::history_walker::run(commits),

        Commands::SerializeBench { rounds } => commands::bench::serialize_bench::run(rounds),

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
