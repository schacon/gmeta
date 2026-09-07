use anyhow::Result;
use dialoguer::{Confirm, Input};

use crate::context::CommandContext;
use git_meta_lib::prune::{parse_size, read_prune_rules};
use git_meta_lib::types::{MetaValue, Target};

pub(crate) fn run() -> Result<()> {
    let ctx = CommandContext::open(None)?;

    let existing = read_prune_rules(ctx.session.store())?;

    if let Some(ref rules) = existing {
        println!("Current auto-prune configuration:");
        print_pair("keys", rules.max_keys, rules.min_keys, |value| {
            value.to_string()
        });
        print_pair("size", rules.max_size, rules.min_size, format_size);
        println!();
    } else {
        println!("No auto-prune rules configured yet.");
        println!();
    }

    println!("Auto-prune keeps the published tree between a maximum and a minimum.");
    println!("When it grows past the maximum, the oldest keys are dropped until it");
    println!("reaches the minimum, and it is left alone until it grows past the");
    println!("maximum again. Pruning by date is a separate, manual operation:");
    println!("  git meta prune --since 90d");
    println!();

    // -- key limits --
    let want_keys = Confirm::new()
        .with_prompt("Limit the number of keys in the tree?")
        .default(existing.as_ref().is_none_or(|r| r.max_keys.is_some()))
        .interact()?;

    let (max_keys, min_keys) = if want_keys {
        let max: String = Input::new()
            .with_prompt("  Maximum keys before pruning")
            .default(
                existing
                    .as_ref()
                    .and_then(|r| r.max_keys)
                    .unwrap_or(10_000)
                    .to_string(),
            )
            .validate_with(|input: &String| validate_count(input))
            .interact_text()?;
        let max_value: u64 = max.parse().unwrap_or(10_000);
        let min: String = Input::new()
            .with_prompt("  Prune back down to")
            .default(
                existing
                    .as_ref()
                    .and_then(|r| r.min_keys)
                    .unwrap_or(max_value / 2)
                    .to_string(),
            )
            .validate_with(|input: &String| validate_below(input, max_value, validate_count))
            .interact_text()?;
        (Some(max), Some(min))
    } else {
        (None, None)
    };

    // -- size limits --
    let want_size = Confirm::new()
        .with_prompt("Limit the total size of the tree?")
        .default(existing.as_ref().is_some_and(|r| r.max_size.is_some()))
        .interact()?;

    let (max_size, min_size) = if want_size {
        let max: String = Input::new()
            .with_prompt("  Maximum size before pruning (e.g. 50m)")
            .default(
                existing
                    .as_ref()
                    .and_then(|r| r.max_size)
                    .map_or_else(|| "50m".to_string(), format_size),
            )
            .validate_with(|input: &String| validate_size(input))
            .interact_text()?;
        let max_value = parse_size(&max)?;
        let min: String = Input::new()
            .with_prompt("  Prune back down to")
            .default(
                existing
                    .as_ref()
                    .and_then(|r| r.min_size)
                    .map_or_else(|| format_size(max_value / 2), format_size),
            )
            .validate_with(|input: &String| validate_below(input, max_value, validate_size))
            .interact_text()?;
        (Some(max), Some(min))
    } else {
        (None, None)
    };

    if max_keys.is_none() && max_size.is_none() {
        println!();
        println!("At least one limit is needed for auto-pruning. Nothing saved.");
        return Ok(());
    }

    // -- summary --
    println!();
    println!("Configuration to save:");
    for (key, value) in [
        ("meta:prune:max-keys", &max_keys),
        ("meta:prune:min-keys", &min_keys),
        ("meta:prune:max-size", &max_size),
        ("meta:prune:min-size", &min_size),
    ] {
        if let Some(value) = value {
            println!("  {key} = {value}");
        }
    }

    let confirm = Confirm::new()
        .with_prompt("Save these settings?")
        .default(true)
        .interact()?;

    if !confirm {
        println!("Aborted.");
        return Ok(());
    }

    // -- write --
    let project = project_target();
    let handle = ctx.session.target(&project);
    for (key, value) in [
        ("meta:prune:max-keys", &max_keys),
        ("meta:prune:min-keys", &min_keys),
        ("meta:prune:max-size", &max_size),
        ("meta:prune:min-size", &min_size),
    ] {
        match value {
            Some(value) => set_config(&ctx, key, value)?,
            None => {
                handle.remove(key)?;
            }
        }
    }

    println!("Auto-prune rules saved.");
    Ok(())
}

/// Show a configured maximum with the minimum it prunes back to.
fn print_pair(what: &str, max: Option<u64>, min: Option<u64>, render: impl Fn(u64) -> String) {
    if let Some(max) = max {
        let min = min.map_or_else(|| "half".to_string(), &render);
        println!("  max-{what}: {} (prunes back to {min})", render(max));
    }
}

#[allow(clippy::ptr_arg)]
fn validate_count(input: &String) -> std::result::Result<(), String> {
    match input.trim().parse::<u64>() {
        Ok(value) if value > 0 => Ok(()),
        Ok(_) => Err("must be greater than zero".to_string()),
        Err(_) => Err("must be a whole number".to_string()),
    }
}

#[allow(clippy::ptr_arg)]
fn validate_size(input: &String) -> std::result::Result<(), String> {
    parse_size(input)
        .map(|_| ())
        .map_err(|_| "use a size like 512k, 10m or 1g".to_string())
}

#[allow(clippy::ptr_arg)]
fn validate_below(
    input: &String,
    max: u64,
    validate: impl Fn(&String) -> std::result::Result<(), String>,
) -> std::result::Result<(), String> {
    validate(input)?;
    let value = input
        .trim()
        .parse::<u64>()
        .or_else(|_| parse_size(input).map_err(|_| "invalid value".to_string()))?;
    if value >= max {
        return Err(format!("must be below the maximum ({max})"));
    }
    Ok(())
}

fn project_target() -> Target {
    Target::project()
}

fn set_config(ctx: &CommandContext, key: &str, value: &str) -> Result<()> {
    let meta_value = MetaValue::String(value.to_string());
    ctx.session.target(&project_target()).set(key, meta_value)?;
    Ok(())
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 && bytes.is_multiple_of(1024 * 1024 * 1024) {
        format!("{}g", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 && bytes.is_multiple_of(1024 * 1024) {
        format!("{}m", bytes / (1024 * 1024))
    } else if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{}k", bytes / 1024)
    } else {
        bytes.to_string()
    }
}
