use anyhow::Result;
use chrono::Utc;
use dialoguer::{Confirm, Input, Select};

use crate::commands::auto_prune::{parse_size, read_prune_rules};
use crate::db::Db;
use crate::git_utils;

pub fn run() -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let existing = read_prune_rules(&db)?;

    if let Some(ref rules) = existing {
        println!("Current auto-prune configuration:");
        println!("  since:    {}", rules.since);
        if let Some(max_keys) = rules.max_keys {
            println!("  max-keys: {}", max_keys);
        }
        if let Some(max_size) = rules.max_size {
            println!("  max-size: {} bytes", max_size);
        }
        if let Some(min_size) = rules.min_size {
            println!("  min-size: {} bytes", min_size);
        }
        println!();
    } else {
        println!("No auto-prune rules configured yet.");
        println!();
    }

    // -- since --
    println!("Retention window (meta:prune:since)");
    println!("How far back should metadata be kept?");
    println!("Examples: 90d, 6m, 1y, or a date like 2025-01-01");

    let since_options = ["90d", "6m", "1y", "Custom"];
    let since_default = existing
        .as_ref()
        .and_then(|r| since_options.iter().position(|&o| o == r.since))
        .unwrap_or(1);

    let since_idx = Select::new()
        .with_prompt("Retention window")
        .items(&since_options)
        .default(since_default)
        .interact()?;

    let since = if since_idx == since_options.len() - 1 {
        Input::<String>::new()
            .with_prompt("Enter retention value (e.g. 180d, 2025-01-01)")
            .interact_text()?
    } else {
        since_options[since_idx].to_string()
    };

    // -- max-keys --
    println!();
    println!("Key count trigger (meta:prune:max-keys)");
    println!("Auto-prune runs when total metadata keys exceed this count.");

    let want_max_keys = Confirm::new()
        .with_prompt("Set a max-keys trigger?")
        .default(existing.as_ref().map_or(true, |r| r.max_keys.is_some()))
        .interact()?;

    let max_keys: Option<String> = if want_max_keys {
        let default = existing
            .as_ref()
            .and_then(|r| r.max_keys)
            .unwrap_or(10000)
            .to_string();
        let val = Input::<String>::new()
            .with_prompt("Max keys")
            .default(default)
            .interact_text()?;
        // Validate
        val.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid number: {}", val))?;
        Some(val)
    } else {
        None
    };

    // -- max-size --
    println!();
    println!("Size trigger (meta:prune:max-size)");
    println!("Auto-prune runs when total serialized tree size exceeds this.");
    println!("Examples: 512k, 10m, 1g");

    let want_max_size = Confirm::new()
        .with_prompt("Set a max-size trigger?")
        .default(existing.as_ref().map_or(true, |r| r.max_size.is_some()))
        .interact()?;

    let max_size: Option<String> = if want_max_size {
        let default = existing
            .as_ref()
            .and_then(|r| r.max_size)
            .map(format_size)
            .unwrap_or_else(|| "10m".to_string());
        let val = Input::<String>::new()
            .with_prompt("Max size")
            .default(default)
            .interact_text()?;
        // Validate
        parse_size(&val)?;
        Some(val)
    } else {
        None
    };

    // Need at least one trigger
    if max_keys.is_none() && max_size.is_none() {
        println!();
        println!("Warning: at least one trigger (max-keys or max-size) is required for auto-prune.");
        println!("Auto-prune will not activate without a trigger.");
        println!("Saving retention window only.");
    }

    // -- min-size --
    println!();
    println!("Minimum subtree size (meta:prune:min-size)");
    println!("Subtrees smaller than this are exempt from pruning.");

    let want_min_size = Confirm::new()
        .with_prompt("Set a min-size exemption?")
        .default(existing.as_ref().map_or(false, |r| r.min_size.is_some()))
        .interact()?;

    let min_size: Option<String> = if want_min_size {
        let default = existing
            .as_ref()
            .and_then(|r| r.min_size)
            .map(format_size)
            .unwrap_or_else(|| "1k".to_string());
        let val = Input::<String>::new()
            .with_prompt("Min size")
            .default(default)
            .interact_text()?;
        parse_size(&val)?;
        Some(val)
    } else {
        None
    };

    // -- summary --
    println!();
    println!("Configuration to save:");
    println!("  meta:prune:since    = {}", since);
    if let Some(ref v) = max_keys {
        println!("  meta:prune:max-keys = {}", v);
    }
    if let Some(ref v) = max_size {
        println!("  meta:prune:max-size = {}", v);
    }
    if let Some(ref v) = min_size {
        println!("  meta:prune:min-size = {}", v);
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
    let email = git_utils::get_email(&repo)?;
    let ts = Utc::now().timestamp_millis();

    set_config(&db, "meta:prune:since", &since, &email, ts)?;

    match max_keys {
        Some(ref v) => set_config(&db, "meta:prune:max-keys", v, &email, ts)?,
        None => { db.rm("project", "", "meta:prune:max-keys", &email, ts)?; }
    }
    match max_size {
        Some(ref v) => set_config(&db, "meta:prune:max-size", v, &email, ts)?,
        None => { db.rm("project", "", "meta:prune:max-size", &email, ts)?; }
    }
    match min_size {
        Some(ref v) => set_config(&db, "meta:prune:min-size", v, &email, ts)?,
        None => { db.rm("project", "", "meta:prune:min-size", &email, ts)?; }
    }

    println!("Auto-prune rules saved.");
    Ok(())
}

fn set_config(db: &Db, key: &str, value: &str, email: &str, ts: i64) -> Result<()> {
    let stored = serde_json::to_string(value)?;
    db.set("project", "", key, &stored, "string", email, ts)?;
    Ok(())
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 && bytes % (1024 * 1024 * 1024) == 0 {
        format!("{}g", bytes / (1024 * 1024 * 1024))
    } else if bytes >= 1024 * 1024 && bytes % (1024 * 1024) == 0 {
        format!("{}m", bytes / (1024 * 1024))
    } else if bytes >= 1024 && bytes % 1024 == 0 {
        format!("{}k", bytes / 1024)
    } else {
        bytes.to_string()
    }
}
