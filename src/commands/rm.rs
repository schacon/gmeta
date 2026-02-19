use anyhow::Result;
use chrono::Utc;

use crate::db::Db;
use crate::git_utils;
use crate::types::Target;

pub fn run(target_str: &str, key: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    let removed = db.rm(target.type_str(), target.value_str(), key, &email, timestamp)?;

    if !removed {
        eprintln!("key '{}' not found", key);
    }

    Ok(())
}
