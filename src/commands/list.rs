use anyhow::Result;
use chrono::Utc;

use crate::db::Db;
use crate::git_utils;
use crate::types::Target;

pub fn run_push(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    db.list_push(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &email,
        timestamp,
    )?;

    Ok(())
}

pub fn run_pop(target_str: &str, key: &str, value: &str) -> Result<()> {
    let mut target = Target::parse(target_str)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let timestamp = Utc::now().timestamp_millis();

    let db = Db::open(&db_path)?;

    db.list_pop(
        target.type_str(),
        target.value_str(),
        key,
        value,
        &email,
        timestamp,
    )?;

    Ok(())
}
