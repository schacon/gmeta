use crate::error::Result;
use crate::list_value::{encode_entries, parse_entries};
use crate::types::{MetaValue, Target, ValueType};

use super::Store;

impl Store {
    /// Set a metadata value using the type-safe [`MetaValue`] enum.
    ///
    /// This is the preferred API — it's impossible to pass a value whose
    /// content doesn't match its declared type.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target (commit, branch, path, etc.)
    /// - `key`: the metadata key name
    /// - `value`: the typed value to store
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the underlying database write fails.
    pub fn set_value(
        &self,
        target: &Target,
        key: &str,
        value: &MetaValue,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        match value {
            MetaValue::String(s) => {
                let json = serde_json::to_string(s)?;
                self.set(target, key, &json, &ValueType::String, email, timestamp)
            }
            MetaValue::List(entries) => {
                let json = encode_entries(entries)?;
                self.set(target, key, &json, &ValueType::List, email, timestamp)
            }
            MetaValue::Set(members) => {
                let json = serde_json::to_string(&members.iter().collect::<Vec<_>>())?;
                self.set(target, key, &json, &ValueType::Set, email, timestamp)
            }
        }
    }

    /// Get a metadata value as a type-safe [`MetaValue`].
    ///
    /// Returns `None` if the key doesn't exist. Strings are returned
    /// unquoted (the JSON encoding is stripped).
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target to query
    /// - `key`: the metadata key name
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or deserialization fails.
    pub fn get_value(&self, target: &Target, key: &str) -> Result<Option<MetaValue>> {
        let result = self.get(target, key)?;
        match result {
            None => Ok(None),
            Some(entry) => match entry.value_type {
                ValueType::String => {
                    let s: String =
                        serde_json::from_str(&entry.value).unwrap_or_else(|_| entry.value.clone());
                    Ok(Some(MetaValue::String(s)))
                }
                ValueType::List => {
                    let entries = parse_entries(&entry.value)?;
                    Ok(Some(MetaValue::List(entries)))
                }
                ValueType::Set => {
                    let members: Vec<String> = serde_json::from_str(&entry.value)?;
                    Ok(Some(MetaValue::Set(members.into_iter().collect())))
                }
            },
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::list_value::ListEntry;

    #[test]
    fn test_set_and_get_string_value() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();
        let value = MetaValue::String("hello".to_string());
        db.set_value(&target, "greeting", &value, "a@b.com", 1000)
            .unwrap();

        let result = db.get_value(&target, "greeting").unwrap();
        assert_eq!(result, Some(MetaValue::String("hello".to_string())));
    }

    #[test]
    fn test_set_and_get_list_value() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();
        let entries = vec![
            ListEntry {
                value: "first".to_string(),
                timestamp: 1000,
            },
            ListEntry {
                value: "second".to_string(),
                timestamp: 2000,
            },
        ];
        let value = MetaValue::List(entries.clone());
        db.set_value(&target, "tags", &value, "a@b.com", 3000)
            .unwrap();

        let result = db.get_value(&target, "tags").unwrap();
        assert_eq!(result, Some(MetaValue::List(entries)));
    }

    #[test]
    fn test_set_and_get_set_value() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();
        let mut members = BTreeSet::new();
        members.insert("alice".to_string());
        members.insert("bob".to_string());
        let value = MetaValue::Set(members.clone());
        db.set_value(&target, "reviewers", &value, "a@b.com", 1000)
            .unwrap();

        let result = db.get_value(&target, "reviewers").unwrap();
        assert_eq!(result, Some(MetaValue::Set(members)));
    }

    #[test]
    fn test_get_value_returns_none_for_missing_key() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();
        let result = db.get_value(&target, "nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_set_value_upsert() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();
        db.set_value(
            &target,
            "key",
            &MetaValue::String("v1".into()),
            "a@b.com",
            1000,
        )
        .unwrap();
        db.set_value(
            &target,
            "key",
            &MetaValue::String("v2".into()),
            "a@b.com",
            2000,
        )
        .unwrap();

        let result = db.get_value(&target, "key").unwrap();
        assert_eq!(result, Some(MetaValue::String("v2".to_string())));
    }

    #[test]
    fn test_set_value_and_get_via_store() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        db.set_value(
            &target,
            "key",
            &MetaValue::String("val".into()),
            "a@b.com",
            1000,
        )
        .unwrap();
        let result = db.get_value(&target, "key").unwrap();
        assert_eq!(result, Some(MetaValue::String("val".to_string())));
    }

    #[test]
    fn test_remove_via_store() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        db.set_value(
            &target,
            "key",
            &MetaValue::String("val".into()),
            "a@b.com",
            1000,
        )
        .unwrap();
        assert!(db.remove(&target, "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get_value(&target, "key").unwrap(), None);
    }

    #[test]
    fn test_list_push_pop_via_store() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        db.list_push(&target, "tags", "a", "a@b.com", 1000).unwrap();
        db.list_push(&target, "tags", "b", "a@b.com", 2000).unwrap();
        db.list_pop(&target, "tags", "b", "a@b.com", 3000).unwrap();

        let result = db.get_value(&target, "tags").unwrap().unwrap();
        match result {
            MetaValue::List(entries) => assert_eq!(entries.len(), 1),
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn test_set_add_remove_via_store() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        db.set_add(&target, "owners", "alice", "a@b.com", 1000)
            .unwrap();
        db.set_add(&target, "owners", "bob", "a@b.com", 2000)
            .unwrap();
        db.set_remove(&target, "owners", "bob", "a@b.com", 3000)
            .unwrap();

        let result = db.get_value(&target, "owners").unwrap().unwrap();
        let mut expected = BTreeSet::new();
        expected.insert("alice".to_string());
        assert_eq!(result, MetaValue::Set(expected));
    }
}
