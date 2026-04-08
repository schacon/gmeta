use crate::error::Result;
use crate::types::{MetaValue, Target, TargetType, ValueType};

use super::Store;

/// An operation to be applied in a batch.
enum BatchOp {
    Set {
        target_type: TargetType,
        target_value: String,
        key: String,
        value: String,
        value_type: ValueType,
        email: String,
        timestamp: i64,
    },
    Remove {
        target_type: TargetType,
        target_value: String,
        key: String,
        email: String,
        timestamp: i64,
    },
    ListPush {
        target_type: TargetType,
        target_value: String,
        key: String,
        value: String,
        email: String,
        timestamp: i64,
    },
    SetAdd {
        target_type: TargetType,
        target_value: String,
        key: String,
        value: String,
        email: String,
        timestamp: i64,
    },
}

/// A batch of metadata operations applied atomically.
///
/// Collect mutations with [`set_value`](Self::set_value), [`remove`](Self::remove),
/// [`list_push`](Self::list_push), and [`set_add`](Self::set_add), then apply them
/// with [`Store::apply_batch()`].
///
/// # Example
///
/// ```ignore
/// let mut batch = store.batch();
/// batch.set_value(&target, "key1", &MetaValue::String("a".into()), "user@example.com", ts);
/// batch.set_value(&target, "key2", &MetaValue::String("b".into()), "user@example.com", ts);
/// store.apply_batch(batch)?;
/// ```
pub struct Batch {
    ops: Vec<BatchOp>,
}

impl Batch {
    /// Create a new, empty batch.
    pub(super) fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Returns the number of operations queued in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns `true` if the batch contains no operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Queue a set operation using the type-safe [`MetaValue`] enum.
    ///
    /// The value is encoded to its JSON storage form immediately; the database
    /// write happens when [`Store::apply_batch()`] is called.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target (commit, branch, path, etc.)
    /// - `key`: the metadata key name
    /// - `value`: the typed value to store
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    pub fn set_value(
        &mut self,
        target: &Target,
        key: &str,
        value: &MetaValue,
        email: &str,
        timestamp: i64,
    ) {
        let (json, value_type) = encode_meta_value(value);
        self.ops.push(BatchOp::Set {
            target_type: target.target_type.clone(),
            target_value: target.value_str().to_string(),
            key: key.to_string(),
            value: json,
            value_type,
            email: email.to_string(),
            timestamp,
        });
    }

    /// Queue a remove operation.
    ///
    /// The actual deletion happens when [`Store::apply_batch()`] is called.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target to remove the key from
    /// - `key`: the metadata key to remove
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    pub fn remove(&mut self, target: &Target, key: &str, email: &str, timestamp: i64) {
        self.ops.push(BatchOp::Remove {
            target_type: target.target_type.clone(),
            target_value: target.value_str().to_string(),
            key: key.to_string(),
            email: email.to_string(),
            timestamp,
        });
    }

    /// Queue a list push operation.
    ///
    /// The value is appended to the list when [`Store::apply_batch()`] is called.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key (must be a list or will be converted)
    /// - `value`: the string value to append
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    pub fn list_push(
        &mut self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) {
        self.ops.push(BatchOp::ListPush {
            target_type: target.target_type.clone(),
            target_value: target.value_str().to_string(),
            key: key.to_string(),
            value: value.to_string(),
            email: email.to_string(),
            timestamp,
        });
    }

    /// Queue a set-add operation.
    ///
    /// The member is added to the set when [`Store::apply_batch()`] is called.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key (must be a set or will be created as one)
    /// - `value`: the string value to add
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    pub fn set_add(
        &mut self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) {
        self.ops.push(BatchOp::SetAdd {
            target_type: target.target_type.clone(),
            target_value: target.value_str().to_string(),
            key: key.to_string(),
            value: value.to_string(),
            email: email.to_string(),
            timestamp,
        });
    }
}

/// Encode a [`MetaValue`] into its JSON storage form and corresponding [`ValueType`].
fn encode_meta_value(value: &MetaValue) -> (String, ValueType) {
    match value {
        MetaValue::String(s) => {
            // serde_json::to_string on a String always succeeds
            let json = serde_json::to_string(s).unwrap_or_default();
            (json, ValueType::String)
        }
        MetaValue::List(entries) => {
            let json = crate::list_value::encode_entries(entries).unwrap_or_default();
            (json, ValueType::List)
        }
        MetaValue::Set(members) => {
            let json =
                serde_json::to_string(&members.iter().collect::<Vec<_>>()).unwrap_or_default();
            (json, ValueType::Set)
        }
    }
}

impl Store {
    /// Create a new empty batch for collecting operations.
    ///
    /// Use the returned [`Batch`] to queue mutations, then apply them
    /// atomically with [`apply_batch()`](Self::apply_batch).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut batch = store.batch();
    /// batch.set_value(&target, "key", &MetaValue::String("val".into()), email, ts);
    /// store.apply_batch(batch)?;
    /// ```
    #[must_use]
    pub fn batch(&self) -> Batch {
        Batch::new()
    }

    /// Apply a batch of operations in a single SQLite savepoint.
    ///
    /// All operations succeed or fail together. If any individual operation
    /// returns an error, the entire batch is rolled back.
    ///
    /// Each per-operation method creates its own nested savepoint internally,
    /// which nests safely inside the outer batch savepoint.
    ///
    /// # Parameters
    ///
    /// - `batch`: the collected operations to apply
    ///
    /// # Errors
    ///
    /// Returns an error if any operation in the batch fails. On error,
    /// no operations from the batch are persisted.
    pub fn apply_batch(&self, batch: Batch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let sp = self.savepoint()?;
        self.apply_batch_ops(batch)?;
        sp.commit()?;
        Ok(())
    }

    /// Execute all batch operations sequentially.
    fn apply_batch_ops(&self, batch: Batch) -> Result<()> {
        for op in batch.ops {
            match op {
                BatchOp::Set {
                    target_type,
                    target_value,
                    key,
                    value,
                    value_type,
                    email,
                    timestamp,
                } => {
                    self.set(
                        &target_type,
                        &target_value,
                        &key,
                        &value,
                        &value_type,
                        &email,
                        timestamp,
                    )?;
                }
                BatchOp::Remove {
                    target_type,
                    target_value,
                    key,
                    email,
                    timestamp,
                } => {
                    self.remove(&target_type, &target_value, &key, &email, timestamp)?;
                }
                BatchOp::ListPush {
                    target_type,
                    target_value,
                    key,
                    value,
                    email,
                    timestamp,
                } => {
                    self.list_push(&target_type, &target_value, &key, &value, &email, timestamp)?;
                }
                BatchOp::SetAdd {
                    target_type,
                    target_value,
                    key,
                    value,
                    email,
                    timestamp,
                } => {
                    self.set_add(&target_type, &target_value, &key, &value, &email, timestamp)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::types::Target;
    use std::collections::BTreeSet;

    #[test]
    fn test_batch_set_two_values_atomically() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        let mut batch = db.batch();
        batch.set_value(
            &target,
            "key1",
            &MetaValue::String("alpha".into()),
            "user@example.com",
            1000,
        );
        batch.set_value(
            &target,
            "key2",
            &MetaValue::String("beta".into()),
            "user@example.com",
            1000,
        );
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        db.apply_batch(batch).unwrap();

        let v1 = db.get_value(&target, "key1").unwrap();
        let v2 = db.get_value(&target, "key2").unwrap();
        assert_eq!(v1, Some(MetaValue::String("alpha".to_string())));
        assert_eq!(v2, Some(MetaValue::String("beta".to_string())));
    }

    #[test]
    fn test_batch_with_remove() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        // Pre-populate a key
        db.set_value(
            &target,
            "existing",
            &MetaValue::String("will-be-removed".into()),
            "user@example.com",
            1000,
        )
        .unwrap();

        let mut batch = db.batch();
        batch.set_value(
            &target,
            "new-key",
            &MetaValue::String("new-value".into()),
            "user@example.com",
            2000,
        );
        batch.remove(&target, "existing", "user@example.com", 2000);
        db.apply_batch(batch).unwrap();

        assert_eq!(
            db.get_value(&target, "new-key").unwrap(),
            Some(MetaValue::String("new-value".to_string()))
        );
        assert_eq!(db.get_value(&target, "existing").unwrap(), None);
    }

    #[test]
    fn test_batch_list_push() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        let mut batch = db.batch();
        batch.list_push(&target, "tags", "first", "user@example.com", 1000);
        batch.list_push(&target, "tags", "second", "user@example.com", 2000);
        db.apply_batch(batch).unwrap();

        let entry = db
            .get(&target.target_type, target.value_str(), "tags")
            .unwrap()
            .unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["first", "second"]);
    }

    #[test]
    fn test_batch_set_add() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        let mut batch = db.batch();
        batch.set_add(&target, "reviewers", "alice", "user@example.com", 1000);
        batch.set_add(&target, "reviewers", "bob", "user@example.com", 2000);
        db.apply_batch(batch).unwrap();

        let result = db.get_value(&target, "reviewers").unwrap().unwrap();
        let mut expected = BTreeSet::new();
        expected.insert("alice".to_string());
        expected.insert("bob".to_string());
        assert_eq!(result, MetaValue::Set(expected));
    }

    #[test]
    fn test_empty_batch_is_noop() {
        let db = Store::open_in_memory().unwrap();
        let batch = db.batch();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        db.apply_batch(batch).unwrap();
    }

    #[test]
    fn test_batch_rollback_on_error() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        // Set a string value, then batch a set_add on the same key (type mismatch)
        db.set_value(
            &target,
            "key",
            &MetaValue::String("value".into()),
            "user@example.com",
            1000,
        )
        .unwrap();

        let mut batch = db.batch();
        batch.set_value(
            &target,
            "new-key",
            &MetaValue::String("should-be-rolled-back".into()),
            "user@example.com",
            2000,
        );
        // This will fail because "key" is a string, not a set
        batch.set_add(&target, "key", "member", "user@example.com", 2000);

        let result = db.apply_batch(batch);
        assert!(result.is_err());

        // The first op (new-key) should have been rolled back
        assert_eq!(db.get_value(&target, "new-key").unwrap(), None);
        // The original value should be unchanged
        assert_eq!(
            db.get_value(&target, "key").unwrap(),
            Some(MetaValue::String("value".to_string()))
        );
    }

    #[test]
    fn test_batch_mixed_operations() {
        let db = Store::open_in_memory().unwrap();
        let target = Target::parse("commit:abc123").unwrap();

        let mut batch = db.batch();
        batch.set_value(
            &target,
            "name",
            &MetaValue::String("test".into()),
            "user@example.com",
            1000,
        );
        batch.list_push(&target, "tags", "v1.0", "user@example.com", 1000);
        batch.set_add(&target, "owners", "alice", "user@example.com", 1000);
        db.apply_batch(batch).unwrap();

        assert_eq!(
            db.get_value(&target, "name").unwrap(),
            Some(MetaValue::String("test".to_string()))
        );

        let entry = db
            .get(&target.target_type, target.value_str(), "tags")
            .unwrap()
            .unwrap();
        let list = crate::list_value::list_values_from_json(&entry.value).unwrap();
        assert_eq!(list, vec!["v1.0"]);

        let owners = db.get_value(&target, "owners").unwrap().unwrap();
        let mut expected = BTreeSet::new();
        expected.insert("alice".to_string());
        assert_eq!(owners, MetaValue::Set(expected));
    }
}
