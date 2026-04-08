use crate::error::Result;
use crate::types::{MetaValue, Target};

use super::Store;

/// A scoped handle for operations on a specific target.
///
/// Created via [`Store::target()`]. Eliminates repeating target_type and
/// target_value on every operation.
///
/// # Example
///
/// ```ignore
/// let handle = store.target(&Target::parse("commit:abc123")?);
/// handle.set_value("agent:model", &MetaValue::String("claude".into()), email, ts)?;
/// let val = handle.get_value("agent:model")?;
/// ```
pub struct TargetHandle<'a> {
    store: &'a Store,
    target: Target,
}

impl<'a> TargetHandle<'a> {
    /// Create a new target handle.
    ///
    /// This is `pub(super)` so only [`Store::target()`] can construct it.
    pub(super) fn new(store: &'a Store, target: Target) -> Self {
        Self { store, target }
    }

    /// Get a metadata value by key as a type-safe [`MetaValue`].
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key to look up
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or deserialization fails.
    pub fn get_value(&self, key: &str) -> Result<Option<MetaValue>> {
        self.store.get_value(&self.target, key)
    }

    /// Set a metadata value using the type-safe [`MetaValue`] enum.
    ///
    /// # Parameters
    ///
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
        key: &str,
        value: &MetaValue,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        self.store
            .set_value(&self.target, key, value, email, timestamp)
    }

    /// Get the raw metadata entry (value as stored, with type and git_ref flag).
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key to look up
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn get(&self, key: &str) -> Result<Option<super::types::MetadataValue>> {
        self.store
            .get(&self.target.target_type, self.target.value_str(), key)
    }

    /// Remove a metadata key.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key to remove
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Returns
    ///
    /// `true` if the key existed and was removed, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn remove(&self, key: &str, email: &str, timestamp: i64) -> Result<bool> {
        self.store.remove(
            &self.target.target_type,
            self.target.value_str(),
            key,
            email,
            timestamp,
        )
    }

    /// Push a value onto a list.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key (must be a list or will be converted)
    /// - `value`: the string value to append
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn list_push(&self, key: &str, value: &str, email: &str, timestamp: i64) -> Result<()> {
        self.store.list_push(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            email,
            timestamp,
        )
    }

    /// Pop a value from a list.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key (must be a list)
    /// - `value`: the string value to remove (last occurrence)
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Errors
    ///
    /// Returns an error if the key is not a list or the value is not found.
    pub fn list_pop(&self, key: &str, value: &str, email: &str, timestamp: i64) -> Result<()> {
        self.store.list_pop(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            email,
            timestamp,
        )
    }

    /// Add a member to a set.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key (must be a set or will be created as one)
    /// - `value`: the string value to add
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Errors
    ///
    /// Returns an error if the key exists with a different type or the database operation fails.
    pub fn set_add(&self, key: &str, value: &str, email: &str, timestamp: i64) -> Result<()> {
        self.store.set_add(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            email,
            timestamp,
        )
    }

    /// Remove a member from a set.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key (must be a set)
    /// - `value`: the string value to remove
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (seconds since epoch)
    ///
    /// # Errors
    ///
    /// Returns an error if the key is not a set or the value is not found.
    pub fn set_remove(&self, key: &str, value: &str, email: &str, timestamp: i64) -> Result<()> {
        self.store.set_remove(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            email,
            timestamp,
        )
    }

    /// The target this handle is scoped to.
    #[must_use]
    pub fn target(&self) -> &Target {
        &self.target
    }
}
