use crate::error::Result;
use crate::session::Session;
use crate::types::{MetaValue, Target, ValueType};

/// A scoped handle for operations on a specific target within a session.
///
/// Created via [`Session::target()`]. Carries the target, email, and
/// timestamp from the session so callers never have to pass them.
///
/// # Example
///
/// ```ignore
/// let session = Session::discover()?;
/// let handle = session.target(&Target::parse("commit:abc123")?);
/// handle.set_value("agent:model", &MetaValue::String("claude".into()))?;
/// let val = handle.get_value("agent:model")?;
/// ```
pub struct SessionTargetHandle<'a> {
    session: &'a Session,
    target: Target,
}

impl<'a> SessionTargetHandle<'a> {
    pub(crate) fn new(session: &'a Session, target: Target) -> Self {
        Self { session, target }
    }

    /// Get a metadata value by key.
    pub fn get_value(&self, key: &str) -> Result<Option<MetaValue>> {
        self.session.store().get_value(&self.target, key)
    }

    /// Set a metadata value with convenience conversion.
    ///
    /// Accepts anything that converts to [`MetaValue`]: `&str`, `String`,
    /// `Vec<ListEntry>`, `BTreeSet<String>`, or `MetaValue` directly.
    ///
    /// ```ignore
    /// handle.set("key", "hello")?;                    // string
    /// handle.set("key", MetaValue::String("hello".into()))?; // explicit
    /// ```
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set(&self, key: &str, value: impl Into<MetaValue>) -> Result<()> {
        let meta_value = value.into();
        self.session.store().set_value(
            &self.target,
            key,
            &meta_value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Set a metadata value.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set_value(&self, key: &str, value: &MetaValue) -> Result<()> {
        self.session.store().set_value(
            &self.target,
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Remove a metadata key.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn remove(&self, key: &str) -> Result<bool> {
        self.session.store().remove(
            &self.target.target_type,
            self.target.value_str(),
            key,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Push a value onto a list.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_push(&self, key: &str, value: &str) -> Result<()> {
        self.session.store().list_push(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Pop a value from a list.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_pop(&self, key: &str, value: &str) -> Result<()> {
        self.session.store().list_pop(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Remove a list entry by index.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn list_remove(&self, key: &str, index: usize) -> Result<()> {
        self.session.store().list_remove(
            &self.target.target_type,
            self.target.value_str(),
            key,
            index,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Add a member to a set.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set_add(&self, key: &str, value: &str) -> Result<()> {
        self.session.store().set_add(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// Remove a member from a set.
    ///
    /// Uses the session's email and timestamp automatically.
    pub fn set_remove(&self, key: &str, value: &str) -> Result<()> {
        self.session.store().set_remove(
            &self.target.target_type,
            self.target.value_str(),
            key,
            value,
            self.session.email(),
            self.session.now(),
        )
    }

    /// The target this handle is scoped to.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// Get all metadata for this target as typed (key, value) pairs.
    ///
    /// Optionally filters by key prefix (e.g., `Some("agent")` returns
    /// all keys starting with `agent` or `agent:`).
    ///
    /// # Parameters
    ///
    /// - `prefix`: optional key prefix to filter by
    ///
    /// # Returns
    ///
    /// A vector of `(key, MetaValue)` pairs for matching metadata entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read or deserialization fails.
    pub fn get_all_values(&self, prefix: Option<&str>) -> Result<Vec<(String, MetaValue)>> {
        let entries = self.session.store().get_all(
            &self.target.target_type,
            self.target.value_str(),
            prefix,
        )?;
        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            let meta_value = match entry.value_type {
                ValueType::String => {
                    let s: String =
                        serde_json::from_str(&entry.value).unwrap_or_else(|_| entry.value.clone());
                    MetaValue::String(s)
                }
                ValueType::List => {
                    let entries = crate::list_value::parse_entries(&entry.value)?;
                    MetaValue::List(entries)
                }
                ValueType::Set => {
                    let members: Vec<String> = serde_json::from_str(&entry.value)?;
                    MetaValue::Set(members.into_iter().collect())
                }
            };
            result.push((entry.key, meta_value));
        }
        Ok(result)
    }

    /// Get list entries for a key on this target.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key name
    ///
    /// # Returns
    ///
    /// A vector of [`ListEntry`](crate::list_value::ListEntry) values with
    /// resolved content and timestamps.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is missing, the value is not a list, or
    /// the database read fails.
    pub fn list_entries(&self, key: &str) -> Result<Vec<crate::list_value::ListEntry>> {
        self.session
            .store()
            .list_entries(&self.target.target_type, self.target.value_str(), key)
    }

    /// Get authorship info (last author email and timestamp) for a key on this target.
    ///
    /// # Parameters
    ///
    /// - `key`: the metadata key name
    ///
    /// # Returns
    ///
    /// `Some(Authorship)` if the key has been modified at least once,
    /// `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the database read fails.
    pub fn get_authorship(&self, key: &str) -> Result<Option<crate::db::types::Authorship>> {
        self.session
            .store()
            .get_authorship(&self.target.target_type, self.target.value_str(), key)
    }
}
