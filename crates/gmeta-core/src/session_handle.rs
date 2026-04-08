use crate::error::Result;
use crate::session::Session;
use crate::types::{MetaValue, Target};

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
}
