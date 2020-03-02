//! Tools for writing trace descriptions

use chrono::{DateTime, Utc};
use std::fmt;

use uuid::Uuid;

/// Macro for easy extraction of potentially absent fields from trace event structures.
///
/// Used with fields that are `Option<T>` types.
///
/// ```
///     // Retrieves the value of self.foo if it is present, otherwise it generates and
///     // propagates a PVMError::MissingField
///     let f = field!(self.foo);
///
///     // As above, but takes a reference to the field value instead.
///     let rf = field!(&self.foo);
/// ```
///
#[macro_export]
macro_rules! field {
    ($TR:ident. $F:ident) => {
        $TR.$F.clone().ok_or(PVMError::MissingField {
            evt: $TR.event.clone(),
            field: stringify!($F),
        })?
    };
    (&$TR:ident. $F:ident) => {
        $TR.$F.as_ref().ok_or(PVMError::MissingField {
            evt: $TR.event.clone(),
            field: stringify!($F),
        })?
    };
}

pub trait MapFmt {
    fn entry(&self, f: &mut fmt::DebugMap, key: &str);
}

impl<T: fmt::Debug> MapFmt for T {
    default fn entry(&self, f: &mut fmt::DebugMap, key: &str) {
        f.entry(&key, self);
    }
}

impl<T: MapFmt + fmt::Debug> MapFmt for Option<T> {
    fn entry(&self, f: &mut fmt::DebugMap, key: &str) {
        if let Some(v) = self {
            v.entry(f, key);
        }
    }
}

impl MapFmt for Uuid {
    fn entry(&self, f: &mut fmt::DebugMap, key: &str) {
        f.entry(&key, &self.to_hyphenated_ref().to_string());
    }
}

impl MapFmt for DateTime<Utc> {
    fn entry(&self, f: &mut fmt::DebugMap, key: &str) {
        f.entry(&key, &self.to_rfc3339());
    }
}

#[macro_export]
macro_rules! fields_to_map {
    ($ret:ident; ) => {};
    ($ret:ident; $s:ident.$f:ident) => {
        $s.$f.entry(&mut $ret, &stringify!($f));
    };
    ($ret:ident; $s:ident.$f:ident, $($tail:tt)*) => {
        fields_to_map!($ret; $s.$f);
        fields_to_map!($ret; $($tail)*)
    };
}

pub mod cadets;
