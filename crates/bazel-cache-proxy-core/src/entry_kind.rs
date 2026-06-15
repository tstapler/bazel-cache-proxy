use std::fmt;
use std::str::FromStr;
use crate::error::CacheError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    AC,
    CAS,
}

impl EntryKind {
    pub fn path_segment(&self) -> &'static str {
        match self {
            EntryKind::AC => "AC",
            EntryKind::CAS => "CAS",
        }
    }
}

impl fmt::Display for EntryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntryKind::AC => write!(f, "ac"),
            EntryKind::CAS => write!(f, "cas"),
        }
    }
}

impl FromStr for EntryKind {
    type Err = CacheError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ac" => Ok(EntryKind::AC),
            "cas" => Ok(EntryKind::CAS),
            other => Err(CacheError::InvalidArgument(format!(
                "unknown entry kind: {other:?} (expected 'ac' or 'cas')"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_kind_from_str_cas() {
        assert_eq!("cas".parse::<EntryKind>().unwrap(), EntryKind::CAS);
    }

    #[test]
    fn entry_kind_from_str_ac() {
        assert_eq!("ac".parse::<EntryKind>().unwrap(), EntryKind::AC);
    }

    #[test]
    fn entry_kind_from_str_invalid() {
        assert!("blob".parse::<EntryKind>().is_err());
        assert!("".parse::<EntryKind>().is_err());
    }

    #[test]
    fn entry_kind_path_segment_roundtrip() {
        // Display gives lowercase, path_segment gives uppercase
        assert_eq!(EntryKind::CAS.to_string(), "cas");
        assert_eq!(EntryKind::CAS.path_segment(), "CAS");
    }
}
