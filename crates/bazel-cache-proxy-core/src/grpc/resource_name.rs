/// Parsed ByteStream resource name for a read request.
/// Format: `[{instance_name}/]blobs/{hash}/{size}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResourceName {
    pub instance_name: String,
    pub hash: String,
    pub size: i64,
}

/// Parsed ByteStream resource name for a write request.
/// Format: `[{instance_name}/]uploads/{uuid}/blobs/{hash}/{size}`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteResourceName {
    pub instance_name: String,
    pub upload_id: String,
    pub hash: String,
    pub size: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ResourceNameError {
    #[error("invalid resource name format: {0}")]
    InvalidFormat(String),
    #[error("invalid size in resource name: {0}")]
    InvalidSize(String),
}

impl ReadResourceName {
    pub fn parse(resource_name: &str) -> Result<Self, ResourceNameError> {
        let segments: Vec<&str> = resource_name.split('/').collect();
        let blobs_pos = segments
            .iter()
            .position(|&s| s == "blobs")
            .ok_or_else(|| ResourceNameError::InvalidFormat(resource_name.to_string()))?;

        if blobs_pos + 2 >= segments.len() {
            return Err(ResourceNameError::InvalidFormat(resource_name.to_string()));
        }

        let hash = segments[blobs_pos + 1].to_string();
        let size = segments[blobs_pos + 2]
            .parse::<i64>()
            .map_err(|_| ResourceNameError::InvalidSize(segments[blobs_pos + 2].to_string()))?;

        let instance_name = if blobs_pos > 0 {
            segments[..blobs_pos].join("/")
        } else {
            String::new()
        };

        Ok(Self { instance_name, hash, size })
    }
}

impl WriteResourceName {
    pub fn parse(resource_name: &str) -> Result<Self, ResourceNameError> {
        let segments: Vec<&str> = resource_name.split('/').collect();
        let uploads_pos = segments
            .iter()
            .position(|&s| s == "uploads")
            .ok_or_else(|| ResourceNameError::InvalidFormat(resource_name.to_string()))?;

        // After uploads: {uuid}/blobs/{hash}/{size} → need 4 more segments
        if uploads_pos + 4 >= segments.len() {
            return Err(ResourceNameError::InvalidFormat(resource_name.to_string()));
        }

        let upload_id = segments[uploads_pos + 1].to_string();

        if segments[uploads_pos + 2] != "blobs" {
            return Err(ResourceNameError::InvalidFormat(resource_name.to_string()));
        }

        let hash = segments[uploads_pos + 3].to_string();
        let size = segments[uploads_pos + 4]
            .parse::<i64>()
            .map_err(|_| ResourceNameError::InvalidSize(segments[uploads_pos + 4].to_string()))?;

        let instance_name = if uploads_pos > 0 {
            segments[..uploads_pos].join("/")
        } else {
            String::new()
        };

        Ok(Self { instance_name, upload_id, hash, size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_no_instance() {
        let r = ReadResourceName::parse("blobs/abc123/42").unwrap();
        assert_eq!(r.instance_name, "");
        assert_eq!(r.hash, "abc123");
        assert_eq!(r.size, 42);
    }

    #[test]
    fn test_read_with_instance() {
        let r = ReadResourceName::parse("myproject/blobs/abc123/42").unwrap();
        assert_eq!(r.instance_name, "myproject");
        assert_eq!(r.hash, "abc123");
        assert_eq!(r.size, 42);
    }

    #[test]
    fn test_write_no_instance() {
        let r = WriteResourceName::parse("uploads/some-uuid/blobs/abc123/42").unwrap();
        assert_eq!(r.instance_name, "");
        assert_eq!(r.upload_id, "some-uuid");
        assert_eq!(r.hash, "abc123");
        assert_eq!(r.size, 42);
    }

    #[test]
    fn test_write_with_instance() {
        let r = WriteResourceName::parse("myproject/uploads/some-uuid/blobs/abc123/42").unwrap();
        assert_eq!(r.instance_name, "myproject");
        assert_eq!(r.upload_id, "some-uuid");
        assert_eq!(r.hash, "abc123");
        assert_eq!(r.size, 42);
    }

    #[test]
    fn test_read_missing_blobs_segment() {
        assert!(ReadResourceName::parse("myproject/abc123/42").is_err());
    }

    #[test]
    fn test_write_missing_uploads_segment() {
        assert!(WriteResourceName::parse("myproject/blobs/abc123/42").is_err());
    }
}
