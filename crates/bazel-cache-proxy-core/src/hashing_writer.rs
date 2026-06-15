use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use sha2::{Digest as Sha2Digest, Sha256};
use tokio::io::AsyncWrite;
use crate::{digest::Digest, error::CacheError};

/// Wraps an `AsyncWrite` and computes the SHA-256 hash of all bytes written.
/// After all bytes are written, call `finalize(expected)` to verify the digest.
pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes_written: i64,
}

impl<W: AsyncWrite + Unpin> HashingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }

    /// Verifies the SHA-256 and byte count match `expected`.
    /// Returns `Err(HashMismatch)` or `Err(SizeMismatch)` on failure.
    pub fn finalize(self, expected: &Digest) -> Result<(), CacheError> {
        let actual_hash = format!("{:x}", self.hasher.finalize());
        let actual_size = self.bytes_written;

        if actual_hash != expected.hash() {
            return Err(CacheError::HashMismatch {
                expected: expected.hash().to_string(),
                actual: actual_hash,
            });
        }
        if actual_size != expected.size() {
            return Err(CacheError::SizeMismatch {
                expected: expected.size(),
                actual: actual_size,
            });
        }
        Ok(())
    }

    pub fn bytes_written(&self) -> i64 {
        self.bytes_written
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for HashingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                self.hasher.update(&buf[..n]);
                self.bytes_written += n as i64;
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    async fn hash_bytes(input: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(input);
        format!("{:x}", h.finalize())
    }

    #[tokio::test]
    async fn hashing_writer_empty_produces_sha256_of_empty() {
        let buf: Vec<u8> = Vec::new();
        let w = HashingWriter::new(buf);
        let expected = Digest::new(hash_bytes(b"").await, 0).unwrap();
        w.finalize(&expected).unwrap();
    }

    #[tokio::test]
    async fn hashing_writer_single_chunk_matches_sha256sum() {
        let input = b"hello world";
        let expected_hash = hash_bytes(input).await;
        let mut buf = Vec::new();
        let mut w = HashingWriter::new(&mut buf);
        w.write_all(input).await.unwrap();
        let expected = Digest::new(expected_hash, input.len() as i64).unwrap();
        w.finalize(&expected).unwrap();
    }

    #[tokio::test]
    async fn hashing_writer_multi_chunk_consistent() {
        let input = b"hello world this is a test";
        let expected_hash = hash_bytes(input).await;

        // Write in one chunk
        let mut buf1 = Vec::new();
        let mut w1 = HashingWriter::new(&mut buf1);
        w1.write_all(input).await.unwrap();

        // Write in multiple chunks
        let mut buf2 = Vec::new();
        let mut w2 = HashingWriter::new(&mut buf2);
        for chunk in input.chunks(3) {
            w2.write_all(chunk).await.unwrap();
        }

        let expected = Digest::new(expected_hash, input.len() as i64).unwrap();
        w1.finalize(&expected).unwrap();
        // Reset w2 finalize with same expected
        let expected2 = Digest::new(hash_bytes(input).await, input.len() as i64).unwrap();
        w2.finalize(&expected2).unwrap();
    }

    #[tokio::test]
    async fn hashing_writer_mismatch_returns_error() {
        let input = b"hello world";
        let wrong_hash = "a".repeat(64);
        let mut buf = Vec::new();
        let mut w = HashingWriter::new(&mut buf);
        w.write_all(input).await.unwrap();
        let wrong_digest = Digest::new(wrong_hash, input.len() as i64).unwrap();
        assert!(matches!(w.finalize(&wrong_digest), Err(CacheError::HashMismatch { .. })));
    }

    #[tokio::test]
    async fn hashing_writer_correct_digest_returns_ok() {
        let input = b"correct content";
        let hash = hash_bytes(input).await;
        let mut buf = Vec::new();
        let mut w = HashingWriter::new(&mut buf);
        w.write_all(input).await.unwrap();
        let expected = Digest::new(hash, input.len() as i64).unwrap();
        assert!(w.finalize(&expected).is_ok());
    }
}
