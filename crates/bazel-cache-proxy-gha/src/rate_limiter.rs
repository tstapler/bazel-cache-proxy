use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct TokenBucket {
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)]
    max_tokens: u32,
}

impl TokenBucket {
    pub fn new(max_tokens: u32, refill_count: u32, refill_interval_ms: u64) -> Arc<Self> {
        let semaphore = Arc::new(Semaphore::new(max_tokens as usize));
        let bucket = Arc::new(Self {
            semaphore: semaphore.clone(),
            max_tokens,
        });
        let sem_clone = semaphore.clone();
        let max = max_tokens as usize;
        tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(refill_interval_ms);
            loop {
                tokio::time::sleep(interval).await;
                let current = sem_clone.available_permits();
                let to_add = (refill_count as usize).min(max.saturating_sub(current));
                if to_add > 0 {
                    sem_clone.add_permits(to_add);
                }
            }
        });
        bucket
    }

    pub fn new_default() -> Arc<Self> {
        Self::new(200, 4, 1200)
    }

    pub async fn acquire(&self) {
        let permit = self.semaphore.acquire().await.expect("semaphore closed");
        permit.forget(); // consume the token — do not release it back
    }

    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn token_bucket_allows_burst_at_startup() {
        let bucket = TokenBucket::new(10, 4, 60000);
        for _ in 0..10 {
            bucket.acquire().await;
        }
        assert_eq!(bucket.available(), 0);
    }

    #[tokio::test]
    async fn token_bucket_refills_after_interval() {
        let bucket = TokenBucket::new(5, 5, 50);
        for _ in 0..5 {
            bucket.acquire().await;
        }
        // 6th acquire should succeed after refill
        tokio::time::timeout(std::time::Duration::from_millis(500), bucket.acquire())
            .await
            .expect("should acquire after refill within 500ms");
    }
}
