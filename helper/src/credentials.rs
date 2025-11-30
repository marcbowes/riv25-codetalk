use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

#[derive(Clone, Debug)]
struct CachedCredentials {
    credentials: Credentials,
    expires_at: SystemTime,
}

#[derive(Debug)]
pub struct CredentialCache {
    cached: Arc<RwLock<Option<CachedCredentials>>>,
    provider: aws_credential_types::provider::SharedCredentialsProvider,
}

impl CredentialCache {
    pub async fn new() -> Result<Self> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let provider = config.credentials_provider().unwrap().clone();

        Ok(Self {
            cached: Arc::new(RwLock::new(None)),
            provider,
        })
    }

    pub async fn get_credentials(&self) -> Result<Credentials> {
        // Check if we have valid cached credentials
        {
            let cached = self.cached.read().await;
            if let Some(cached_creds) = cached.as_ref() {
                // Refresh 5 minutes before expiry
                let refresh_threshold = SystemTime::now() + Duration::from_secs(300);
                if cached_creds.expires_at > refresh_threshold {
                    return Ok(cached_creds.credentials.clone());
                }
            }
        }

        // Need to fetch new credentials
        let mut cached = self.cached.write().await;

        // Double-check in case another task just updated
        if let Some(cached_creds) = cached.as_ref() {
            let refresh_threshold = SystemTime::now() + Duration::from_secs(300);
            if cached_creds.expires_at > refresh_threshold {
                return Ok(cached_creds.credentials.clone());
            }
        }

        // Fetch fresh credentials
        let credentials = self
            .provider
            .provide_credentials()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch credentials: {}", e))?;

        // Determine expiry time
        let expires_at = credentials
            .expiry()
            .unwrap_or_else(|| SystemTime::now() + Duration::from_secs(3600));

        *cached = Some(CachedCredentials {
            credentials: credentials.clone(),
            expires_at,
        });

        Ok(credentials)
    }
}
