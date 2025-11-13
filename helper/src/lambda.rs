use anyhow::Result;
use async_once_cell::OnceCell;
use aws_config::BehaviorVersion;
use aws_sdk_lambda::{primitives::Blob, Client};
use serde::{de::DeserializeOwned, Serialize};

const FUNCTION_NAME: &str = "reinvent-dat401";

pub mod greeting {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize)]
    pub struct Request {
        pub name: String,
    }

    #[derive(Deserialize)]
    pub struct Response {
        pub greeting: String,
    }
}

pub mod tpcb {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Debug)]
    pub struct Request {
        pub payer_id: u32,
        pub payee_id: u32,
        pub amount: u32,
    }

    #[derive(Deserialize)]
    pub struct Response {
        pub balance: Option<String>,
        pub duration: Option<u64>,
        pub retries: Option<u32>,
        pub error: Option<String>,
        pub error_code: Option<String>,
    }
}

// Wrapper that implements ProvideCredentials using our cache
#[derive(Clone, Debug)]
struct CachedCredentialsProvider {
    cache: &'static crate::credentials::CredentialCache,
}

impl aws_credential_types::provider::ProvideCredentials for CachedCredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            self.cache
                .get_credentials()
                .await
                .map_err(|e| aws_credential_types::provider::error::CredentialsError::not_loaded(e))
        })
    }
}

const CLIENT: OnceCell<Client> = OnceCell::new();

pub async fn invoke_lambda<T: Serialize, R: DeserializeOwned>(payload: T) -> Result<R> {
    let client = CLIENT
        .get_or_init(async {
            let cache = crate::credentials::get_credential_cache().await;

            // Create a credentials provider that uses our cache
            let credentials_provider = aws_credential_types::provider::SharedCredentialsProvider::new(
                CachedCredentialsProvider {
                    cache,
                }
            );

            let config = aws_config::defaults(BehaviorVersion::latest())
                .credentials_provider(credentials_provider)
                .load()
                .await;
            Client::new(&config)
        })
        .await
        .clone();

    let payload_str = serde_json::to_string(&payload)?;
    let response = client
        .invoke()
        .function_name(FUNCTION_NAME)
        .payload(Blob::new(payload_str.as_bytes()))
        .send()
        .await?;

    let response_bytes = response.payload().unwrap().as_ref();
    tracing::trace!(?response_bytes);

    if let Some(err) = response.function_error() {
        tracing::trace!(?err, "function error");
        let msg = String::from_utf8_lossy(response_bytes);
        anyhow::bail!("function error: {msg}");
    }

    Ok(serde_json::from_slice(response_bytes)?)
}
