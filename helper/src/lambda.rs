use std::time::Duration;

use anyhow::Result;
use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion};
use aws_sdk_lambda::{error::SdkError, primitives::Blob, Client};
use serde::{de::DeserializeOwned, Serialize};

use crate::credentials::CredentialCache;

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
        pub balance: Option<u32>,
        pub duration: Option<u64>,
        pub retries: Option<u32>,
        pub error: Option<String>,
        pub error_code: Option<String>,
    }
}

pub async fn client(creds: &CredentialCache) -> Result<Client> {
    let credentials = creds.get_credentials().await?;
    let credentials_provider =
        aws_credential_types::provider::SharedCredentialsProvider::new(credentials);

    let config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(credentials_provider)
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(30))
                .build(),
        )
        .retry_config(RetryConfig::standard().with_max_attempts(3))
        .load()
        .await;
    Ok(Client::new(&config))
}

pub async fn invoke<T: Serialize, R: DeserializeOwned>(client: &Client, payload: T) -> Result<R> {
    let payload_str = serde_json::to_string(&payload)?;
    let response = client
        .invoke()
        .function_name(FUNCTION_NAME)
        .payload(Blob::new(payload_str.as_bytes()))
        .send()
        .await;
    let response = match response {
        Ok(r) => r,
        Err(err) => {
            if let SdkError::DispatchFailure(ref d) = err {
                tracing::error!(?d, "dispatch failure");
            }
            return Err(err)?;
        }
    };

    let response_bytes = response.payload().unwrap().as_ref();
    tracing::trace!(?response_bytes);

    if let Some(err) = response.function_error() {
        tracing::trace!(?err, "function error");
        let msg = String::from_utf8_lossy(response_bytes);
        anyhow::bail!("function error: {msg}");
    }

    Ok(serde_json::from_slice(response_bytes)?)
}
