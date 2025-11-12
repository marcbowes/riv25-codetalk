use anyhow::Result;
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

    #[derive(Serialize)]
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

pub async fn invoke_lambda<T: Serialize, R: DeserializeOwned>(payload: T) -> Result<R> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

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
