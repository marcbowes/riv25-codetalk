use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_lambda::{Client, primitives::Blob};
use serde::{Deserialize, Serialize};

const FUNCTION_NAME: &str = "reinvent-dat401";

#[derive(Serialize)]
pub struct TransferRequest {
    pub payer_id: u32,
    pub payee_id: u32,
    pub amount: u32,
}

#[derive(Serialize)]
pub struct GreetingRequest {
    pub name: String,
}

#[derive(Deserialize)]
pub struct Response {
    pub greeting: Option<String>,
    pub balance: Option<i32>,
    pub duration: Option<u64>,
    pub retries: Option<u32>,
    pub error: Option<String>,
}

pub async fn invoke_lambda<T: Serialize>(payload: &T) -> Result<Response> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let payload_str = serde_json::to_string(payload)?;
    let response = client
        .invoke()
        .function_name(FUNCTION_NAME)
        .payload(Blob::new(payload_str.as_bytes()))
        .send()
        .await?;

    let response_bytes = response.payload().unwrap().as_ref();
    let result: Response = serde_json::from_slice(response_bytes)?;

    Ok(result)
}
