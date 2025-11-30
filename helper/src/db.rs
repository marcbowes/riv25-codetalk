use crate::credentials::CredentialCache;
use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_dsql::auth_token::{AuthTokenGenerator, Config};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};

pub async fn get_pool(creds: &CredentialCache) -> Result<Pool<Postgres>> {
    let cluster_endpoint = std::env::var("CLUSTER_ENDPOINT")?;
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());

    let credentials = creds.get_credentials().await?;
    let credentials_provider =
        aws_credential_types::provider::SharedCredentialsProvider::new(credentials);

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(credentials_provider)
        .region(aws_config::Region::new(region.clone()))
        .load()
        .await;

    let config = Config::builder()
        .hostname(&cluster_endpoint)
        .region(aws_config::Region::new(region))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build config: {}", e))?;

    let signer = AuthTokenGenerator::new(config);
    let token = signer
        .db_connect_admin_auth_token(&sdk_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate admin token: {}", e))?;

    let options = PgConnectOptions::new()
        .host(&cluster_endpoint)
        .port(5432)
        .database("postgres")
        .username("admin")
        .password(token.as_str())
        .ssl_mode(sqlx::postgres::PgSslMode::Require);

    let pool = PgPoolOptions::new()
        .max_connections(1_000)
        .connect_with(options)
        .await?;

    Ok(pool)
}
