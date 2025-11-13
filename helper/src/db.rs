use anyhow::Result;
use aws_sdk_dsql::auth_token::{AuthToken, AuthTokenGenerator, Config};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};

async fn generate_token(cluster_endpoint: &str, region: &str, is_admin: bool) -> Result<AuthToken> {
    let cache = crate::credentials::get_credential_cache().await;

    // Get cached credentials
    let credentials = cache.get_credentials().await?;

    // Build SDK config with our cached credentials
    let credentials_provider = aws_credential_types::provider::SharedCredentialsProvider::new(credentials);
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(credentials_provider)
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;

    let config = Config::builder()
        .hostname(cluster_endpoint)
        .region(aws_config::Region::new(region.to_string()))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build config: {}", e))?;

    let signer = AuthTokenGenerator::new(config);

    // Use the SDK config with cached credentials
    let token = if is_admin {
        signer
            .db_connect_admin_auth_token(&sdk_config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to generate admin token: {}", e))?
    } else {
        signer
            .db_connect_auth_token(&sdk_config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to generate token: {}", e))?
    };

    Ok(token)
}

pub async fn get_pool() -> Result<Pool<Postgres>> {
    let cluster_endpoint = std::env::var("CLUSTER_ENDPOINT")?;
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());

    let token = generate_token(&cluster_endpoint, &region, true).await?;

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
