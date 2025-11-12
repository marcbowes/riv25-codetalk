use crate::db;
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};

pub async fn setup_schema(num_accounts: u32) -> Result<()> {
    println!("Setting up database schema...");
    let pool = db::get_pool().await?;

    // Create accounts table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            balance NUMERIC NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;
    println!("Created accounts table");

    // Create transactions table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS transactions (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            payer_id INT,
            payee_id INT,
            amount INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        "#,
    )
    .execute(&pool)
    .await?;
    println!("Created transactions table");

    // Clear existing data
    sqlx::query("DELETE FROM accounts").execute(&pool).await?;
    sqlx::query("DELETE FROM transactions").execute(&pool).await?;
    println!("Cleared existing data");

    // Insert accounts using generate_series
    println!("Inserting {} accounts...", num_accounts);
    sqlx::query("INSERT INTO accounts (id, balance) SELECT id, 100 FROM generate_series(1, $1) AS id")
        .bind(num_accounts as i32)
        .execute(&pool)
        .await?;

    println!("Database setup complete!");
    Ok(())
}

pub async fn setup_chapter4() -> Result<()> {
    println!("Setting up Chapter 4: Creating 1M accounts\n");

    const TARGET_ACCOUNTS: i64 = 1_000_000;
    let pool = db::get_pool().await?;

    // Check current account count
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM accounts")
        .fetch_one(&pool)
        .await?;
    let current_count = row.0;

    println!("Current account count: {}", current_count);

    if current_count >= TARGET_ACCOUNTS {
        println!("Already have sufficient accounts\n");
        println!("✅ Chapter 4 setup complete");
        return Ok(());
    }

    let needed_accounts = TARGET_ACCOUNTS - current_count;
    println!("Inserting {} more accounts to reach {}...\n", needed_accounts, TARGET_ACCOUNTS);

    let pb = ProgressBar::new(needed_accounts as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{bar:40}] {pos}/{len} accounts")?
            .progress_chars("=>-"),
    );

    const BATCH_SIZE: i64 = 10_000;
    let mut inserted = 0i64;

    while inserted < needed_accounts {
        let start_id = current_count + inserted + 1;
        let end_id = (current_count + inserted + BATCH_SIZE).min(TARGET_ACCOUNTS);
        let batch_count = end_id - start_id + 1;

        sqlx::query("INSERT INTO accounts (id, balance) SELECT id, 100 FROM generate_series($1, $2) AS id")
            .bind(start_id as i32)
            .bind(end_id as i32)
            .execute(&pool)
            .await?;

        inserted += batch_count;
        pb.set_position(inserted as u64);
    }

    pb.finish();
    println!("\nAccount insertion complete!");
    println!("✅ Chapter 4 setup complete");

    Ok(())
}
