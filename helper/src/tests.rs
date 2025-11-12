use crate::{
    db,
    lambda::{self, greeting, tpcb},
    stress,
};
use anyhow::Result;

#[derive(sqlx::FromRow)]
struct Transaction {
    id: uuid::Uuid,
    payer_id: i32,
    payee_id: i32,
    amount: i32,
    created_at: chrono::NaiveDateTime,
}

pub async fn run_test(chapter: u32) -> Result<()> {
    match chapter {
        0 => test_chapter0().await,
        1 => test_chapter1().await,
        2 => test_chapter2().await,
        3 => test_chapter3().await,
        4 => test_chapter4().await,
        _ => {
            eprintln!("Unknown test chapter: {}", chapter);
            std::process::exit(1);
        }
    }
}

async fn test_chapter0() -> Result<()> {
    println!("Testing Chapter 0: Basic Lambda invocation with DSQL connection\n");

    let req = greeting::Request {
        name: "reinvent".to_string(),
    };

    let response: greeting::Response = lambda::invoke_lambda(&req).await?;
    println!("Response: {:?}", response.greeting);

    if response.greeting.contains("connected to DSQL successfully") {
        println!("✅ Chapter 0 test PASSED");
    } else {
        anyhow::bail!("Test failed");
    }

    Ok(())
}

async fn test_chapter1() -> Result<()> {
    println!("Testing Chapter 1: Money transfer\n");

    let req = tpcb::Request {
        payer_id: 1,
        payee_id: 2,
        amount: 10,
    };

    let response: tpcb::Response = lambda::invoke_lambda(req).await?;

    if let Some(balance) = response.balance {
        println!("✅ Chapter 1 test PASSED");
        println!("   Payer balance after transfer: {}", balance);
    } else {
        anyhow::bail!("Test failed");
    }

    Ok(())
}

async fn test_chapter2() -> Result<()> {
    println!("Testing Chapter 2: Stress Test - 10K Invocations\n");
    stress::run_stress_test(10_000, 1_000, 1_000).await?;
    println!("✅ Chapter 2 test complete");
    Ok(())
}

async fn test_chapter3() -> Result<()> {
    println!("Testing Chapter 3: Transaction history with UUID primary keys\n");

    let req = tpcb::Request {
        payer_id: 1,
        payee_id: 2,
        amount: 10,
    };

    println!("Invoking Lambda function 'reinvent-dat401' with payload '{:?}'", req);
    let response: tpcb::Response = lambda::invoke_lambda(req).await?;

    if let Some(balance) = response.balance {
        println!("Response: balance = {}", balance);
        if let Some(duration) = response.duration {
            println!("  Duration: {}ms", duration);
        }
        if let Some(retries) = response.retries {
            println!("  Retries: {}", retries);
        }
    } else {
        anyhow::bail!("Test failed - no balance in response");
    }

    // Query the database to verify transaction was recorded
    println!("\nChecking transactions table...");
    let pool = db::get_pool().await?;

    let transactions: Vec<Transaction> = sqlx::query_as(
        "SELECT id, payer_id, payee_id, amount, created_at
         FROM transactions
         WHERE payer_id = $1
         ORDER BY created_at DESC
         LIMIT 5"
    )
    .bind(1i32)
    .fetch_all(&pool)
    .await?;

    println!("Found {} recent transactions:", transactions.len());
    for (i, tx) in transactions.iter().enumerate() {
        println!(
            "  {}. ID: {}, Payer: {}, Payee: {}, Amount: {}, Time: {}",
            i + 1, tx.id, tx.payer_id, tx.payee_id, tx.amount, tx.created_at
        );
    }

    println!("\n✅ Chapter 3 test PASSED");
    Ok(())
}

async fn test_chapter4() -> Result<()> {
    println!("Testing Chapter 4: 1M Invocations\n");
    stress::run_stress_test(1_000_000, 10_000, 1_000_000).await?;
    println!("✅ Chapter 4 test complete");
    Ok(())
}
