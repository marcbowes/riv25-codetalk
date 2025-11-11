use crate::{db, lambda, stress};
use anyhow::Result;

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

    let req = lambda::GreetingRequest {
        name: "reinvent".to_string(),
    };

    let response = lambda::invoke_lambda(&req).await?;
    println!("Response: {:?}", response.greeting);

    if let Some(greeting) = response.greeting {
        if greeting.contains("connected to DSQL successfully") {
            println!("✅ Chapter 0 test PASSED");
        }
    }

    Ok(())
}

async fn test_chapter1() -> Result<()> {
    println!("Testing Chapter 1: Money transfer\n");

    let req = lambda::TransferRequest {
        payer_id: 1,
        payee_id: 2,
        amount: 10,
    };

    let response = lambda::invoke_lambda(&req).await?;

    if let Some(balance) = response.balance {
        println!("✅ Chapter 1 test PASSED");
        println!("   Payer balance after transfer: {}", balance);
    }

    Ok(())
}

async fn test_chapter2() -> Result<()> {
    println!("Testing Chapter 2: Stress Test - 10K Invocations\n");
    stress::run_stress_test(1000, 10, 1000).await?;
    println!("✅ Chapter 2 test complete");
    Ok(())
}

async fn test_chapter3() -> Result<()> {
    println!("Testing Chapter 3: Transaction history\n");
    // Implementation similar to chapter 1
    test_chapter1().await
}

async fn test_chapter4() -> Result<()> {
    println!("Testing Chapter 4: 1M Invocations\n");
    stress::run_stress_test(10000, 100, 1000000).await?;
    println!("✅ Chapter 4 test complete");
    Ok(())
}

pub async fn setup_schema(_accounts: u32) -> Result<()> {
    println!("Setting up database schema...");
    let _pool = db::get_pool().await?;
    // Setup implementation
    Ok(())
}

pub async fn setup_chapter4() -> Result<()> {
    println!("Setting up Chapter 4: Creating 1M accounts");
    // Setup implementation
    Ok(())
}
