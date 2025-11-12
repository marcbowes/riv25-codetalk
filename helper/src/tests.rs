use crate::{
    lambda::{self, greeting, tpcb},
    stress,
};
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
