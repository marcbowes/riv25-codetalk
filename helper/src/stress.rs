use crate::lambda::{invoke_lambda, TransferRequest};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Instant;
use tokio::task::JoinSet;

pub async fn run_stress_test(
    parallel_calls: usize,
    iterations: usize,
    num_accounts: u32,
) -> Result<()> {
    let total_calls = parallel_calls * iterations;

    println!("Total invocations: {}", total_calls);
    println!("Parallel requests per batch: {}", parallel_calls);
    println!("Number of batches: {}\n", iterations);

    let pb = ProgressBar::new(total_calls as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{bar:40}] {pos}/{len} ({per_sec}) {msg}")?
            .progress_chars("=>-"),
    );

    let start = Instant::now();
    let mut success = 0;
    let mut errors = 0;

    for _ in 0..iterations {
        let mut tasks = JoinSet::new();

        for _ in 0..parallel_calls {
            let payer_id = rand::random::<u32>() % num_accounts + 1;
            let mut payee_id = rand::random::<u32>() % num_accounts + 1;
            while payee_id == payer_id {
                payee_id = rand::random::<u32>() % num_accounts + 1;
            }

            tasks.spawn(async move {
                invoke_lambda(&TransferRequest {
                    payer_id,
                    payee_id,
                    amount: 1,
                })
                .await
            });
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(response)) => {
                    if response.error.is_some() {
                        errors += 1;
                    } else {
                        success += 1;
                    }
                }
                _ => errors += 1,
            }
            pb.inc(1);
        }
    }

    pb.finish();

    let elapsed = start.elapsed();
    println!("\nTotal calls: {}", total_calls);
    println!("Successful: {} ({:.2}%)", success, (success as f64 / total_calls as f64) * 100.0);
    println!("Errors: {}", errors);
    println!("Total time: {:.2}s", elapsed.as_secs_f64());

    Ok(())
}
