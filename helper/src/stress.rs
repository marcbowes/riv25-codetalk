use crate::lambda::{self, tpcb};
use anyhow::Result;
use aws_sdk_lambda::Client;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

pub async fn run_stress_test(
    client: &Client,
    total_calls: usize,
    parallel_calls: usize,
    num_accounts: u32,
) -> Result<()> {
    println!("Total invocations: {}", total_calls);
    println!("Max parallel requests: {}", parallel_calls);
    println!();

    let client = Arc::new(client.clone());

    let m = MultiProgress::new();

    let concurrent = m.add(ProgressBar::new(parallel_calls as u64));
    let pb = m.add(ProgressBar::new(total_calls as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{bar:40}] {pos}/{len} ({per_sec}) {msg}")?
            .progress_chars("=>-"),
    );

    let start = Instant::now();
    let mut success = 0;
    let mut errors = 0;
    let mut min_duration = u64::MAX;
    let mut max_duration = 0u64;
    let mut total_duration = 0u64;
    let mut duration_count = 0usize;
    let mut total_retries = 0u64;
    let mut max_retries = 0u32;
    let mut transactions_with_retries = 0usize;
    let mut error_types: HashMap<String, usize> = HashMap::new();

    let mut tasks = JoinSet::new();
    let mut launched = 0;

    loop {
        let rem = parallel_calls - tasks.len();
        if launched < total_calls && rem > 0 {
            for _ in 0..rem {
                let payer_id = rand::random::<u32>() % num_accounts + 1;
                let mut payee_id = rand::random::<u32>() % num_accounts + 1;
                while payee_id == payer_id {
                    payee_id = rand::random::<u32>() % num_accounts + 1;
                }

                let c = client.clone();
                tasks.spawn(async move {
                    lambda::invoke::<_, tpcb::Response>(&c, tpcb::Request {
                        payer_id,
                        payee_id,
                        amount: 1,
                    })
                    .await
                });
                launched += 1;
                concurrent.inc(1);
            }
        }

        if let Some(result) = tasks.join_next().await {
            concurrent.dec(1);

            match result {
                Ok(Ok(response)) => {
                    if let Some(error) = &response.error {
                        errors += 1;
                        let error_key = if let Some(code) = &response.error_code {
                            format!("{} ({})", error, code)
                        } else {
                            error.clone()
                        };
                        *error_types.entry(error_key).or_insert(0) += 1;
                    } else {
                        success += 1;
                    }

                    if let Some(duration) = response.duration {
                        min_duration = min_duration.min(duration);
                        max_duration = max_duration.max(duration);
                        total_duration += duration;
                        duration_count += 1;
                    }

                    if let Some(retries) = response.retries {
                        total_retries += retries as u64;
                        max_retries = max_retries.max(retries);
                        if retries > 0 {
                            transactions_with_retries += 1;
                        }
                    }
                }
                Ok(Err(err)) => {
                    errors += 1;
                    *error_types
                        .entry(format!("Lambda invocation failed: {err}"))
                        .or_insert(0) += 1;
                }
                _ => unreachable!("tasks should not be crashing"),
            }

            pb.inc(1);
        } else {
            break;
        }
    }

    concurrent.finish_and_clear();
    pb.finish_and_clear();

    let elapsed = start.elapsed();

    println!();
    println!("{}", "=".repeat(60));
    println!("STATS");
    println!("{}", "=".repeat(60));
    println!("Total calls:        {}", total_calls);
    println!(
        "Successful:         {} ({:.2}%)",
        success,
        (success as f64 / total_calls as f64) * 100.0
    );
    println!(
        "Errors:             {} ({:.2}%)",
        errors,
        (errors as f64 / total_calls as f64) * 100.0
    );
    println!();
    println!("Total time:         {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput:         {:.0} calls/second",
        total_calls as f64 / elapsed.as_secs_f64()
    );
    println!();

    if duration_count > 0 {
        let avg_duration = total_duration as f64 / duration_count as f64;
        println!("Lambda Execution Times:");
        println!("  Min:                {:.2}ms", min_duration);
        println!("  Max:                {:.2}ms", max_duration);
        println!("  Avg:                {:.2}ms", avg_duration);
        println!();
    }

    if total_retries > 0 {
        let avg_retries = total_retries as f64 / total_calls as f64;
        let retry_rate = (transactions_with_retries as f64 / total_calls as f64) * 100.0;
        println!("OCC Retry Statistics:");
        println!("  Total retries:      {}", total_retries);
        println!("  Max retries:        {}", max_retries);
        println!("  Avg retries/call:   {:.2}", avg_retries);
        println!(
            "  Transactions with retries: {} ({:.2}%)",
            transactions_with_retries, retry_rate
        );
        println!();
    }

    if !error_types.is_empty() {
        println!("Error Breakdown:");
        let mut error_vec: Vec<_> = error_types.iter().collect();
        error_vec.sort_by(|a, b| b.1.cmp(a.1));
        for (error_type, count) in error_vec {
            println!("  {}: {}", error_type, count);
        }
        println!();
    }

    Ok(())
}

pub async fn run_sustained_load(
    client: &Client,
    invocations_per_sec: u32,
    num_accounts: u32,
) -> Result<()> {
    use async_rate_limiter::RateLimiter;

    println!("Sustained Load Generator");
    println!("========================");
    println!("Target rate: {}/sec", invocations_per_sec);
    println!("Max in-flight: {}", invocations_per_sec * 50);
    println!("Account pool: {}", num_accounts);
    println!();
    println!("Press Ctrl-C to stop...");
    println!();

    let client = Arc::new(client.clone());

    let running = Arc::new(AtomicBool::new(true));
    let total_calls = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let total_duration = Arc::new(AtomicU64::new(0));
    let total_retries = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicUsize::new(0));

    let running_clone = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\nShutting down...");
        running_clone.store(false, Ordering::SeqCst);
    });

    let start = Instant::now();

    let m = MultiProgress::new();
    let pb = m.add(ProgressBar::new_spinner());
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );

    let limiter = RateLimiter::new(invocations_per_sec as usize);
    let max_in_flight = invocations_per_sec as usize * 50;

    let stats_running = running.clone();
    let stats_total = total_calls.clone();
    let stats_success = success_count.clone();
    let stats_errors = error_count.clone();
    let stats_in_flight = in_flight.clone();
    let stats_pb = pb.clone();
    let stats_handle = tokio::spawn(async move {
        let mut last_calls = 0usize;
        while stats_running.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let current_calls = stats_total.load(Ordering::Relaxed);
            let calls_per_sec = current_calls - last_calls;
            let success = stats_success.load(Ordering::Relaxed);
            let errors = stats_errors.load(Ordering::Relaxed);
            let flying = stats_in_flight.load(Ordering::Relaxed);
            let success_rate = if current_calls > 0 {
                (success as f64 / current_calls as f64) * 100.0
            } else {
                0.0
            };

            stats_pb.set_message(format!(
                "Calls: {} | {}/s | Success: {:.4}% | Errors: {} | In-flight: {}",
                current_calls, calls_per_sec, success_rate, errors, flying
            ));

            last_calls = current_calls;
        }
    });

    let mut handles = Vec::new();
    while running.load(Ordering::SeqCst) {
        limiter.acquire().await;

        if !running.load(Ordering::SeqCst) {
            break;
        }

        while in_flight.load(Ordering::Relaxed) >= max_in_flight {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if !running.load(Ordering::SeqCst) {
                break;
            }
        }

        if !running.load(Ordering::SeqCst) {
            break;
        }

        let payer_id = rand::random::<u32>() % num_accounts + 1;
        let mut payee_id = rand::random::<u32>() % num_accounts + 1;
        while payee_id == payer_id {
            payee_id = rand::random::<u32>() % num_accounts + 1;
        }

        let c = client.clone();
        let total = total_calls.clone();
        let success = success_count.clone();
        let errors = error_count.clone();
        let duration_sum = total_duration.clone();
        let retries_sum = total_retries.clone();
        let flying = in_flight.clone();

        flying.fetch_add(1, Ordering::Relaxed);

        let handle = tokio::spawn(async move {
            let result = lambda::invoke::<_, tpcb::Response>(
                &c,
                tpcb::Request {
                    payer_id,
                    payee_id,
                    amount: 1,
                },
            )
            .await;

            flying.fetch_sub(1, Ordering::Relaxed);
            total.fetch_add(1, Ordering::Relaxed);

            match result {
                Ok(response) => {
                    if response.error.is_some() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        success.fetch_add(1, Ordering::Relaxed);
                    }

                    if let Some(duration) = response.duration {
                        duration_sum.fetch_add(duration, Ordering::Relaxed);
                    }

                    if let Some(retries) = response.retries {
                        retries_sum.fetch_add(retries as u64, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    pb.set_message("Waiting for in-flight requests to complete...");
    for handle in handles {
        let _ = handle.await;
    }

    stats_handle.abort();
    pb.finish_and_clear();

    let elapsed = start.elapsed();
    let final_calls = total_calls.load(Ordering::Relaxed);
    let final_success = success_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);
    let final_duration = total_duration.load(Ordering::Relaxed);
    let final_retries = total_retries.load(Ordering::Relaxed);

    println!();
    println!("{}", "=".repeat(60));
    println!("FINAL STATS");
    println!("{}", "=".repeat(60));
    println!("Total calls:        {}", final_calls);
    println!(
        "Successful:         {} ({:.2}%)",
        final_success,
        if final_calls > 0 {
            (final_success as f64 / final_calls as f64) * 100.0
        } else {
            0.0
        }
    );
    println!(
        "Errors:             {} ({:.2}%)",
        final_errors,
        if final_calls > 0 {
            (final_errors as f64 / final_calls as f64) * 100.0
        } else {
            0.0
        }
    );
    println!();
    println!("Total time:         {:.2}s", elapsed.as_secs_f64());
    println!(
        "Throughput:         {:.0} calls/second",
        if elapsed.as_secs_f64() > 0.0 {
            final_calls as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        }
    );

    if final_calls > 0 {
        let avg_duration = final_duration as f64 / final_calls as f64;
        println!();
        println!("Avg Lambda Time:    {:.2}ms", avg_duration);
        println!("Total OCC Retries:  {}", final_retries);
    }

    println!();

    Ok(())
}
