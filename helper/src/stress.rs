use crate::lambda::{self, tpcb, ClientPool};
use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

pub async fn run_stress_test(
    client_pool: &ClientPool,
    total_calls: usize,
    parallel_calls: usize,
    num_accounts: u32,
) -> Result<()> {
    println!("Total invocations: {}", total_calls);
    println!("Max parallel requests: {}", parallel_calls);
    println!();

    let client_pool = client_pool.clone();

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

                let pool = client_pool.clone();
                tasks.spawn(async move {
                    lambda::invoke::<_, tpcb::Response>(pool.get(), tpcb::Request {
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
    client_pool: &ClientPool,
    invocations_per_sec: u32,
    num_accounts: u32,
) -> Result<()> {
    println!("Sustained Load Generator (AIMD)");
    println!("========================================");
    println!("Target rate: {}/sec", invocations_per_sec);
    println!("Max in-flight: {}", invocations_per_sec * 50);
    println!("Account pool: {}", num_accounts);
    println!();
    println!("Press Ctrl-C to stop...");
    println!();

    let client_pool = client_pool.clone();
    let max_in_flight = (invocations_per_sec * 50) as usize;

    let running = Arc::new(AtomicBool::new(true));
    let total_calls = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let dispatch_error_count = Arc::new(AtomicUsize::new(0)); // Failed to call Lambda - triggers AIMD backoff
    let occ_error_count = Arc::new(AtomicUsize::new(0)); // OCC errors (40001)
    let total_duration = Arc::new(AtomicU64::new(0));
    let total_retries = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let concurrency_target = Arc::new(AtomicUsize::new(10)); // Start small

    // Channel for latency samples
    let (latency_tx, mut latency_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();

    // Ctrl-C handler
    let running_clone = running.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\nShutting down...");
        running_clone.store(false, Ordering::SeqCst);
    });

    let start = Instant::now();

    let m = MultiProgress::new();
    let pb = m.add(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg}").unwrap());

    // AIMD controller - adjusts concurrency based on dispatch errors only
    let aimd_running = running.clone();
    let aimd_success = success_count.clone();
    let aimd_errors = dispatch_error_count.clone(); // Only dispatch failures trigger AIMD backoff
    let aimd_display_errors = error_count.clone(); // All errors for display
    let aimd_occ_errors = occ_error_count.clone(); // OCC errors for display
    let aimd_target = concurrency_target.clone();
    let aimd_pb = pb.clone();
    let aimd_in_flight = in_flight.clone();

    let aimd_handle = tokio::spawn(async move {
        use hdrhistogram::Histogram;
        let mut hist: Histogram<u64> = Histogram::new(3).unwrap();
        let mut last_success = 0usize;
        let mut last_errors = 0usize;
        let mut last_good_concurrency = 10usize;
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        while aimd_running.load(Ordering::SeqCst) {
            interval.tick().await;

            // Drain all pending latency samples
            while let Ok(latency) = latency_rx.try_recv() {
                let _ = hist.record(latency);
            }

            let current_success = aimd_success.load(Ordering::Relaxed);
            let current_dispatch_errors = aimd_errors.load(Ordering::Relaxed);
            let display_errors = aimd_display_errors.load(Ordering::Relaxed);
            let occ_errors = aimd_occ_errors.load(Ordering::Relaxed);
            let success_this_sec = current_success - last_success;
            let dispatch_errors_this_sec = current_dispatch_errors - last_errors;
            let flying = aimd_in_flight.load(Ordering::Relaxed);
            let current_target = aimd_target.load(Ordering::Relaxed);

            // AIMD: back off only on dispatch failures (couldn't reach Lambda)
            // Any Lambda response (success or error) means we can increase
            let new_target = if dispatch_errors_this_sec == 0 && success_this_sec > 0 {
                last_good_concurrency = current_target;
                (current_target + 10).min(max_in_flight)
            } else if dispatch_errors_this_sec > 0 {
                last_good_concurrency.max(10)
            } else {
                current_target
            };
            aimd_target.store(new_target, Ordering::Relaxed);

            let p50 = hist.value_at_quantile(0.5);
            let p99 = hist.value_at_quantile(0.99);

            aimd_pb.set_message(format!(
                "{}/s | p50: {}ms p99: {}ms | Err: {} OCC: {} | Target: {} | Inflight: {}",
                success_this_sec, p50, p99, display_errors, occ_errors, new_target, flying
            ));

            last_success = current_success;
            last_errors = current_dispatch_errors;
        }
    });

    // Main loop - spawn tasks up to concurrency target, rate limited
    let mut tasks = JoinSet::new();
    let mut spawned_this_sec = 0usize;
    let mut last_reset = Instant::now();
    let target_rate = invocations_per_sec as usize;

    while running.load(Ordering::SeqCst) {
        // Reset rate limit counter every second
        if last_reset.elapsed() >= Duration::from_secs(1) {
            spawned_this_sec = 0;
            last_reset = Instant::now();
        }

        let target = concurrency_target.load(Ordering::Relaxed);
        let current = in_flight.load(Ordering::Relaxed);

        // Spawn tasks up to concurrency target AND rate limit
        let to_spawn = target.saturating_sub(current)
            .min(target_rate.saturating_sub(spawned_this_sec));

        for _ in 0..to_spawn {
            if !running.load(Ordering::SeqCst) { break; }

            let payer_id = rand::random::<u32>() % num_accounts + 1;
            let mut payee_id = rand::random::<u32>() % num_accounts + 1;
            while payee_id == payer_id {
                payee_id = rand::random::<u32>() % num_accounts + 1;
            }

            let pool = client_pool.clone();
            let total = total_calls.clone();
            let success = success_count.clone();
            let errors = error_count.clone();
            let dispatch_errors = dispatch_error_count.clone();
            let occ_errors = occ_error_count.clone();
            let duration_sum = total_duration.clone();
            let retries_sum = total_retries.clone();
            let flying = in_flight.clone();
            let lat_tx = latency_tx.clone();

            flying.fetch_add(1, Ordering::Relaxed);

            tasks.spawn(async move {
                let result = lambda::invoke::<_, tpcb::Response>(pool.get(), tpcb::Request {
                    payer_id, payee_id, amount: 1,
                }).await;

                flying.fetch_sub(1, Ordering::Relaxed);
                total.fetch_add(1, Ordering::Relaxed);

                match result {
                    Ok(response) => {
                        // Got a response from Lambda - this is good for AIMD
                        if let Some(ref err) = response.error {
                            errors.fetch_add(1, Ordering::Relaxed);
                            if response.error_code.as_deref() == Some("40001") {
                                occ_errors.fetch_add(1, Ordering::Relaxed);
                            } else {
                                tracing::warn!(error = %err, code = ?response.error_code, "Lambda error");
                            }
                        } else {
                            success.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(d) = response.duration {
                            duration_sum.fetch_add(d, Ordering::Relaxed);
                            let _ = lat_tx.send(d);
                        }
                        if let Some(r) = response.retries { retries_sum.fetch_add(r as u64, Ordering::Relaxed); }
                    }
                    Err(_) => {
                        // Failed to call Lambda - triggers AIMD backoff
                        errors.fetch_add(1, Ordering::Relaxed);
                        dispatch_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            spawned_this_sec += 1;
        }

        // Process completed tasks
        match tokio::time::timeout(Duration::from_millis(10), tasks.join_next()).await {
            Ok(Some(_)) => {}
            _ => {}
        }
    }

    // Drain remaining tasks
    pb.set_message("Waiting for in-flight requests to complete...");
    while tasks.join_next().await.is_some() {}

    aimd_handle.abort();
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
