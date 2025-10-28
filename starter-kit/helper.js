#!/usr/bin/env node

const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const { NodeHttpHandler } = require("@smithy/node-http-handler");
const { Worker } = require("worker_threads");
const { readFileSync } = require("fs");
const { parseArgs } = require("util");
const { Client } = require("pg");
const { DsqlSigner } = require("@aws-sdk/dsql-signer");
const path = require("path");

const FUNCTION_NAME = "reinvent-dat401";

class Stats {
  constructor() {
    this.successCount = 0;
    this.errorCount = 0;
    this.insufficientBalanceCount = 0;
    this.totalLatencyMs = 0;
  }

  print(completedCount) {
    console.log();
    console.log(`Completed ${completedCount} invocations`);
    console.log();
    console.log("Results:");
    console.log(`  Success: ${this.successCount}`);
    console.log(`  Errors:  ${this.errorCount}`);
    console.log(`  Insufficient balance: ${this.insufficientBalanceCount}`);
    if (this.successCount > 0) {
      const avgLatency = this.totalLatencyMs / this.successCount;
      console.log(`  Avg latency: ${avgLatency.toFixed(3)}ms`);
    }
  }
}

function parseArguments() {
  const { values } = parseArgs({
    options: {
      "test-chapter": { type: "string" },
      setup: { type: "boolean", default: false },
      "setup-ch06": { type: "boolean", default: false },
      iters: { type: "string", default: "1000" },
      threads: { type: "string", default: "1" },
      accounts: { type: "string", default: "1000" },
      uuids: { type: "boolean", default: false },
    },
  });

  return {
    testChapter: values["test-chapter"]
      ? parseInt(values["test-chapter"])
      : null,
    setup: values.setup,
    setupCh06: values["setup-ch06"],
    iters: parseInt(values.iters),
    threads: parseInt(values.threads),
    accounts: parseInt(values.accounts),
    useUuids: values.uuids,
  };
}

async function getDsqlClient() {
  const clusterEndpoint = process.env.CLUSTER_ENDPOINT;
  if (!clusterEndpoint) {
    throw new Error("CLUSTER_ENDPOINT environment variable not set");
  }

  const region = process.env.AWS_REGION || "us-west-2";
  const signer = new DsqlSigner({
    hostname: clusterEndpoint,
    region,
  });

  const client = new Client({
    host: clusterEndpoint,
    port: 5432,
    database: "postgres",
    user: "admin",
    password: async () => await signer.getDbConnectAdminAuthToken(),
    ssl: {
      rejectUnauthorized: true,
      // see https://marc-bowes.com/postgres-direct-tls.html for a weird case where firewalls can break connections
      maxVersion: "TLSv1.2",
    },
  });

  await client.connect();
  return client;
}

async function setupSchema(numAccounts) {
  console.log("Setting up database schema...");
  const client = await getDsqlClient();

  try {
    // Create accounts table
    await client.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY,
        balance NUMERIC NOT NULL
      )
    `);
    console.log("Created accounts table");

    // Create transactions table
    await client.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        payer_id INT,
        payee_id INT,
        amount INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log("Created transactions table");

    // Create indexes asynchronously (DSQL requirement)
    try {
      await client.query(
        "CREATE INDEX ASYNC idx_transactions_payer ON transactions(payer_id, created_at)",
      );
      console.log("Started creating idx_transactions_payer");
    } catch (err) {
      if (err.code === "42P07") {
        console.log("Index idx_transactions_payer already exists");
      } else {
        throw err;
      }
    }

    try {
      await client.query(
        "CREATE INDEX ASYNC idx_transactions_payee ON transactions(payee_id, created_at)",
      );
      console.log("Started creating idx_transactions_payee");
    } catch (err) {
      if (err.code === "42P07") {
        console.log("Index idx_transactions_payee already exists");
      } else {
        throw err;
      }
    }

    // Clear existing data
    await client.query("DELETE FROM accounts");
    await client.query("DELETE FROM transactions");
    console.log("Cleared existing data");

    // Insert accounts
    console.log(`Inserting ${numAccounts} accounts...`);
    const batchSize = 100;
    for (let i = 0; i < numAccounts; i += batchSize) {
      const end = Math.min(i + batchSize, numAccounts);
      const values = [];
      const params = [];
      for (let j = i; j < end; j++) {
        values.push(`($${(j - i) * 2 + 1}, $${(j - i) * 2 + 2})`);
        params.push(j + 1, 100);
      }
      await client.query(
        `INSERT INTO accounts (id, balance) VALUES ${values.join(", ")}`,
        params,
      );
      console.log(`  Inserted accounts ${i + 1} to ${end}`);
    }

    console.log("Database setup complete!");
  } finally {
    await client.end();
  }
}

async function testChapter0() {
  console.log("Testing Chapter 0: Basic Lambda invocation");
  console.log();

  const payload = { name: "reinvent" };
  console.log(
    `Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(
      payload,
    )}'`,
  );

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload),
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(
      Buffer.from(response.Payload).toString(),
    );

    console.log("Response:", JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting === "hello reinvent") {
      console.log("✅ Chapter 0 test PASSED");
    } else {
      console.log("❌ Chapter 0 test FAILED - unexpected response");
    }
  } catch (err) {
    console.error("❌ Chapter 0 test FAILED:", err.message);
  }
}

async function testChapter1() {
  console.log("Testing Chapter 1: DSQL connection");
  console.log();

  const payload = { name: "reinvent" };
  console.log(
    `Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(
      payload,
    )}'`,
  );

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload),
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(
      Buffer.from(response.Payload).toString(),
    );

    console.log("Response:", JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting?.includes("connected to DSQL successfully")) {
      console.log("✅ Chapter 1 test PASSED");
    } else if (responsePayload.errorMessage) {
      console.log(
        "❌ Chapter 1 test FAILED with error:",
        responsePayload.errorMessage,
      );
      if (responsePayload.errorMessage.includes("AccessDenied")) {
        console.log(
          "   This is expected if IAM permissions are not yet added (Step 6)",
        );
      }
    } else {
      console.log("❌ Chapter 1 test FAILED - unexpected response");
    }
  } catch (err) {
    console.error("❌ Chapter 1 test FAILED:", err.message);
  }
}

async function testChapter2() {
  console.log("Testing Chapter 2: DSQL connection with myapp role");
  console.log();

  const payload = { name: "reinvent" };
  console.log(
    `Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(
      payload,
    )}'`,
  );

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload),
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(
      Buffer.from(response.Payload).toString(),
    );

    console.log("Response:", JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting?.includes("connected to DSQL successfully")) {
      console.log("✅ Chapter 2 test PASSED");
    } else if (responsePayload.errorMessage) {
      console.log(
        "❌ Chapter 2 test FAILED with error:",
        responsePayload.errorMessage,
      );
      if (responsePayload.errorMessage.includes("not authorized")) {
        console.log(
          "   Make sure you authorized the Lambda role with: AWS IAM GRANT myapp TO '<lambda-role-arn>';",
        );
      }
    } else {
      console.log("❌ Chapter 2 test FAILED - unexpected response");
    }
  } catch (err) {
    console.error("❌ Chapter 2 test FAILED:", err.message);
  }
}

async function testChapter3() {
  console.log("Testing Chapter 3: Money transfer");
  console.log();

  const payload = { payer_id: 1, payee_id: 2, amount: 10 };
  console.log(
    `Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(
      payload,
    )}'`,
  );

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload),
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(
      Buffer.from(response.Payload).toString(),
    );

    console.log("Response:", JSON.stringify(responsePayload, null, 2));

    if (typeof responsePayload.balance === "number") {
      console.log("✅ Chapter 3 test PASSED");
      console.log(
        `   Payer balance after transfer: ${responsePayload.balance}`,
      );
    } else if (responsePayload.errorMessage) {
      console.log(
        "❌ Chapter 3 test FAILED with error:",
        responsePayload.errorMessage,
      );
      if (responsePayload.errorMessage.includes("Insufficient balance")) {
        console.log(
          "   Account may have insufficient funds. Check account balances.",
        );
      }
    } else {
      console.log("❌ Chapter 3 test FAILED - unexpected response");
    }
  } catch (err) {
    console.error("❌ Chapter 3 test FAILED:", err.message);
  }
}

function loadUuids(numAccounts) {
  try {
    const content = readFileSync("uuids.txt", "utf-8");
    return content
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .slice(0, numAccounts);
  } catch (err) {
    console.error("Failed to read uuids.txt:", err.message);
    process.exit(1);
  }
}

function randomFloat(min, max) {
  return Math.random() * (max - min) + min;
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function runStressTest(config) {
  const { parallelCalls, iterations, numAccounts, numWorkers = 1 } = config;
  const TOTAL_CALLS = parallelCalls * iterations;

  console.log(`Total invocations: ${TOTAL_CALLS.toLocaleString()}`);
  console.log(`Parallel requests per batch: ${parallelCalls.toLocaleString()}`);
  console.log(`Number of batches: ${iterations.toLocaleString()}`);
  if (numWorkers > 1) {
    console.log(`Number of workers: ${numWorkers}`);
  }
  console.log();

  if (numWorkers === 1) {
    // Single-threaded mode (original implementation)
    // Configure Lambda client with enough sockets for high concurrency
    const client = new LambdaClient({
      requestHandler: new NodeHttpHandler({
        connectionTimeout: 5000,
        socketTimeout: 30000,
        maxSockets: Math.max(parallelCalls, 1000),
      }),
    });

    // Track stats
    const stats = {
      success: 0,
      errors: 0,
      totalDuration: 0,
      minDuration: Infinity,
      maxDuration: 0,
      totalRetries: 0,
      maxRetries: 0,
      transactionsWithRetries: 0,
      errorTypes: {},
    };

    const startTime = Date.now();

    for (let iteration = 0; iteration < iterations; iteration++) {
      const promises = [];

      for (let i = 0; i < parallelCalls; i++) {
        const payerId = randomInt(1, numAccounts);
        let payeeId = randomInt(1, numAccounts);
        while (payeeId === payerId) {
          payeeId = randomInt(1, numAccounts);
        }

        const payload = {
          payer_id: payerId,
          payee_id: payeeId,
          amount: 1,
        };

        const command = new InvokeCommand({
          FunctionName: FUNCTION_NAME,
          Payload: JSON.stringify(payload),
        });

        const promise = client
          .send(command)
          .then((response) => {
            const responsePayload = JSON.parse(
              Buffer.from(response.Payload).toString(),
            );

            if (responsePayload.error) {
              stats.errors++;
              const errorKey = responsePayload.errorCode
                ? `${responsePayload.error} (${responsePayload.errorCode})`
                : responsePayload.error;
              stats.errorTypes[errorKey] =
                (stats.errorTypes[errorKey] || 0) + 1;
            } else {
              stats.success++;
            }

            if (responsePayload.duration !== undefined) {
              stats.totalDuration += responsePayload.duration;
              stats.minDuration = Math.min(
                stats.minDuration,
                responsePayload.duration,
              );
              stats.maxDuration = Math.max(
                stats.maxDuration,
                responsePayload.duration,
              );
            }

            if (responsePayload.retries !== undefined) {
              const retries = responsePayload.retries;
              stats.totalRetries += retries;
              stats.maxRetries = Math.max(stats.maxRetries, retries);
              if (retries > 0) {
                stats.transactionsWithRetries++;
              }
            }
          })
          .catch((err) => {
            stats.errors++;
            const errorType = err.message || "Unknown error";
            stats.errorTypes[errorType] =
              (stats.errorTypes[errorType] || 0) + 1;
          });

        promises.push(promise);
      }

      await Promise.all(promises);

      // Print real-time progress (updates the same line)
      const completed = (iteration + 1) * parallelCalls;
      const progress = (((iteration + 1) / iterations) * 100).toFixed(0);
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (completed / parseFloat(elapsed)).toFixed(0);
      const successRate =
        stats.success + stats.errors > 0
          ? ((stats.success / (stats.success + stats.errors)) * 100).toFixed(1)
          : "0.0";

      process.stdout.write(
        `\r[${progress.padStart(3, " ")}%] ${completed
          .toString()
          .padStart(
            7,
            " ",
          )}/${TOTAL_CALLS.toLocaleString()} calls | ${rate.padStart(
          5,
          " ",
        )} calls/s | Success: ${successRate.padStart(5, " ")}% | ${elapsed}s`,
      );
    }

    console.log();

    const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    const callsPerSecond = (TOTAL_CALLS / totalElapsed).toFixed(0);

    // Print summary
    console.log();
    console.log("=".repeat(60));
    console.log("STATS");
    console.log("=".repeat(60));
    console.log(`Total calls:        ${TOTAL_CALLS.toLocaleString()}`);
    console.log(
      `Successful:         ${stats.success.toLocaleString()} (${(
        (stats.success / TOTAL_CALLS) *
        100
      ).toFixed(2)}%)`,
    );
    console.log(
      `Errors:             ${stats.errors.toLocaleString()} (${(
        (stats.errors / TOTAL_CALLS) *
        100
      ).toFixed(2)}%)`,
    );
    console.log();
    console.log(`Total time:         ${totalElapsed}s`);
    console.log(`Throughput:         ${callsPerSecond} calls/second`);
    console.log();

    if (stats.success > 0) {
      const avgDuration = stats.totalDuration / stats.success;
      console.log("Lambda Execution Times:");
      console.log(`  Min:                ${stats.minDuration.toFixed(2)}ms`);
      console.log(`  Max:                ${stats.maxDuration.toFixed(2)}ms`);
      console.log(`  Avg:                ${avgDuration.toFixed(2)}ms`);
      console.log();
    }

    if (stats.totalRetries > 0) {
      const avgRetries = stats.totalRetries / TOTAL_CALLS;
      const retryRate = (
        (stats.transactionsWithRetries / TOTAL_CALLS) *
        100
      ).toFixed(2);
      console.log("OCC Retry Statistics:");
      console.log(
        `  Total retries:      ${stats.totalRetries.toLocaleString()}`,
      );
      console.log(`  Max retries:        ${stats.maxRetries}`);
      console.log(`  Avg retries/call:   ${avgRetries.toFixed(2)}`);
      console.log(
        `  Transactions with retries: ${stats.transactionsWithRetries.toLocaleString()} (${retryRate}%)`,
      );
      console.log();
    }

    if (Object.keys(stats.errorTypes).length > 0) {
      console.log("Error Breakdown:");
      for (const [errorType, count] of Object.entries(stats.errorTypes).sort(
        (a, b) => b[1] - a[1],
      )) {
        console.log(`  ${errorType}: ${count.toLocaleString()}`);
      }
      console.log();
    }
  } else {
    // Multi-worker mode with true parallelism
    // Divide parallel calls across workers, but each worker does ALL iterations
    const parallelCallsPerWorker = Math.ceil(parallelCalls / numWorkers);
    const startTime = Date.now();

    const workerPromises = [];
    const workerCompletedCounts = new Array(numWorkers).fill(0);

    console.log(`Starting ${numWorkers} workers...`);
    console.log(
      `Each worker handles ${parallelCallsPerWorker} parallel calls per iteration, for ${iterations} iterations`,
    );

    for (let workerId = 0; workerId < numWorkers; workerId++) {
      const workerPromise = new Promise((resolve, reject) => {
        const worker = new Worker(path.join(__dirname, "stress-worker.js"), {
          workerData: {
            workerId: workerId + 1,
            parallelCalls: parallelCallsPerWorker,
            iterations: iterations,
            numAccounts,
            functionName: FUNCTION_NAME,
          },
        });

        worker.on("message", (msg) => {
          if (msg.type === "progress") {
            workerCompletedCounts[workerId] = msg.completed;

            // Calculate total progress across all workers
            const totalCompleted = workerCompletedCounts.reduce(
              (a, b) => a + b,
              0,
            );
            const progress = ((totalCompleted / TOTAL_CALLS) * 100).toFixed(0);
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            const rate = (totalCompleted / parseFloat(elapsed)).toFixed(0);

            process.stdout.write(
              `\r[${progress.padStart(3, " ")}%] ${totalCompleted
                .toString()
                .padStart(
                  7,
                  " ",
                )}/${TOTAL_CALLS.toLocaleString()} calls | ${rate.padStart(
                5,
                " ",
              )} calls/s | ${elapsed}s`,
            );
          } else if (msg.type === "done") {
            resolve(msg.stats);
          } else if (msg.type === "error") {
            console.error(`Worker ${msg.workerId} error: ${msg.error}`);
            reject(new Error(msg.error));
          }
        });

        worker.on("error", (err) => {
          console.error(`Worker ${workerId + 1} error:`, err);
          reject(err);
        });
        worker.on("exit", (code) => {
          if (code !== 0) {
            console.error(`Worker ${workerId + 1} exited with code ${code}`);
            reject(
              new Error(
                `Worker ${workerId + 1} stopped with exit code ${code}`,
              ),
            );
          }
        });
      });

      workerPromises.push(workerPromise);
    }

    console.log(`Created ${workerPromises.length} worker(s)`);

    // Wait for all workers and aggregate stats
    const allStats = await Promise.all(workerPromises);

    console.log();

    const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(2);
    const callsPerSecond = (TOTAL_CALLS / totalElapsed).toFixed(0);

    // Aggregate stats from all workers
    const aggregatedStats = {
      success: 0,
      errors: 0,
      totalDuration: 0,
      minDuration: Infinity,
      maxDuration: 0,
      totalRetries: 0,
      maxRetries: 0,
      transactionsWithRetries: 0,
      errorTypes: {},
    };

    for (const stats of allStats) {
      aggregatedStats.success += stats.success;
      aggregatedStats.errors += stats.errors;
      aggregatedStats.totalDuration += stats.totalDuration;
      aggregatedStats.minDuration = Math.min(
        aggregatedStats.minDuration,
        stats.minDuration,
      );
      aggregatedStats.maxDuration = Math.max(
        aggregatedStats.maxDuration,
        stats.maxDuration,
      );
      aggregatedStats.totalRetries += stats.totalRetries;
      aggregatedStats.maxRetries = Math.max(
        aggregatedStats.maxRetries,
        stats.maxRetries,
      );
      aggregatedStats.transactionsWithRetries += stats.transactionsWithRetries;

      for (const [errorType, count] of Object.entries(stats.errorTypes)) {
        aggregatedStats.errorTypes[errorType] =
          (aggregatedStats.errorTypes[errorType] || 0) + count;
      }
    }

    // Print summary
    console.log();
    console.log("=".repeat(60));
    console.log("STATS");
    console.log("=".repeat(60));
    console.log(`Total calls:        ${TOTAL_CALLS.toLocaleString()}`);
    console.log(
      `Successful:         ${aggregatedStats.success.toLocaleString()} (${(
        (aggregatedStats.success / TOTAL_CALLS) *
        100
      ).toFixed(2)}%)`,
    );
    console.log(
      `Errors:             ${aggregatedStats.errors.toLocaleString()} (${(
        (aggregatedStats.errors / TOTAL_CALLS) *
        100
      ).toFixed(2)}%)`,
    );
    console.log();
    console.log(`Total time:         ${totalElapsed}s`);
    console.log(`Throughput:         ${callsPerSecond} calls/second`);
    console.log();

    if (aggregatedStats.success > 0) {
      const avgDuration =
        aggregatedStats.totalDuration / aggregatedStats.success;
      console.log("Lambda Execution Times:");
      console.log(
        `  Min:                ${aggregatedStats.minDuration.toFixed(2)}ms`,
      );
      console.log(
        `  Max:                ${aggregatedStats.maxDuration.toFixed(2)}ms`,
      );
      console.log(`  Avg:                ${avgDuration.toFixed(2)}ms`);
      console.log();
    }

    if (aggregatedStats.totalRetries > 0) {
      const avgRetries = aggregatedStats.totalRetries / TOTAL_CALLS;
      const retryRate = (
        (aggregatedStats.transactionsWithRetries / TOTAL_CALLS) *
        100
      ).toFixed(2);
      console.log("OCC Retry Statistics:");
      console.log(
        `  Total retries:      ${aggregatedStats.totalRetries.toLocaleString()}`,
      );
      console.log(`  Max retries:        ${aggregatedStats.maxRetries}`);
      console.log(`  Avg retries/call:   ${avgRetries.toFixed(2)}`);
      console.log(
        `  Transactions with retries: ${aggregatedStats.transactionsWithRetries.toLocaleString()} (${retryRate}%)`,
      );
      console.log();
    }

    if (Object.keys(aggregatedStats.errorTypes).length > 0) {
      console.log("Error Breakdown:");
      for (const [errorType, count] of Object.entries(
        aggregatedStats.errorTypes,
      ).sort((a, b) => b[1] - a[1])) {
        console.log(`  ${errorType}: ${count.toLocaleString()}`);
      }
      console.log();
    }
  }
}

async function testChapter4() {
  console.log(
    "Testing Chapter 4: Stress Test - 10K Invocations (1000 parallel x 10 iterations)",
  );
  console.log();

  await runStressTest({
    parallelCalls: 1000,
    iterations: 10,
    numAccounts: 1000,
  });

  console.log("✅ Chapter 4 test complete");
}

async function testChapter5() {
  console.log("Testing Chapter 5: Transaction history with UUID primary keys");
  console.log();

  const payload = { payer_id: 1, payee_id: 2, amount: 10 };
  console.log(
    `Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(
      payload,
    )}'`,
  );

  const lambdaClient = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload),
    });

    const response = await lambdaClient.send(command);
    const responsePayload = JSON.parse(
      Buffer.from(response.Payload).toString(),
    );

    console.log("Response:", JSON.stringify(responsePayload, null, 2));
    console.log();

    if (typeof responsePayload.balance === "number") {
      console.log("✅ Transfer successful");

      // Now check the transactions table using the composite indexes
      console.log("Checking transactions table...");
      const dsqlClient = await getDsqlClient();

      try {
        // Query transactions where account 1 was the payer (uses idx_transactions_payer)
        const payerResult = await dsqlClient.query(
          "SELECT id, payer_id, payee_id, amount, created_at FROM transactions WHERE payer_id = $1 ORDER BY created_at DESC LIMIT 5",
          [1],
        );

        // Query transactions where account 2 was the payee (uses idx_transactions_payee)
        const payeeResult = await dsqlClient.query(
          "SELECT id, payer_id, payee_id, amount, created_at FROM transactions WHERE payee_id = $1 ORDER BY created_at DESC LIMIT 5",
          [2],
        );

        // Merge and sort by created_at, limit to 5
        const allTransactions = [...payerResult.rows, ...payeeResult.rows]
          .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
          .slice(0, 5);

        console.log(
          `Found ${allTransactions.length} recent transactions for accounts 1 and 2:`,
        );
        allTransactions.forEach((row, idx) => {
          console.log(
            `  ${idx + 1}. ID: ${row.id}, Payer: ${row.payer_id}, Payee: ${
              row.payee_id
            }, Amount: ${row.amount}, Time: ${row.created_at}`,
          );
        });
        console.log();
        console.log("✅ Chapter 5 test PASSED");
      } finally {
        await dsqlClient.end();
      }
    } else if (responsePayload.error) {
      console.log(
        "❌ Chapter 5 test FAILED with error:",
        responsePayload.error,
      );
    } else {
      console.log("❌ Chapter 5 test FAILED - unexpected response");
    }
  } catch (err) {
    console.error("❌ Chapter 5 test FAILED:", err.message);
  }
}

async function setupChapter6() {
  console.log("Setting up Chapter 6: Creating 1M accounts");
  console.log();

  const TARGET_ACCOUNTS = 1000000;
  const client = await getDsqlClient();

  try {
    // Check current account count
    const countResult = await client.query("SELECT COUNT(*) FROM accounts");
    const currentCount = parseInt(countResult.rows[0].count);
    console.log(`Current account count: ${currentCount.toLocaleString()}`);

    if (currentCount < TARGET_ACCOUNTS) {
      const neededAccounts = TARGET_ACCOUNTS - currentCount;
      console.log(
        `Inserting ${neededAccounts.toLocaleString()} more accounts to reach ${TARGET_ACCOUNTS.toLocaleString()}...`,
      );

      // Insert in batches of 1,000 using generate_series, with 128 worker threads
      const batchSize = 1000;
      const numWorkers = 128;
      const accountsPerWorker = Math.ceil(neededAccounts / numWorkers);

      const workers = [];
      const workerPromises = [];

      for (let workerId = 0; workerId < numWorkers; workerId++) {
        const workerStartId = currentCount + workerId * accountsPerWorker + 1;
        const workerEndId = Math.min(
          currentCount + (workerId + 1) * accountsPerWorker,
          TARGET_ACCOUNTS,
        );

        // Skip if this worker has no work
        if (workerStartId > TARGET_ACCOUNTS) break;

        const workerPromise = new Promise((resolve, reject) => {
          const worker = new Worker(path.join(__dirname, "insert-worker.js"), {
            workerData: {
              workerId: workerId + 1,
              startId: workerStartId,
              endId: workerEndId,
              batchSize: batchSize,
            },
          });

          worker.on("message", (msg) => {
            if (msg.type === "progress") {
              console.log(
                `  [Worker ${
                  msg.workerId
                }] Inserted accounts ${msg.startId.toLocaleString()} to ${msg.endId.toLocaleString()}`,
              );
            } else if (msg.type === "done") {
              resolve();
            } else if (msg.type === "error") {
              reject(new Error(msg.error));
            }
          });

          worker.on("error", reject);
          worker.on("exit", (code) => {
            if (code !== 0) {
              reject(
                new Error(
                  `Worker ${workerId + 1} stopped with exit code ${code}`,
                ),
              );
            }
          });

          workers.push(worker);
        });

        workerPromises.push(workerPromise);
      }

      await Promise.all(workerPromises);

      console.log("Account insertion complete!");
      console.log();
    } else {
      console.log("Already have sufficient accounts");
      console.log();
    }
  } finally {
    await client.end();
  }

  console.log("✅ Chapter 6 setup complete");
}

async function testChapter6() {
  console.log(
    "Testing Chapter 6: Extreme Stress Test - 1M Invocations (10,000 parallel x 100 iterations)",
  );
  console.log();

  const TARGET_ACCOUNTS = 1000000;

  await runStressTest({
    parallelCalls: 10000,
    iterations: 100,
    numAccounts: TARGET_ACCOUNTS,
    numWorkers: 50,
  });

  console.log("✅ Chapter 6 test complete");
}

async function runInvocations(
  client,
  threadId,
  start,
  end,
  total,
  numAccounts,
  stats,
  uuids,
) {
  for (let i = start; i <= end; i++) {
    let payerId, payeeId, amount, payerDisplay, payeeDisplay;

    if (uuids.length === 0) {
      // Integer ID mode
      payerId = randomInt(1, numAccounts);
      payeeId = randomInt(1, numAccounts);
      while (payerId === payeeId) {
        payeeId = randomInt(1, numAccounts);
      }
      amount = Math.round(randomFloat(0.01, 10.0) * 100) / 100;
      payerDisplay = payerId.toString();
      payeeDisplay = payeeId.toString();
    } else {
      // UUID mode
      const payerIdx = randomInt(0, uuids.length - 1);
      let payeeIdx = randomInt(0, uuids.length - 1);
      while (payerIdx === payeeIdx) {
        payeeIdx = randomInt(0, uuids.length - 1);
      }
      payerId = uuids[payerIdx];
      payeeId = uuids[payeeIdx];
      amount = Math.round(randomFloat(0.01, 10.0) * 100) / 100;
      payerDisplay = payerId;
      payeeDisplay = payeeId;
    }

    const payload = {
      payer_id: payerId,
      payee_id: payeeId,
      amount: amount.toString(),
    };

    try {
      const command = new InvokeCommand({
        FunctionName: FUNCTION_NAME,
        Payload: JSON.stringify(payload),
      });

      const response = await client.send(command);
      const responsePayload = JSON.parse(
        Buffer.from(response.Payload).toString(),
      );

      let isError = false;
      let isInsufficientBalance = false;
      let latencyMs = 0;

      if (responsePayload.errorType || responsePayload.errorMessage) {
        isError = true;
        if (responsePayload.errorMessage?.includes("Insufficient balance")) {
          isInsufficientBalance = true;
        }
      } else if (responsePayload.transaction_time) {
        // Extract latency from "16.955ms" format
        const match = responsePayload.transaction_time.match(/^([\d.]+)ms$/);
        if (match) {
          latencyMs = parseFloat(match[1]);
        }
      }

      if (isError) {
        if (isInsufficientBalance) {
          stats.insufficientBalanceCount++;
        } else {
          stats.errorCount++;
        }
      } else {
        stats.successCount++;
        stats.totalLatencyMs += latencyMs;
      }

      console.log(
        `[Thread ${threadId}: ${i}/${total}] Transferring ${amount} from account ${payerDisplay} to ${payeeDisplay} => ${JSON.stringify(
          responsePayload,
        )}`,
      );
    } catch (err) {
      stats.errorCount++;
      console.error(
        `[Thread ${threadId}: ${i}/${total}] Error transferring ${amount} from account ${payerDisplay} to ${payeeDisplay}: ${err.message}`,
      );
    }
  }
}

async function runLoadTest(args) {
  console.log(
    `Running ${args.iters} invocations across ${args.threads} thread(s)`,
  );

  // Load UUIDs if --uuids flag is set
  const uuids = args.useUuids ? loadUuids(args.accounts) : [];
  if (uuids.length > 0) {
    console.log(`Using ${uuids.length} UUIDs from uuids.txt`);
  }

  const client = new LambdaClient({});
  const stats = new Stats();

  // Calculate iterations per thread
  const itersPerThread = Math.floor(args.iters / args.threads);
  const remainder = args.iters % args.threads;

  const tasks = [];
  let start = 1;

  for (let t = 1; t <= args.threads; t++) {
    const end =
      t === args.threads
        ? start + itersPerThread - 1 + remainder
        : start + itersPerThread - 1;

    tasks.push(
      runInvocations(
        client,
        t,
        start,
        end,
        args.iters,
        args.accounts,
        stats,
        uuids,
      ),
    );

    start = end + 1;
  }

  // Handle Ctrl-C
  let interrupted = false;
  process.on("SIGINT", () => {
    if (!interrupted) {
      console.log();
      console.log("Interrupted! Waiting for current tasks to finish...");
      interrupted = true;
    }
  });

  try {
    await Promise.all(tasks);
  } catch (err) {
    console.error("Task failed:", err.message);
  }

  stats.print(args.threads);
}

async function main() {
  const args = parseArguments();

  if (args.testChapter !== null) {
    if (args.testChapter === 0) {
      await testChapter0();
    } else if (args.testChapter === 1) {
      await testChapter1();
    } else if (args.testChapter === 2) {
      await testChapter2();
    } else if (args.testChapter === 3) {
      await testChapter3();
    } else if (args.testChapter === 4) {
      await testChapter4();
    } else if (args.testChapter === 5) {
      await testChapter5();
    } else if (args.testChapter === 6) {
      await testChapter6();
    } else {
      console.error(`Unknown test chapter: ${args.testChapter}`);
      process.exit(1);
    }
  } else if (args.setup) {
    await setupSchema(args.accounts);
  } else if (args.setupCh06) {
    await setupChapter6();
  } else {
    await runLoadTest(args);
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
