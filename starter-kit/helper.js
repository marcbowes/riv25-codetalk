#!/usr/bin/env node

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { readFileSync } = require('fs');
const { parseArgs } = require('util');
const { Client } = require('pg');
const { DsqlSigner } = require('@aws-sdk/dsql-signer');

const FUNCTION_NAME = 'reinvent-dat401';

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
    console.log('Results:');
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
      'test-chapter': { type: 'string' },
      'setup': { type: 'boolean', default: false },
      'iters': { type: 'string', default: '1000' },
      'threads': { type: 'string', default: '1' },
      'accounts': { type: 'string', default: '1000' },
      'uuids': { type: 'boolean', default: false }
    }
  });

  return {
    testChapter: values['test-chapter'] ? parseInt(values['test-chapter']) : null,
    setup: values.setup,
    iters: parseInt(values.iters),
    threads: parseInt(values.threads),
    accounts: parseInt(values.accounts),
    useUuids: values.uuids
  };
}

async function getDsqlClient() {
  const clusterEndpoint = process.env.CLUSTER_ENDPOINT;
  if (!clusterEndpoint) {
    throw new Error('CLUSTER_ENDPOINT environment variable not set');
  }

  const region = process.env.AWS_REGION || 'us-west-2';
  const signer = new DsqlSigner({
    hostname: clusterEndpoint,
    region
  });

  const client = new Client({
    host: clusterEndpoint,
    port: 5432,
    database: 'postgres',
    user: 'admin',
    password: async () => await signer.getDbConnectAdminAuthToken(),
    ssl: true
  });

  await client.connect();
  return client;
}

async function setupSchema(numAccounts) {
  console.log('Setting up database schema...');
  const client = await getDsqlClient();

  try {
    // Create accounts table
    await client.query(`
      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY,
        balance NUMERIC NOT NULL
      )
    `);
    console.log('Created accounts table');

    // Clear existing data
    await client.query('DELETE FROM accounts');
    console.log('Cleared existing data');

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
        `INSERT INTO accounts (id, balance) VALUES ${values.join(', ')}`,
        params
      );
      console.log(`  Inserted accounts ${i + 1} to ${end}`);
    }

    console.log('Database setup complete!');
  } finally {
    await client.end();
  }
}

async function testChapter0() {
  console.log('Testing Chapter 0: Basic Lambda invocation');
  console.log();

  const payload = { name: 'reinvent' };
  console.log(`Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(payload)}'`);

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload)
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

    console.log('Response:', JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting === 'hello reinvent') {
      console.log('✅ Chapter 0 test PASSED');
    } else {
      console.log('❌ Chapter 0 test FAILED - unexpected response');
    }
  } catch (err) {
    console.error('❌ Chapter 0 test FAILED:', err.message);
  }
}

async function testChapter1() {
  console.log('Testing Chapter 1: DSQL connection');
  console.log();

  const payload = { name: 'reinvent' };
  console.log(`Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(payload)}'`);

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload)
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

    console.log('Response:', JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting?.includes('connected to DSQL successfully')) {
      console.log('✅ Chapter 1 test PASSED');
    } else if (responsePayload.errorMessage) {
      console.log('❌ Chapter 1 test FAILED with error:', responsePayload.errorMessage);
      if (responsePayload.errorMessage.includes('AccessDenied')) {
        console.log('   This is expected if IAM permissions are not yet added (Step 6)');
      }
    } else {
      console.log('❌ Chapter 1 test FAILED - unexpected response');
    }
  } catch (err) {
    console.error('❌ Chapter 1 test FAILED:', err.message);
  }
}

async function testChapter2() {
  console.log('Testing Chapter 2: DSQL connection with myapp role');
  console.log();

  const payload = { name: 'reinvent' };
  console.log(`Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(payload)}'`);

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload)
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

    console.log('Response:', JSON.stringify(responsePayload, null, 2));

    if (responsePayload.greeting?.includes('connected to DSQL successfully')) {
      console.log('✅ Chapter 2 test PASSED');
    } else if (responsePayload.errorMessage) {
      console.log('❌ Chapter 2 test FAILED with error:', responsePayload.errorMessage);
      if (responsePayload.errorMessage.includes('not authorized')) {
        console.log('   Make sure you authorized the Lambda role with: AWS IAM GRANT myapp TO \'<lambda-role-arn>\';');
      }
    } else {
      console.log('❌ Chapter 2 test FAILED - unexpected response');
    }
  } catch (err) {
    console.error('❌ Chapter 2 test FAILED:', err.message);
  }
}

async function testChapter3() {
  console.log('Testing Chapter 3: Money transfer');
  console.log();

  const payload = { payer_id: 1, payee_id: 2, amount: 10 };
  console.log(`Invoking Lambda function '${FUNCTION_NAME}' with payload '${JSON.stringify(payload)}'`);

  const client = new LambdaClient({});

  try {
    const command = new InvokeCommand({
      FunctionName: FUNCTION_NAME,
      Payload: JSON.stringify(payload)
    });

    const response = await client.send(command);
    const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

    console.log('Response:', JSON.stringify(responsePayload, null, 2));

    if (typeof responsePayload.balance === 'number') {
      console.log('✅ Chapter 3 test PASSED');
      console.log(`   Payer balance after transfer: ${responsePayload.balance}`);
    } else if (responsePayload.errorMessage) {
      console.log('❌ Chapter 3 test FAILED with error:', responsePayload.errorMessage);
      if (responsePayload.errorMessage.includes('Insufficient balance')) {
        console.log('   Account may have insufficient funds. Check account balances.');
      }
    } else {
      console.log('❌ Chapter 3 test FAILED - unexpected response');
    }
  } catch (err) {
    console.error('❌ Chapter 3 test FAILED:', err.message);
  }
}

function loadUuids(numAccounts) {
  try {
    const content = readFileSync('uuids.txt', 'utf-8');
    return content
      .split('\n')
      .map(line => line.trim())
      .filter(line => line.length > 0)
      .slice(0, numAccounts);
  } catch (err) {
    console.error('Failed to read uuids.txt:', err.message);
    process.exit(1);
  }
}

function randomFloat(min, max) {
  return Math.random() * (max - min) + min;
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function testChapter4() {
  console.log('Testing Chapter 4: Stress Test - 1M Invocations (1000 parallel x 1000 iterations)');
  console.log();

  const NUM_ACCOUNTS = 1000;
  const PARALLEL_CALLS = 1000;
  const ITERATIONS = 10;
  const TOTAL_CALLS = PARALLEL_CALLS * ITERATIONS;

  console.log(`Total invocations: ${TOTAL_CALLS.toLocaleString()}`);
  console.log(`Parallel requests per batch: ${PARALLEL_CALLS.toLocaleString()}`);
  console.log(`Number of batches: ${ITERATIONS.toLocaleString()}`);
  console.log();

  const client = new LambdaClient({});

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
    errorTypes: {}
  };

  const startTime = Date.now();

  for (let iteration = 0; iteration < ITERATIONS; iteration++) {
    const promises = [];

    // Create 1000 parallel invocations
    for (let i = 0; i < PARALLEL_CALLS; i++) {
      // Pick two random accounts
      const payerId = randomInt(1, NUM_ACCOUNTS);
      let payeeId = randomInt(1, NUM_ACCOUNTS);
      while (payeeId === payerId) {
        payeeId = randomInt(1, NUM_ACCOUNTS);
      }

      const payload = {
        payer_id: payerId,
        payee_id: payeeId,
        amount: 1
      };

      const command = new InvokeCommand({
        FunctionName: FUNCTION_NAME,
        Payload: JSON.stringify(payload)
      });

      const promise = client.send(command)
        .then(response => {
          const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

          if (responsePayload.error) {
            stats.errors++;
            const errorKey = responsePayload.errorCode
              ? `${responsePayload.error} (${responsePayload.errorCode})`
              : responsePayload.error;
            stats.errorTypes[errorKey] = (stats.errorTypes[errorKey] || 0) + 1;
          } else {
            stats.success++;
          }

          if (responsePayload.duration !== undefined) {
            stats.totalDuration += responsePayload.duration;
            stats.minDuration = Math.min(stats.minDuration, responsePayload.duration);
            stats.maxDuration = Math.max(stats.maxDuration, responsePayload.duration);
          }

          // Track retries
          if (responsePayload.retries !== undefined) {
            const retries = responsePayload.retries;
            stats.totalRetries += retries;
            stats.maxRetries = Math.max(stats.maxRetries, retries);
            if (retries > 0) {
              stats.transactionsWithRetries++;
            }
          }
        })
        .catch(err => {
          stats.errors++;
          const errorType = err.message || 'Unknown error';
          stats.errorTypes[errorType] = (stats.errorTypes[errorType] || 0) + 1;
        });

      promises.push(promise);
    }

    // Wait for all promises in this batch to complete
    await Promise.all(promises);

    // Print real-time progress (updates the same line)
    const completed = (iteration + 1) * PARALLEL_CALLS;
    const progress = ((iteration + 1) / ITERATIONS * 100).toFixed(0);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const rate = (completed / parseFloat(elapsed)).toFixed(0);
    const successRate = (stats.success + stats.errors > 0) ?
      ((stats.success / (stats.success + stats.errors)) * 100).toFixed(1) : '0.0';

    // Use \r to overwrite the same line
    process.stdout.write(`\r[${progress.padStart(3, ' ')}%] ${completed.toString().padStart(6, ' ')}/${TOTAL_CALLS.toString()} calls | ${rate.padStart(4, ' ')} calls/s | Success: ${successRate.padStart(5, ' ')}% | ${elapsed}s`);
  }

  // Print a newline after the loop completes
  console.log();

  const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  const callsPerSecond = (TOTAL_CALLS / totalElapsed).toFixed(0);

  // Print summary
  console.log();
  console.log('='.repeat(60));
  console.log('STATS');
  console.log('='.repeat(60));
  console.log(`Total calls:        ${TOTAL_CALLS.toLocaleString()}`);
  console.log(`Successful:         ${stats.success.toLocaleString()} (${(stats.success / TOTAL_CALLS * 100).toFixed(2)}%)`);
  console.log(`Errors:             ${stats.errors.toLocaleString()} (${(stats.errors / TOTAL_CALLS * 100).toFixed(2)}%)`);
  console.log();
  console.log(`Total time:         ${totalElapsed}s`);
  console.log(`Throughput:         ${callsPerSecond} calls/second`);
  console.log();

  if (stats.success > 0) {
    const avgDuration = stats.totalDuration / stats.success;
    console.log('Lambda Execution Times:');
    console.log(`  Min:                ${stats.minDuration.toFixed(2)}ms`);
    console.log(`  Max:                ${stats.maxDuration.toFixed(2)}ms`);
    console.log(`  Avg:                ${avgDuration.toFixed(2)}ms`);
    console.log();
  }

  // Display retry statistics
  if (stats.totalRetries > 0) {
    const avgRetries = stats.totalRetries / TOTAL_CALLS;
    const retryRate = (stats.transactionsWithRetries / TOTAL_CALLS * 100).toFixed(2);
    console.log('OCC Retry Statistics:');
    console.log(`  Total retries:      ${stats.totalRetries.toLocaleString()}`);
    console.log(`  Max retries:        ${stats.maxRetries}`);
    console.log(`  Avg retries/call:   ${avgRetries.toFixed(2)}`);
    console.log(`  Transactions with retries: ${stats.transactionsWithRetries.toLocaleString()} (${retryRate}%)`);
    console.log();
  }

  if (Object.keys(stats.errorTypes).length > 0) {
    console.log('Error Breakdown:');
    for (const [errorType, count] of Object.entries(stats.errorTypes).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${errorType}: ${count.toLocaleString()}`);
    }
    console.log();
  }

  console.log('✅ Chapter 4 test complete');
}

async function runInvocations(client, threadId, start, end, total, numAccounts, stats, uuids) {
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
      amount: amount.toString()
    };

    try {
      const command = new InvokeCommand({
        FunctionName: FUNCTION_NAME,
        Payload: JSON.stringify(payload)
      });

      const response = await client.send(command);
      const responsePayload = JSON.parse(Buffer.from(response.Payload).toString());

      let isError = false;
      let isInsufficientBalance = false;
      let latencyMs = 0;

      if (responsePayload.errorType || responsePayload.errorMessage) {
        isError = true;
        if (responsePayload.errorMessage?.includes('Insufficient balance')) {
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
        `[Thread ${threadId}: ${i}/${total}] Transferring ${amount} from account ${payerDisplay} to ${payeeDisplay} => ${JSON.stringify(responsePayload)}`
      );
    } catch (err) {
      stats.errorCount++;
      console.error(
        `[Thread ${threadId}: ${i}/${total}] Error transferring ${amount} from account ${payerDisplay} to ${payeeDisplay}: ${err.message}`
      );
    }
  }
}

async function runLoadTest(args) {
  console.log(`Running ${args.iters} invocations across ${args.threads} thread(s)`);

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
    const end = (t === args.threads)
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
        uuids
      )
    );

    start = end + 1;
  }

  // Handle Ctrl-C
  let interrupted = false;
  process.on('SIGINT', () => {
    if (!interrupted) {
      console.log();
      console.log('Interrupted! Waiting for current tasks to finish...');
      interrupted = true;
    }
  });

  try {
    await Promise.all(tasks);
  } catch (err) {
    console.error('Task failed:', err.message);
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
    } else {
      console.error(`Unknown test chapter: ${args.testChapter}`);
      process.exit(1);
    }
  } else if (args.setup) {
    await setupSchema(args.accounts);
  } else {
    await runLoadTest(args);
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
