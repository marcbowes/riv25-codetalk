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

    if (responsePayload.message?.includes('connected to DSQL successfully')) {
      console.log('✅ Chapter 1 test PASSED');
    } else if (responsePayload.errorMessage) {
      console.log('❌ Chapter 1 test FAILED with error:', responsePayload.errorMessage);
      if (responsePayload.errorMessage.includes('AccessDenied')) {
        console.log('   This is expected if IAM permissions are not yet added (Step 5)');
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

    if (responsePayload.message?.includes('connected to DSQL successfully')) {
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
