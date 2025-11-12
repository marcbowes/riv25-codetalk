# Amazon Aurora DSQL: A developer's perspective (DAT401)

In this live coding session, we'll show you how to work with Amazon Aurora DSQL from a developer's perspective. We'll develop a sample application to highlight some of the ways developing for Aurora DSQL is different than PostgreSQL. We'll cover authentication and connection management, optimistic concurrency transaction patterns, primary key selection, analyzing query performance, and best practices.

**Total talk duration:** 45 minutes

## Prerequisites

- Node.js 20+ and npm
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS credentials configured
- PostgreSQL client (`psql`) for database operations

## Getting Started

Before the session, set up your working directory:

```sh
# Copy the starter project to your working directory
cp -r starter-kit my-dsql-app
cd my-dsql-app

# Install dependencies (includes helper.js dependencies and workspace packages)
npm install
```

Deploy the stack with DSQL cluster and Lambda function:

```sh
# Bootstrap CDK (only needed once per account and region)
cd cdk
npx cdk bootstrap

# Deploy the stack (~2 minutes - includes DSQL cluster creation)
npx cdk deploy
```

The deployment will output:
- `ClusterEndpoint`: Your DSQL cluster endpoint
- `LambdaRoleArn`: The Lambda execution role ARN (needed for database setup)

Set up the database:

```sh
# Set environment variables (use the ClusterEndpoint from deployment output)
export CLUSTER_ENDPOINT=<your-cluster-endpoint-from-output>
export PGHOST=$CLUSTER_ENDPOINT
export PGUSER=admin
export PGDATABASE=postgres
export PGSSLMODE=require

# Generate IAM auth token and connect with psql
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname $PGHOST)
psql
```

In the psql session, create the application role and authorize the Lambda:

```sql
-- Create the myapp role
CREATE ROLE myapp WITH LOGIN;

-- Authorize Lambda to use myapp role (replace with your LambdaRoleArn from deployment output)
AWS IAM GRANT myapp TO 'arn:aws:iam::123456789012:role/ReinventDat401Stack-ReinventDat401FunctionServi-XXXXXXXXXXXX';

-- Verify the authorization
SELECT * FROM sys.iam_pg_role_mappings;

-- Exit psql
\q
```

Test the Lambda function:

```sh
# From the chapter root
cd ..
node helper.js --test-chapter 0
# Expected: ✅ Chapter 0 test PASSED
#           Response shows "connected to DSQL successfully!"
```

Throughout this session, you'll modify this same project in place. The `ch01`, `ch02`, etc. directories in this repository are self-contained snapshots showing what your code should look like after completing each chapter.

## Chapter 01: Build a Money Transfer API

**Time:** ~10 minutes

In this chapter, we'll build a simple money transfer API that moves funds between accounts, demonstrating basic transaction handling.

### Step 1: Create Accounts Table

Connect to your DSQL cluster with psql (if not already connected) and create the accounts table:

```sql
CREATE TABLE accounts (
  id INT PRIMARY KEY,
  balance INT
);
```

Insert 1000 test accounts, each with a balance of 100:

```sql
INSERT INTO accounts (id, balance)
SELECT id, 100 FROM generate_series(1, 1000) AS id;
```

Verify the data:

```sql
SELECT COUNT(*) FROM accounts;
SELECT * FROM accounts LIMIT 5;
```

### Step 2: Update Lambda Request and Response Types

Edit `lambda/src/index.ts` and update the interfaces:

```typescript
interface Request {
  payer_id: number;
  payee_id: number;
  amount: number;
}

interface Response {
  balance: number;
}
```

### Step 3: Implement Transfer Logic with Transaction

Update the handler to perform the money transfer in a transaction:

```typescript
export const handler: Handler<Request, Response> = async (event) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    // Begin transaction
    await client.query("BEGIN");

    // Deduct from payer
    const deductResult = await client.query(
      "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
      [event.amount, event.payer_id],
    );

    if (deductResult.rows.length === 0) {
      throw new Error("Payer account not found");
    }

    const payerBalance = deductResult.rows[0].balance;

    if (payerBalance < 0) {
      throw new Error("Insufficient balance");
    }

    // Add to payee
    const addResult = await client.query(
      "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
      [event.amount, event.payee_id],
    );

    if (addResult.rowCount === 0) {
      throw new Error("Payee account not found");
    }

    // Commit transaction
    await client.query("COMMIT");

    return {
      balance: payerBalance,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};
```

This implementation:

- Uses explicit BEGIN/COMMIT/ROLLBACK for transaction control
- Deducts from payer and returns the new balance
- Checks for insufficient funds
- Adds to payee
- Rolls back on any error

### Step 4: Deploy

```sh
cd cdk
npx cdk deploy
```

### Step 5: Test the Transfer

In your psql session, grant permissions on the new table `accounts` to `myapp` role:

```sql
-- Grant read and write permissions
GRANT ALL ON public.accounts TO myapp;
```

Then, test a transfer from account 1 to account 2 using the helper:

```sh
node helper.js --test-chapter 1
# Expected: ✅ Chapter 1 test PASSED
#           Payer balance after transfer: 90
```

Or test directly with aws lambda invoke:

```sh
aws lambda invoke \
  --function-name reinvent-dat401 \
  --payload '{"payer_id":1,"payee_id":2,"amount":10}' \
  response.json && cat response.json && rm response.json
```

Expected output: `{"balance":80}` (account 1 now has 80 after another 10 transfer)

Verify in psql:

```sql
SELECT * FROM accounts WHERE id IN (1, 2);
```

You should see the updated balances reflecting the transfers.

**Reference:** See `ch01/` directory for the complete implementation.

## Chapter 02: Handling Optimistic Concurrency Control (OCC)

**Time:** ~10 minutes

In this chapter, we'll run a stress test to observe how Aurora DSQL handles high concurrency with optimistic concurrency control (OCC), then implement retry logic to handle transaction conflicts.

### Step 1: Add Telemetry to Track Performance

First, let's add telemetry fields to track execution time. Edit `lambda/src/index.ts` and update the Response interface:

```typescript
interface Response {
  balance?: number;
  error?: string;
  errorCode?: string;
  duration: number;
  retries?: number;
}
```

Update the handler to track execution time. First, import the error helper:

```typescript
import { getPool, isPgError } from "./db";

export const handler: Handler<Request, Response> = async (event) => {
  const startTime = Date.now();
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Deduct from payer
    const deductResult = await client.query(
      "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
      [event.amount, event.payer_id],
    );

    if (deductResult.rows.length === 0) {
      throw new Error("Payer account not found");
    }

    const payerBalance = deductResult.rows[0].balance;

    if (payerBalance < 0) {
      throw new Error("Insufficient balance");
    }

    // Add to payee
    const addResult = await client.query(
      "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
      [event.amount, event.payee_id],
    );

    if (addResult.rowCount === 0) {
      throw new Error("Payee account not found");
    }

    await client.query("COMMIT");
    client.release();

    const duration = Date.now() - startTime;
    return {
      balance: payerBalance,
      duration,
      retries: 0,
    };
  } catch (error: unknown) {
    const duration = Date.now() - startTime;

    try {
      await client.query("ROLLBACK");
      client.release();
    } catch (rollbackError) {
      // If rollback fails, connection is corrupted - destroy it
      try {
        client.release(true);
      } catch (releaseError) {
        // Ignore if release also fails
      }
      throw rollbackError;
    }

    if (!isPgError(error)) {
      throw error;
    }

    return {
      error: error.message,
      errorCode: error.code,
      duration,
      retries: 0,
    };
  }
};
```

### Step 2: Deploy

```sh
cd cdk
npx cdk deploy
```

**During deployment (~1 minute):** Explain that we're adding telemetry to measure transaction performance under load.

### Step 3: Run the Initial Stress Test

The helper script includes a stress test that makes 10,000 API calls (1,000 parallel requests × 10 batches), randomly transferring $1 between accounts:

```sh
node helper.js --test-chapter 2
```

You should see real-time progress and a summary like this:

```
Testing Chapter 2: Stress Test - 10K Invocations (1000 parallel x 10 iterations)

Total invocations: 10,000
Parallel requests per batch: 1,000
Number of batches: 10

[100%]  10000/10000 calls |  909 calls/s | Success:  93.4% | 11.0s

============================================================
STATS
============================================================
Total calls:        10,000
Successful:         9,344 (93.44%)
Errors:             656 (6.56%)

Total time:         11.01s
Throughput:         908 calls/second

Lambda Execution Times:
  Min:                9.00ms
  Max:                663.00ms
  Avg:                20.12ms

Error Breakdown:
  change conflicts with another transaction, please retry: (OC000) (40001): 656

✅ Chapter 2 test complete
```

**Key observations:**

- **Success rate (~93%)**: Most transactions complete successfully on first attempt
- **OCC conflicts (~7%)**: Under high concurrency, about 7% of transactions fail with error code `40001`
- **High throughput**: DSQL handles ~900 concurrent requests/second efficiently
- **Low latency**: Average execution time is ~20ms despite high concurrency

**What's happening:**
Aurora DSQL uses optimistic concurrency control (OCC). When multiple transactions try to update the same rows simultaneously, DSQL detects conflicts and rejects some transactions with PostgreSQL error code **`40001`** (serialization failure). This is expected behavior - your application **must** implement retry logic to handle these conflicts.

### Step 4: Implement Retry Logic

Edit `lambda/src/index.ts` to add automatic retry logic for OCC conflicts. First, import the error helpers from `db.ts`:

```typescript
import { getPool, isPgError, isOccError } from "./db";
```

Now extract the transfer logic into a separate function:

```typescript
async function performTransfer(
  client: any,
  payerId: number,
  payeeId: number,
  amount: number,
): Promise<number> {
  // Begin transaction
  await client.query("BEGIN");

  // Deduct from payer
  const deductResult = await client.query(
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
    [amount, payerId],
  );

  if (deductResult.rows.length === 0) {
    throw new Error("Payer account not found");
  }

  const payerBalance = deductResult.rows[0].balance;

  if (payerBalance < 0) {
    throw new Error("Insufficient balance");
  }

  // Add to payee
  const addResult = await client.query(
    "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
    [amount, payeeId],
  );

  if (addResult.rowCount === 0) {
    throw new Error("Payee account not found");
  }

  // Commit transaction
  await client.query("COMMIT");

  return payerBalance;
}

export const handler: Handler<Request, Response> = async (event) => {
  const startTime = Date.now();
  const pool = await getPool();
  const client = await pool.connect();

  let retryCount = 0;

  // Retry loop for OCC conflicts - retry indefinitely
  while (true) {
    try {
      const balance = await performTransfer(
        client,
        event.payer_id,
        event.payee_id,
        event.amount,
      );

      client.release();
      const duration = Date.now() - startTime;
      return {
        balance,
        duration,
        retries: retryCount,
      };
    } catch (error: unknown) {
      const duration = Date.now() - startTime;

      // Rollback on any error
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        // If rollback fails, connection is corrupted - destroy it
        try {
          client.release(true);
        } catch (releaseError) {
          // Ignore if release also fails
        }
        throw rollbackError;
      }

      // Re-throw if not a PostgreSQL error
      if (!isPgError(error)) {
        client.release();
        throw error;
      }

      // Check if it's an OCC error - if so, retry
      if (isOccError(error)) {
        retryCount++;
        continue;
      }

      // If not an OCC error, return the error
      client.release();
      return {
        error: error.message,
        errorCode: error.code,
        duration,
        retries: retryCount,
      };
    }
  }
};
```

Key changes:

- **Type-safe error handling**: Catch errors as `unknown`, use `isPgError()` to narrow to PostgreSQL errors, re-throw non-PostgreSQL errors
- **Robust rollback handling**: If `ROLLBACK` fails, destroy the connection with `client.release(true)` (double-catch to handle corrupted connections)
- **Error code tracking**: Return both `error` message and `errorCode` for better debugging
- **OCC detection**: Use `isOccError()` helper to check for PostgreSQL error code `40001` (serialization failure)
- **Infinite retry loop**: Continue retrying OCC conflicts until success (reuses same connection)
- **No backoff**: Keep it simple - retry immediately
- **Retry tracking**: Count and return the number of retries for observability

### Step 5: Deploy

```sh
cd cdk
npx cdk deploy
```

### Step 6: Run the Test Again

```sh
node helper.js --test-chapter 2
```

Now you should see **100% success rate** with retry statistics:

```
Testing Chapter 2: Stress Test - 10K Invocations (1000 parallel x 10 iterations)

Total invocations: 10,000
Parallel requests per batch: 1,000
Number of batches: 10

[100%]  10000/10000 calls |  935 calls/s | Success: 100.0% | 10.7s

============================================================
STATS
============================================================
Total calls:        10,000
Successful:         10,000 (100.00%)
Errors:             0 (0.00%)

Total time:         10.74s
Throughput:         931 calls/second

Lambda Execution Times:
  Min:                12.00ms
  Max:                555.00ms
  Avg:                19.29ms

OCC Retry Statistics:
  Total retries:      735
  Max retries:        4
  Avg retries/call:   0.07
  Transactions with retries: 642 (6.42%)

✅ Chapter 2 test complete
```

**Key differences after implementing retries:**

- ✅ **100% success rate** - All transactions complete successfully (up from ~93%)
- ✅ **0 errors** - OCC conflicts are automatically handled (down from ~7% errors)
- ✅ **735 total retries** - The Lambda automatically retried 735 times across all calls
- ✅ **6.42% retry rate** - About 642 transactions (6.42%) needed at least one retry
- ✅ **Max 4 retries** - Even under high contention, most conflicts resolve within a few retries

**Production best practices:**

- Always implement retry logic for error code `40001` in DSQL applications
- Consider adding exponential backoff for very high contention scenarios
- Monitor retry rates to detect hot spots in your data model
- Most transactions (93.58%) succeed on the first attempt

**Reference:** See `ch02/` directory for the complete implementation.

## Chapter 03: Primary Key Selection - UUID vs Integer

**Time:** ~5 minutes

In this chapter, we'll add transaction history tracking using UUID primary keys, demonstrating an important consideration for distributed databases like Aurora DSQL.

### Step 1: Create Transaction History Table

In your psql session, create a new table to record transaction history:

```sql
CREATE TABLE transactions (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  payer_id INT,
  payee_id INT,
  amount INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Why UUIDs?** In distributed databases like DSQL, UUID primary keys provide better distribution across the cluster compared to sequential integers. Sequential IDs can create hotspots where all inserts hit the same partition.

Grant permissions on the new table to the `myapp` role:

```sql
GRANT ALL ON public.transactions TO myapp;
```

### Step 2: Update Lambda to Record Transaction History

Edit `lambda/src/index.ts` and add an INSERT statement to record each transaction:

```typescript
async function performTransfer(
  client: any,
  payerId: number,
  payeeId: number,
  amount: number,
): Promise<number> {
  // Begin transaction
  await client.query("BEGIN");

  // Deduct from payer
  const deductResult = await client.query(
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
    [amount, payerId],
  );

  if (deductResult.rows.length === 0) {
    throw new Error("Payer account not found");
  }

  const payerBalance = deductResult.rows[0].balance;

  if (payerBalance < 0) {
    throw new Error("Insufficient balance");
  }

  // Add to payee
  const addResult = await client.query(
    "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
    [amount, payeeId],
  );

  if (addResult.rowCount === 0) {
    throw new Error("Payee account not found");
  }

  // Record transaction history
  await client.query(
    "INSERT INTO transactions (payer_id, payee_id, amount) VALUES ($1, $2, $3)",
    [payerId, payeeId, amount],
  );

  // Commit transaction
  await client.query("COMMIT");

  return payerBalance;
}
```

The INSERT statement is part of the same transaction, so it will be rolled back if the transfer fails.

### Step 3: Deploy

```sh
cd cdk
npx cdk deploy
```

### Step 4: Test Transaction History

```sh
node helper.js --test-chapter 3
```

Expected output:

```
Testing Chapter 3: Transaction history with UUID primary keys

Invoking Lambda function 'reinvent-dat401' with payload '{"payer_id":1,"payee_id":2,"amount":10}'
Response: {
  "balance": 80,
  "duration": 18,
  "retries": 0
}

✅ Transfer successful
Checking transactions table...
Found 5 recent transactions:
  1. ID: a1b2c3d4-e5f6-4789-a012-3456789abcde, Payer: 1, Payee: 2, Amount: 10, Time: 2025-01-15 10:23:45.123
  2. ID: f1e2d3c4-b5a6-4879-0123-456789abcdef, Payer: 3, Payee: 5, Amount: 10, Time: 2025-01-15 10:23:40.456
  ...

✅ Chapter 3 test PASSED
```

### Understanding Indexes and Primary Keys in DSQL

**Composite Indexes:**
The indexes we created are composite indexes that support efficient queries like:

- Finding all transactions for a specific payer within a date range
- Finding all transactions for a specific payee within a date range

These indexes support queries ordered by date because `created_at` is the second column in the index.

**Key differences between UUID and Integer primary keys:**

| Aspect           | Integer (Sequential)                    | UUID (Random)                                 |
| ---------------- | --------------------------------------- | --------------------------------------------- |
| **Distribution** | All writes hit same partition (hotspot) | Evenly distributed across partitions          |
| **Performance**  | Can bottleneck under high write load    | Scales linearly with write load               |
| **Sortability**  | Naturally ordered by insertion time     | Random order (use timestamp column if needed) |
| **Size**         | 4 bytes (INT) or 8 bytes (BIGINT)       | 16 bytes                                      |

**For DSQL:**

- ✅ Use **UUIDs** for high-write tables (like transaction logs)
- ✅ Use **Integers** for reference tables with low write rates (like our accounts table)
- ✅ The `gen_random_uuid()` function generates v4 UUIDs automatically

**Reference:** See `ch03/` directory for the complete implementation.

## Chapter 04: Analyzing Query Performance

**Time:** ~10 minutes

In this chapter, we'll learn how to analyze query performance in DSQL using `EXPLAIN ANALYZE`, understand how indexes are used, and optimize query execution.

### Step 1: Create Composite Indexes

First, let's create composite indexes to enable efficient lookups by payer and payee with date range queries. In your psql session, run:

```sql
CREATE INDEX ASYNC idx_transactions_payer ON transactions(payer_id, created_at);
CREATE INDEX ASYNC idx_transactions_payee ON transactions(payee_id, created_at);
```

**About `CREATE INDEX ASYNC`:** DSQL supports asynchronous index creation, which allows you to create indexes without blocking writes to the table. The indexes are built in the background and become available once complete. You can check the status with:

```sql
SELECT * FROM sys.index_creation_status;
```

Wait for both indexes to show `state = 'AVAILABLE'` before proceeding.

**Why composite indexes?** By including both `payer_id` (or `payee_id`) and `created_at` in the index, we enable efficient queries that filter by account ID and sort by date - a common pattern for transaction history.

### Step 2: Analyze a Query with EXPLAIN ANALYZE

Now let's analyze how DSQL executes queries using these composite indexes. In your psql session, run:

```sql
EXPLAIN ANALYZE SELECT id, payer_id, payee_id, amount, created_at
FROM transactions
WHERE payer_id = 1
ORDER BY created_at DESC
LIMIT 5;
```

You should see output similar to:

```
                                                              QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=104.92..104.93 rows=5 width=36) (actual time=0.647..0.649 rows=5 loops=1)
   ->  Sort  (cost=104.92..104.93 rows=6 width=36) (actual time=0.646..0.647 rows=5 loops=1)
         Sort Key: created_at DESC
         Sort Method: quicksort  Memory: 25kB
         ->  Full Scan (btree-table) on transactions  (cost=100.76..104.84 rows=6 width=36) (actual time=0.578..0.637 rows=6 loops=1)
               -> Storage Scan on transactions (cost=100.76..104.84 rows=6 width=36) (actual rows=6 loops=1)
                   Projections: id, payer_id, payee_id, amount, created_at
                   Filters: (payer_id = 1)
                   Rows Filtered: 0
                   -> B-Tree Scan on transactions (cost=100.76..104.84 rows=6 width=36) (actual rows=6 loops=1)
 Planning Time: 0.121 ms
 Execution Time: 0.679 ms
```

**Key observations:**

- **B-Tree Scan**: The query is scanning the primary key (UUID) index - this is a **full table scan**, not using our `idx_transactions_payer` composite index
- **Filters: (payer_id = 1)**: DSQL's pushdown compute engine (PCE) filters rows where `payer_id = 1` during the scan
- **Rows Filtered: 0**: All 6 rows in the table match the filter (because we only have test data for payer_id = 1)
- **Sort**: Since the table is ordered by UUID (random), the results must be sorted by `created_at DESC`
- **Execution Time**: Query completes in ~0.68ms because there are only 6 rows total

**Why isn't the composite index being used?**
With only 6 rows, DSQL's query optimizer determines that scanning the entire table is faster than using the composite index. This is a good decision by the optimizer - the overhead of index lookups would be more expensive than just scanning 6 rows.

### Step 3: Setup 1M Accounts for Stress Testing

To see the index being used, we need more data. Let's create 1 million accounts:

```sh
node helper.js --setup-ch04
```

This command uses 128 parallel worker threads to insert 1M accounts efficiently. You should see progress like:

```
Setting up Chapter 4: Creating 1M accounts

Current account count: 1,000
Inserting 999,000 more accounts to reach 1,000,000...
  [Worker 1] Inserted accounts 1,001 to 8,808
  [Worker 2] Inserted accounts 8,809 to 16,616
  ...
Account insertion complete!

✅ Chapter 4 setup complete
```

**During setup (~2-3 minutes):** This demonstrates how to efficiently bulk-load data into DSQL using parallel workers and batched inserts.

### Step 4: Run the Extreme Stress Test

Now let's run a 1M invocation stress test using 50 parallel workers:

```sh
node helper.js --test-chapter 4
```

This will perform:

- **1,000,000 total Lambda invocations**
- **10,000 parallel requests** (divided across 50 workers)
- **100 iterations** per worker
- **200 parallel calls per worker** (10,000 ÷ 50)

Expected output:

```
Testing Chapter 4: Extreme Stress Test - 1M Invocations (10,000 parallel x 100 iterations)

Total invocations: 1,000,000
Parallel requests per batch: 10,000
Number of batches: 100
Number of workers: 50

Starting 50 workers...
Each worker handles 200 parallel calls per iteration, for 100 iterations
Created 50 worker(s)
[100%] 1000000/1,000,000 calls |  4191 calls/s | 238.6s

============================================================
STATS
============================================================
Total calls:        1,000,000
Successful:         1,000,000 (100.00%)
Errors:             0 (0.00%)

Total time:         238.57s
Throughput:         4192 calls/second

Lambda Execution Times:
  Min:                14.00ms
  Max:                4283.00ms
  Avg:                409.45ms

OCC Retry Statistics:
  Total retries:      6,667
  Max retries:        2
  Avg retries/call:   0.01
  Transactions with retries: 6,584 (0.66%)

✅ Chapter 4 test complete
```

**Key observations:**

- ✅ **100% success rate** with 1M invocations
- ✅ **~4,200 calls/second** throughput using 50 workers
- ✅ **0.66% retry rate** - very low because 1M accounts reduces contention significantly
- ✅ **Max 2 retries** - with better data distribution across 1M accounts, OCC conflicts are rare
- ✅ **Average latency 409ms** - includes time for retries and high concurrency queuing

### Step 5: Verify Index Usage with EXPLAIN ANALYZE

Now with ~1M transactions in the table, let's run the same query again:

```sql
EXPLAIN ANALYZE SELECT id, payer_id, payee_id, amount, created_at
FROM transactions
WHERE payer_id = 1
ORDER BY created_at DESC
LIMIT 5;
```

You should now see the composite index being used:

```
                                                                       QUERY PLAN
---------------------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=100.54..208.56 rows=2 width=36) (actual time=1.578..1.597 rows=3 loops=1)
   ->  Index Scan Backward using idx_transactions_payer on transactions  (cost=100.54..208.56 rows=2 width=36) (actual time=1.577..1.595 rows=3 loops=1)
         Index Cond: (payer_id = 1)
         -> Storage Scan on idx_transactions_payer (cost=100.54..208.56 rows=2 width=36) (actual rows=3 loops=1)
             -> B-Tree Scan on idx_transactions_payer (cost=100.54..208.56 rows=2 width=36) (actual rows=3 loops=1)
                 Index Cond: (payer_id = 1)
         -> Storage Lookup on transactions (cost=100.54..208.56 rows=2 width=36) (actual rows=3 loops=1)
             Projections: id, payer_id, payee_id, amount, created_at
             -> B-Tree Lookup on transactions (cost=100.54..208.56 rows=2 width=36) (actual rows=3 loops=1)
 Planning Time: 0.118 ms
 Execution Time: 1.630 ms
```

**Key differences from Step 1:**

- ✅ **Index Scan Backward using idx_transactions_payer** - Now using the composite index!
- ✅ **No Sort operation** - The index already provides data in `(payer_id, created_at)` order
- ✅ **Index Cond: (payer_id = 1)** - The index efficiently filters to just this payer
- ✅ **Storage Lookup on transactions** - Only fetches the specific rows from the table after using the index
- ✅ **Planning Time: 0.118ms** - Much faster than before
- ✅ **Execution Time: 1.630ms** - Still very fast despite ~1M rows in the table
- ✅ **Found 3 rows** - There are actually 3 transactions where account 1 was the payer

**Why the index is used now:**
With ~1M rows in the table, the query optimizer determines that using the composite index is more efficient than scanning all rows. The index allows DSQL to:

1. Quickly locate all transactions for `payer_id = 1`
2. Return them already sorted by `created_at DESC`
3. Stop after finding the matching rows (LIMIT 5)

**Performance comparison:**

- **With 6 rows (Step 1)**: Full table scan, 0.679ms execution time
- **With ~1M rows (Step 4)**: Index scan, 1.630ms execution time

Even with 166,000x more data, the query is only ~2.4x slower thanks to the composite index! This demonstrates the power of proper indexing in distributed databases.

**Reference:** See `ch03/` directory for the complete implementation (Chapter 4 reuses Chapter 3's Lambda code and adds indexes for query analysis).
