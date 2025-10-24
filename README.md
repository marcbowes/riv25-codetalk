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

Deploy the base Lambda function:

```sh
# Bootstrap CDK (only needed once per account and region)
cd cdk
npx cdk bootstrap

# Deploy the stack (~1 minute)
npx cdk deploy
```

Test the Lambda function:

```sh
# From the chapter root
cd ..
node helper.js --test-chapter 0
# Expected: ✅ Chapter 0 test PASSED
```

Throughout this session, you'll modify this same project in place. The `ch01`, `ch02`, etc. directories in this repository are self-contained snapshots showing what your code should look like after completing each chapter.

## Chapter 01: Create DSQL Cluster and Add IAM Authentication

**Time:** ~10 minutes (including 2 minutes for deployments)

In this chapter, we'll create an Aurora DSQL cluster, demonstrate the importance of IAM permissions, and connect using IAM authentication.

### Step 1: Add DSQL Cluster to CDK Stack

Edit `cdk/lib/dat401-stack.ts` and add the DSQL cluster:

```typescript
import * as dsql from 'aws-cdk-lib/aws-dsql';

// Inside the constructor, before the Lambda function:

// Create DSQL cluster
const cluster = new dsql.CfnCluster(this, 'DsqlCluster', {
  deletionProtectionEnabled: false,
  tags: [{
    key: 'Name',
    value: 'DAT401'
  }]
});

// Construct cluster endpoint
const clusterEndpoint = `${cluster.attrIdentifier}.dsql.${this.region}.on.aws`;
```

Update the Lambda function environment to include the cluster endpoint:

```typescript
const lambdaFunction = new nodejs.NodejsFunction(this, 'ReinventDat401Function', {
  // ... existing config ...
  environment: {
    CLUSTER_ENDPOINT: clusterEndpoint
  },
```

Add outputs at the end of the constructor:

```typescript
// Output the cluster endpoint for easy access
new cdk.CfnOutput(this, 'ClusterEndpoint', {
  value: clusterEndpoint,
  description: 'DSQL Cluster Endpoint'
});

// Output the Lambda execution role ARN
new cdk.CfnOutput(this, 'LambdaRoleArn', {
  value: lambdaFunction.role!.roleArn,
  description: 'Lambda Execution Role ARN'
});
```

**Note:** We're intentionally NOT adding IAM permissions yet to demonstrate what happens without them.

Deploy from the `cdk` directory:

```sh
npx cdk deploy
```

**During deployment (~1 minute):** Explain that we're creating the DSQL cluster. The deployment will output the `ClusterEndpoint` and `LambdaRoleArn`.

### Step 2: Connect with psql

Connect to your DSQL cluster using the `ClusterEndpoint` from the deployment output. **Keep this connection open** - you'll need it for Chapter 02.

```sh
# Set environment variables (use the ClusterEndpoint from deployment output)
export CLUSTER_ENDPOINT=<your-cluster-endpoint-from-output>
export PGHOST=$CLUSTER_ENDPOINT
export PGUSER=admin
export PGDATABASE=postgres
export PGSSLMODE=require

# Generate IAM auth token and connect
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname $PGHOST)
psql

# Once connected, try some commands:
postgres=> SELECT 1;
postgres=> \l  # List databases

# DO NOT QUIT - keep this session open for Chapter 02
```

### Step 3: Add DSQL Authentication to db.ts

Edit `lambda/src/db.ts` and replace the placeholder password with DSQL IAM authentication:

```typescript
import { DsqlSigner } from '@aws-sdk/dsql-signer';

export async function getPool(): Promise<Pool> {
  if (pool) {
    return pool;
  }

  const clusterEndpoint = process.env.CLUSTER_ENDPOINT!;
  const region = process.env.AWS_REGION!;

  const signer = new DsqlSigner({
    hostname: clusterEndpoint,
    region,
  });

  pool = new Pool({
    host: clusterEndpoint,
    port: 5432,
    database: 'postgres',
    user: 'admin',
    password: async () => await signer.getDbConnectAdminAuthToken(),
    ssl: true,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  return pool;
}
```

### Step 4: Test Database Connection

Edit `lambda/src/index.ts` to test the connection. Update the Response interface and handler:

```typescript
import { Handler } from 'aws-lambda';
import { getPool } from './db';

interface Request {
  name: string;
}

interface Response {
  greeting: string;
}

export const handler: Handler<Request, Response> = async (event) => {
  const pool = await getPool();

  const result = await pool.query('SELECT 1');

  return {
    greeting: `Hello ${event.name}, connected to DSQL successfully!`
  };
};
```

### Step 5: Deploy Lambda Changes

From the `cdk` directory:

```sh
npx cdk deploy
```

**During deployment (~1 minute):** Explain how DSQL uses IAM authentication instead of traditional database passwords to generate temporary tokens.

### Step 6: Test Lambda - Observe Permission Failure

```sh
node helper.js --test-chapter 1
# Expected: ❌ Chapter 1 test FAILED with AccessDenied error
```

**Expected error:** The Lambda will fail because it doesn't have permission to connect to DSQL. The helper will detect this is expected at Step 6.

**During error discussion (~1 min):** Explain that DSQL requires explicit IAM permissions, unlike traditional databases where you just need credentials.

### Step 7: Add IAM Permissions

Edit `cdk/lib/dat401-stack.ts` and add the import:

```typescript
import * as iam from 'aws-cdk-lib/aws-iam';
```

After the Lambda function definition, add:

```typescript
// Add DSQL DbConnectAdmin permission
lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['dsql:DbConnectAdmin'],
  resources: [cluster.attrResourceArn]
}));
```

### Step 8: Deploy with Permissions

```sh
npx cdk deploy
```

**During deployment (~1 minute):** Explain the IAM policy grants the Lambda's execution role permission to connect to this specific DSQL cluster.

### Step 9: Test - Success

```sh
node helper.js --test-chapter 1
# Expected: ✅ Chapter 1 test PASSED
```

**Reference:** See `ch01/` directory for the complete implementation.

## Chapter 02: Switch from Admin to Application Role

**Time:** ~5 minutes

In this chapter, we'll create a dedicated database role for our application instead of using the admin role. This demonstrates best practices for least-privilege access.

### Step 1: Create Application Role in PostgreSQL

In your psql session from Chapter 01 (which should still be open), create a new role for your application:

```sql
-- Create the myapp role
CREATE ROLE myapp WITH LOGIN;

-- Grant read and write permissions
GRANT ALL ON ALL TABLES IN SCHEMA public TO myapp;
```

You should see `myapp` listed in the roles.

### Step 2: Authorize Lambda to Use myapp Role

Copy the `LambdaRoleArn` from your CDK deployment output (from Chapter 01 Step 1). It looks like:
```
ReinventDat401Stack.LambdaRoleArn = arn:aws:iam::123456789012:role/ReinventDat401Stack-ReinventDat401FunctionServi-XXXXXXXXXXXX
```

If you lost it, you can retrieve it with:
```sh
aws cloudformation describe-stacks --stack-name ReinventDat401Stack \
  --query "Stacks[0].Outputs[?OutputKey=='LambdaRoleArn'].OutputValue" \
  --output text
```

Back in your psql session, authorize the Lambda role to connect as `myapp`:

```sql
-- Replace the ARN with your actual LambdaRoleArn from the CDK output
AWS IAM GRANT myapp TO 'arn:aws:iam::123456789012:role/ReinventDat401Stack-ReinventDat401FunctionServi-XXXXXXXXXXXX';

-- Verify the authorization
SELECT * FROM sys.iam_pg_role_mappings;
```

### Step 3: Update Lambda to Use myapp User

Edit `lambda/src/db.ts` and change the user from `admin` to `myapp`:

```typescript
pool = new Pool({
  host: clusterEndpoint,
  port: 5432,
  database: 'postgres',
  user: 'myapp',  // Changed from 'admin'
  password: async () => await signer.getDbConnectAuthToken(),  // Changed from getDbConnectAdminAuthToken()
  ssl: true,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});
```

### Step 4: Update IAM Permissions

Edit `cdk/lib/dat401-stack.ts` and update the IAM policy to use `DbConnect` instead of `DbConnectAdmin`:

```typescript
// Add DSQL DbConnect permission for myapp role
lambdaFunction.addToRolePolicy(new iam.PolicyStatement({
  effect: iam.Effect.ALLOW,
  actions: ['dsql:DbConnect'],  // Changed from DbConnectAdmin
  resources: [cluster.attrResourceArn]
}));
```

### Step 5: Deploy

```sh
cd cdk
npx cdk deploy
```

**During deployment (~1 minute):** Explain how this demonstrates least-privilege access - the Lambda can only connect as `myapp`, not as `admin`.

### Step 6: Test

```sh
node helper.js --test-chapter 2
# Expected: ✅ Chapter 2 test PASSED
```

The Lambda is now connecting with the `myapp` role instead of `admin`, following the principle of least privilege.

**Reference:** See `ch02/` directory for the complete implementation.

## Chapter 03: Build a Money Transfer API

**Time:** ~10 minutes

In this chapter, we'll build a simple money transfer API that moves funds between accounts, demonstrating basic transaction handling.

### Step 1: Create Accounts Table

In your psql session that's still open from Chapter 01, create the accounts table:

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
    await client.query('BEGIN');

    // Deduct from payer
    const deductResult = await client.query(
      'UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance',
      [event.amount, event.payer_id]
    );

    if (deductResult.rows.length === 0) {
      throw new Error('Payer account not found');
    }

    const payerBalance = deductResult.rows[0].balance;

    if (payerBalance < 0) {
      throw new Error('Insufficient balance');
    }

    // Add to payee
    const addResult = await client.query(
      'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
      [event.amount, event.payee_id]
    );

    if (addResult.rowCount === 0) {
      throw new Error('Payee account not found');
    }

    // Commit transaction
    await client.query('COMMIT');

    return {
      balance: payerBalance
    };
  } catch (error) {
    await client.query('ROLLBACK');
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

In your psql session from Chapter 01 (which should still be open), grant permissions on the new table `accounts` to `myapp` role:

```sql
-- Grant read and write permissions
GRANT ALL ON public.accounts TO myapp;
```

Then, test a transfer from account 1 to account 2 using the helper:

```sh
node helper.js --test-chapter 3
# Expected: ✅ Chapter 3 test PASSED
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

**Reference:** See `ch03/` directory for the complete implementation.

## Chapter 04: Handling Optimistic Concurrency Control (OCC)

**Time:** ~10 minutes

In this chapter, we'll run a stress test to observe how Aurora DSQL handles high concurrency with optimistic concurrency control (OCC), then implement retry logic to handle transaction conflicts.

### Step 1: Run the Initial Stress Test

The helper script includes a stress test that makes 10,000 API calls (1,000 parallel requests × 10 batches), randomly transferring $1 between accounts:

```sh
node helper.js --test-chapter 4
```

You should see real-time progress and a summary like this:

```
Testing Chapter 4: Stress Test - 1M Invocations (1000 parallel x 1000 iterations)

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

✅ Chapter 4 test complete
```

**Key observations:**
- **Success rate (~93%)**: Most transactions complete successfully on first attempt
- **OCC conflicts (~7%)**: Under high concurrency, about 7% of transactions fail with error code `40001`
- **High throughput**: DSQL handles ~900 concurrent requests/second efficiently
- **Low latency**: Average execution time is ~20ms despite high concurrency

**What's happening:**
Aurora DSQL uses optimistic concurrency control (OCC). When multiple transactions try to update the same rows simultaneously, DSQL detects conflicts and rejects some transactions with PostgreSQL error code **`40001`** (serialization failure). This is expected behavior - your application **must** implement retry logic to handle these conflicts.

### Step 2: Implement Retry Logic

Edit `lambda/src/index.ts` to add automatic retry logic for OCC conflicts:

```typescript
interface Response {
  balance?: number;
  error?: string;
  duration: number;
  retries?: number;  // Add retry tracking
}

function isOccError(error: any): boolean {
  // Check for PostgreSQL serialization failure (DSQL OCC error)
  return error?.code === '40001';
}

async function performTransfer(
  client: any,
  payerId: number,
  payeeId: number,
  amount: number
): Promise<number> {
  // Begin transaction
  await client.query('BEGIN');

  // Deduct from payer
  const deductResult = await client.query(
    'UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance',
    [amount, payerId]
  );

  if (deductResult.rows.length === 0) {
    throw new Error('Payer account not found');
  }

  const payerBalance = deductResult.rows[0].balance;

  if (payerBalance < 0) {
    throw new Error('Insufficient balance');
  }

  // Add to payee
  const addResult = await client.query(
    'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
    [amount, payeeId]
  );

  if (addResult.rowCount === 0) {
    throw new Error('Payee account not found');
  }

  // Commit transaction
  await client.query('COMMIT');

  return payerBalance;
}

export const handler: Handler<Request, Response> = async (event) => {
  const startTime = Date.now();
  const pool = await getPool();
  const client = await pool.connect();

  let retryCount = 0;

  try {
    // Retry loop for OCC conflicts - retry indefinitely
    while (true) {
      try {
        const balance = await performTransfer(
          client,
          event.payer_id,
          event.payee_id,
          event.amount
        );

        const duration = Date.now() - startTime;
        return {
          balance,
          duration,
          retries: retryCount
        };
      } catch (error) {
        // Rollback on any error
        try {
          await client.query('ROLLBACK');
        } catch (rollbackError) {
          // Ignore rollback errors
        }

        // Check if it's an OCC error - if so, retry
        if (isOccError(error)) {
          retryCount++;
          continue;
        }

        // If not an OCC error, return the error
        const duration = Date.now() - startTime;
        return {
          error: error instanceof Error ? error.message : 'Unknown error',
          duration,
          retries: retryCount
        };
      }
    }
  } finally {
    client.release();
  }
};
```

Key changes:
- **Error detection**: Check for PostgreSQL error code `40001` (serialization failure)
- **Infinite retry loop**: Continue retrying OCC conflicts until success
- **No backoff**: Keep it simple - retry immediately
- **Retry tracking**: Count and return the number of retries for observability

### Step 3: Deploy

```sh
cd cdk
npx cdk deploy
```

### Step 4: Run the Test Again

```sh
node helper.js --test-chapter 4
```

Now you should see **100% success rate** with retry statistics:

```
Testing Chapter 4: Stress Test - 1M Invocations (1000 parallel x 1000 iterations)

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

✅ Chapter 4 test complete
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

**Reference:** See `ch04/` directory for the complete implementation.

## Project Structure

Each chapter is a self-contained workspace with:
- `package.json` - Workspace configuration with helper.js dependencies
- `helper.js` - Testing and setup utility
- `cdk/` - AWS CDK infrastructure code for deploying the Lambda function
- `lambda/` - Lambda function source code

```
starter-kit/
├── package.json  # Workspace config with dependencies
├── helper.js     # Testing utility
├── cdk/          # Base CDK app with Lambda (no DSQL yet)
└── lambda/       # Lambda function code
    └── src/
        └── index.ts

ch01/
├── package.json  # Workspace config with dependencies
├── helper.js     # Testing utility
├── cdk/          # CDK app with DSQL cluster and IAM auth
└── lambda/       # Lambda function code
    └── src/
        └── index.ts

ch02/
├── package.json  # Workspace config with dependencies
├── helper.js     # Testing utility
├── cdk/          # CDK app with DbConnect permission for myapp
└── lambda/       # Lambda function code using myapp role
    └── src/
        └── index.ts

ch03/
├── package.json  # Workspace config with dependencies
├── helper.js     # Testing utility
├── cdk/          # CDK app unchanged from ch02
└── lambda/       # Lambda function with money transfer API
    └── src/
        └── index.ts

ch04/
├── package.json  # Workspace config with dependencies
├── helper.js     # Testing utility with stress test
├── cdk/          # CDK app unchanged from ch03
└── lambda/       # Lambda function with error tracking and duration reporting
    └── src/
        └── index.ts
```
