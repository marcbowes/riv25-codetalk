# Amazon Aurora DSQL: A developer's perspective (DAT401)

In this live coding session, we'll show you how to work with Amazon Aurora DSQL from a developer's perspective. We'll develop a sample application to highlight some of the ways developing for Aurora DSQL is different than PostgreSQL. We'll cover authentication and connection management, optimistic concurrency transaction patterns, primary key selection, analyzing query performance, and best practices.

## Prerequisites

- Node.js 20+ and npm
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS credentials configured
- PostgreSQL client (`psql`) for database operations

## Project Structure

Each chapter (except ch02) contains two subdirectories:
- `cdk/` - AWS CDK infrastructure code for deploying the Lambda function
- `lambda/` - Lambda function source code

```
ch01/
├── cdk/          # CDK app for deploying ch01 Lambda
└── lambda/       # Lambda function code
    └── src/
        └── index.ts

ch03/
├── cdk/          # CDK app with DSQL permissions
└── lambda/       # Lambda function code
    └── src/
        └── index.ts

ch07/
├── cdk/          # CDK app for query optimization demo
├── lambda/       # Lambda function code
│   └── src/
│       └── index.ts
├── README.md     # Chapter-specific documentation
├── setup.sql     # Database setup script
└── PERFORMANCE_RESULTS.md  # Detailed performance analysis
```

## Quick Start

To deploy any chapter:

``` sh
# Install dependencies
$ npm install

# Deploy a chapter (e.g., ch01)
$ cd ch01/cdk
$ npm run cdk deploy

# For chapters with DSQL (ch03-ch07), provide cluster endpoint
$ export CLUSTER_ENDPOINT=your-cluster-id.dsql.us-east-1.on.aws
$ cd ch03/cdk
$ npm run cdk deploy -- -c clusterEndpoint=$CLUSTER_ENDPOINT

# Test the Lambda function
$ aws lambda invoke --function-name ch01 --payload '{"name": "reinvent"}' response.json
$ cat response.json
```

## Chapter 01

First, we're going to build a Lambda function.

Initialize the project:

``` sh
# Create project directory
$ mkdir lambda
$ cd lambda

# Initialize npm project
$ npm init -y

# Install dependencies
$ npm install @types/aws-lambda

# Install dev dependencies
$ npm install -D typescript @types/node

# Create TypeScript config with proper settings
$ npx tsc --init \
  --target ES2022 \
  --module commonjs \
  --lib ES2022 \
  --outDir ./dist \
  --rootDir ./src \
  --strict \
  --esModuleInterop \
  --skipLibCheck \
  --forceConsistentCasingInFileNames \
  --resolveJsonModule \
  --moduleResolution node

# Create source directory
$ mkdir src
```

Add build scripts to `package.json`:

``` json
{
  "scripts": {
    "build": "tsc",
    "clean": "rm -rf dist"
  }
}
```

Create a simple Lambda function that returns a greeting:

``` typescript
// src/index.ts
import { Handler } from 'aws-lambda';

interface Request {
  name: string;
}

interface Response {
  greeting: string;
}

export const handler: Handler<Request, Response> = async (event) => {
  const name = event.name;

  return {
    greeting: `hello ${name}`
  };
};
```

Build the Lambda function:

``` sh
$ npm run build
```

Now create a CDK project to deploy the Lambda:

``` sh
# Go back to parent directory
$ cd ..

# Create CDK directory
$ mkdir cdk
$ cd cdk

# Initialize CDK app
$ npx cdk init app --language typescript

# Install esbuild for local bundling (avoids Docker)
$ npm install --save-dev esbuild
```

Update `lib/cdk-stack.ts` to deploy your Lambda function:

``` typescript
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import { Construct } from 'constructs';
import * as path from 'path';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const lambdaFunction = new nodejs.NodejsFunction(this, 'DemoFunction', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'demo',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
    });

    new cdk.CfnOutput(this, 'FunctionName', {
      value: lambdaFunction.functionName,
      description: 'Lambda Function Name'
    });
  }
}
```

Update `bin/cdk.ts` to use a unique stack name:

``` typescript
new CdkStack(app, 'LambdaDemoStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
});
```

Deploy with CDK:

``` sh
# Bootstrap CDK (only needed once per account/region)
$ npx cdk bootstrap

# Deploy the stack
$ npx cdk deploy
```

Test the Lambda function:

``` sh
$ aws lambda invoke --function-name demo \
  --cli-binary-format raw-in-base64-out \
  --payload '{"name":"reinvent"}' \
  /tmp/response.json
$ cat /tmp/response.json
{"greeting":"hello reinvent"}
```

## Add Aurora DSQL Cluster

Now let's add a DSQL cluster to the same stack. Update `lib/cdk-stack.ts`:

``` typescript
import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as dsql from 'aws-cdk-lib/aws-dsql';  // ← Add this import
import { Construct } from 'constructs';
import * as path from 'path';

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create DSQL cluster
    const cluster = new dsql.CfnCluster(this, 'DsqlCluster', {
      deletionProtectionEnabled: false,
    });

    const lambdaFunction = new nodejs.NodejsFunction(this, 'DemoFunction', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../../lambda/src/index.ts'),
      handler: 'handler',
      functionName: 'demo',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,
    });

    new cdk.CfnOutput(this, 'FunctionName', {
      value: lambdaFunction.functionName,
      description: 'Lambda Function Name'
    });

    new cdk.CfnOutput(this, 'ClusterEndpoint', {
      value: `${cluster.attrIdentifier}.dsql.${this.region}.on.aws`,
      description: 'DSQL Cluster Endpoint'
    });

    new cdk.CfnOutput(this, 'ClusterId', {
      value: cluster.attrIdentifier,
      description: 'DSQL Cluster ID'
    });
  }
}
```

Deploy the updated stack:

``` sh
$ npx cdk deploy
```

The deployment will output the cluster endpoint. Save this for connecting later!

## Chapter 02

Create an Aurora DSQL cluster using CDK:

``` sh
$ mkdir ch02
$ cd ch02
$ cdk init app --language typescript
```

Update the stack to include an Aurora DSQL cluster with no deletion protection:

``` typescript
import * as cdk from 'aws-cdk-lib';
import * as dsql from 'aws-cdk-lib/aws-dsql';
import { Construct } from 'constructs';

export class Ch02Stack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const cluster = new dsql.CfnCluster(this, 'DsqlCluster', {
      deletionProtectionEnabled: false,
      tags: [{
        key: 'Name',
        value: 'ch02'
      }]
    });

    new cdk.CfnOutput(this, 'ClusterId', {
      value: cluster.attrIdentifier,
      description: 'Aurora DSQL Cluster ID'
    });
  }
}
```

Deploy the stack:

``` sh
$ npx cdk deploy --profile YOUR_AWS_PROFILE --region us-west-2
```

The deployment will output the cluster ID, which you'll need for connecting to the database.

## Chapter 03

We're going to connect to the cluster from our Lambda function. Let's first connect from the command line:

``` sh
export PGUSER=admin
export PGDATABASE=postgres
export PGHOST=YOUR_CLUSTER_ID.dsql.AWS_REGION.on.aws
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --host $PGHOST)

psql
```

Make a table called `accounts`:

``` sql
create table accounts (
  id int primary key,
  balance numeric
)

postgres=> \d accounts
                 Table "public.accounts"
 Column  |     Type      | Collation | Nullable | Default
---------+---------------+-----------+----------+---------
 id      | integer       |           | not null |
 balance | numeric(18,6) |           |          |
Indexes:
    "accounts_pkey" PRIMARY KEY, btree_index (id) INCLUDE (balance)

postgres=> insert into accounts (id, balance) values (1, 100);
INSERT 0 1
```

Create a new Lambda function for connecting to Aurora DSQL:

``` sh
$ mkdir -p ch03/src
$ cd ch03
```

Install dependencies:

``` sh
$ npm install @aws-sdk/dsql-signer postgres
$ npm install -D @types/node typescript
```

Create the connection code using `postgres-js` and the AWS DSQL Signer:

``` typescript
// src/index.ts
import { Handler } from 'aws-lambda';
import { DsqlSigner } from '@aws-sdk/dsql-signer';
import postgres from 'postgres';

interface Request {
  id: number;
}

interface Response {
  balance: string;
}

const CLUSTER_ENDPOINT = process.env.CLUSTER_ENDPOINT || 'YOUR_CLUSTER_ENDPOINT';
const REGION = process.env.REGION || 'us-west-2';
const USER = 'admin';

async function getPasswordToken(clusterEndpoint: string, user: string, region: string): Promise<string> {
  const signer = new DsqlSigner({
    hostname: clusterEndpoint,
    region,
  });

  if (user === 'admin') {
    return await signer.getDbConnectAdminAuthToken();
  } else {
    signer.username = user;
    return await signer.getDbConnectAuthToken();
  }
}

async function getConnection(clusterEndpoint: string, user: string, region: string) {
  const client = postgres({
    host: clusterEndpoint,
    user: user,
    password: async () => await getPasswordToken(clusterEndpoint, user, region),
    database: 'postgres',
    port: 5432,
    idle_timeout: 2,
    ssl: {
      rejectUnauthorized: true,
    }
  });

  return client;
}

export const handler: Handler<Request, Response> = async (event) => {
  let client;

  try {
    client = await getConnection(CLUSTER_ENDPOINT, USER, REGION);

    const rows = await client`
      SELECT balance FROM accounts WHERE id = ${event.id}
    `;

    if (rows.length === 0) {
      throw new Error(`Account ${event.id} not found`);
    }

    const balance = rows[0].balance;

    return {
      balance: balance.toString()
    };
  } catch (error) {
    console.error('Error:', error);
    throw error;
  } finally {
    await client?.end();
  }
};
```

Build and deploy the function:

``` sh
$ npm run build
$ npm run package
$ aws lambda update-function-code --function-name ch03 --zip-file fileb://function.zip
$ aws lambda invoke --function-name ch03 --payload '{"id": 1}' response.json
```

**Note:** After deploying Lambda functions that connect to DSQL, you'll need to add IAM permissions using the `add-dsql-permissions.sh` script:

``` sh
$ ./add-dsql-permissions.sh ch03
```

## Chapter 04

Now we'll implement a money transfer function with transactions. This chapter demonstrates connection reuse and transaction safety.

### Step 1: Reuse the connection

Instead of creating a new connection for each invocation, we'll create a cached connection that gets reused across Lambda invocations. This significantly improves performance by avoiding connection overhead.

Store the connection in a module-level variable:

``` typescript
// Connection reuse - create once and reuse across invocations
let cachedClient: Sql | null = null;

async function getConnection(clusterEndpoint: string, user: string, region: string): Promise<Sql> {
  if (cachedClient) {
    return cachedClient;
  }

  cachedClient = postgres({
    // ... connection config
  });

  return cachedClient;
}
```

### Step 2: Change the API types

Update the request to accept transfer parameters and the response to return transaction results:

``` typescript
interface Request {
  payer_id: number;
  payee_id: number;
  amount: string;
}

interface Response {
  payer_balance: string;
  transaction_time: string;
}
```

### Step 3: Implement the transaction

The transfer uses a PostgreSQL transaction with safety checks:

``` typescript
// Begin transaction and execute transfer
const result = await client.begin(async (sql) => {
  // Deduct from payer and check balance
  const payerRows = await sql`
    UPDATE accounts
    SET balance = balance - ${event.amount}
    WHERE id = ${event.payer_id}
    RETURNING balance
  `;

  if (payerRows.length === 0) {
    throw new Error('Payer account not found');
  }

  const payerBalance = parseFloat(payerRows[0].balance);
  if (payerBalance < 0) {
    throw new Error(`Insufficient balance: ${payerBalance}`);
  }

  // Add to payee
  const payeeResult = await sql`
    UPDATE accounts
    SET balance = balance + ${event.amount}
    WHERE id = ${event.payee_id}
  `;

  if (payeeResult.count !== 1) {
    throw new Error('Payee account not found');
  }

  return {
    payer_balance: payerBalance.toString()
  };
});
```

**Safety checks:**
- Check that exactly 1 row was updated for the payee (validates payee exists)
- Check that the payer's balance is not negative after the deduction
- If either check fails, throw an error and the transaction is automatically rolled back
- Use `Date.now()` to measure the transaction duration

### Step 4: Populate the database

Use the `setup.sql` script to create test data:

``` sql
DELETE FROM accounts;

INSERT INTO accounts (id, balance)
SELECT generate_series(1, 1000), 100;
```

This creates 1000 accounts (IDs 1-1000), each with a balance of 100.

Run the script:

``` sh
$ psql < ch04/setup.sql
```

Deploy and test:

``` sh
$ cd ch04
$ npm run build
$ npm run package
$ aws lambda update-function-code --function-name ch04 --zip-file fileb://function.zip
$ ./add-dsql-permissions.sh ch04
$ aws lambda invoke --function-name ch04 --payload '{"payer_id": 1, "payee_id": 2, "amount": "10"}' response.json
```

## Chapter 05

Chapter 05 extends ch04 by adding automatic retry logic for optimistic concurrency control (OCC) failures. When multiple transactions conflict, DSQL returns a serialization failure error, and the application should retry the transaction.

### Key Changes from Chapter 04

1. **Automatic OCC retry** - Transactions that fail with serialization errors (code `40001`) are automatically retried
2. **Attempts tracking** - The response includes an `attempts` field showing how many tries were needed
3. **Clean separation** - `executeTransfer` function contains transaction logic, retry loop handles OCC errors at commit time

The retry logic uses an infinite loop that only retries on serialization failures:

``` typescript
// Retry loop for OCC failures
let attempts = 0;
let payerBalance: string;

while (true) {
  attempts++;

  try {
    // Execute transaction with retry on OCC error
    payerBalance = await client.begin(async (sql) => {
      return await executeTransfer(sql, event);
    });

    // Transaction committed successfully
    break;
  } catch (error) {
    // Check if this is an OCC error (serialization failure)
    if (isOccError(error)) {
      // Retry on OCC error
      continue;
    }

    // For non-OCC errors, rethrow
    throw error;
  }
}
```

OCC detection checks the PostgreSQL error code:

``` typescript
function isOccError(error: any): boolean {
  // PostgreSQL serialization failure error code
  return error?.code === '40001';
}
```

Deploy and test:

``` sh
$ cd ch05
$ npm run build
$ npm run package
$ aws lambda update-function-code --function-name ch05 --zip-file fileb://function.zip
$ ./add-dsql-permissions.sh ch05
$ aws lambda invoke --function-name ch05 --payload '{"payer_id": 1, "payee_id": 2, "amount": "10"}' response.json
```

## Chapter 06

Chapter 06 extends ch05 by switching from integer account IDs to UUIDs, which will scale better.

### Key Changes from Chapter 05

1. **UUID primary keys** - Uses `UUID` type with `gen_random_uuid()` default
2. **String UUIDs** - Request struct uses string type for UUID fields
3. **New table** - Creates `accounts2` table with UUID IDs

### Database Setup

The `setup.sh` script creates the database and loads accounts with UUIDs:

``` sh
# Setup with default (1 thread)
$ ./ch06/setup.sh --endpoint YOUR_CLUSTER_ENDPOINT

# Setup with multiple threads for faster loading
$ ./ch06/setup.sh --endpoint YOUR_CLUSTER_ENDPOINT --threads 8
```

The script:
- Creates the `accounts2` table with UUID primary keys
- Runs 1000 transactions, each inserting 1000 accounts (1M accounts total)
- Saves all generated UUIDs to `uuids.txt` for load testing
- Supports `--threads` for parallel loading with proper cleanup on ctrl-c
- Uses separate worker files to prevent concurrent write corruption

Database schema:

``` sql
CREATE TABLE accounts2 (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    balance DECIMAL NOT NULL
);
```

### Code Changes

The Request interface now uses string for UUID fields:

``` typescript
interface Request {
  payer_id: string;  // UUID
  payee_id: string;  // UUID
  amount: string;
}
```

The UUIDs are passed as strings and postgres-js handles the conversion automatically.

Deploy and test:

``` sh
$ cd ch06
$ npm run build
$ npm run package
$ aws lambda update-function-code --function-name ch06 --zip-file fileb://function.zip
$ ./add-dsql-permissions.sh ch06
$ aws lambda invoke --function-name ch06 --payload '{"payer_id": "123e4567-e89b-12d3-a456-426614174000", "payee_id": "123e4567-e89b-12d3-a456-426614174001", "amount": "10"}' response.json
```

## Chapter 07

Chapter 07 demonstrates query optimization techniques with Aurora DSQL, showing how proper indexing can visibly improve query performance.

### Overview

This chapter provides a Lambda function with four operations to demonstrate progressive query optimization:

- `setup`: Creates the database table with 5,000 rows and initial suboptimal index
- `query`: Runs a complex query with the suboptimal index and measures performance
- `optimize`: Creates better indexes for the query
- `query_optimized`: Runs the same query with an INCLUDE index

### Database Schema

The `accounts7` table contains typical account data:

``` sql
CREATE TABLE accounts7 (
  id INT,
  account_type VARCHAR(20),     -- 'savings' or 'checking'
  region_code CHAR(2),          -- 'US', 'UK', 'DE', 'FR'
  status VARCHAR(10),           -- 'active' or 'inactive'
  created_at TIMESTAMP DEFAULT NOW(),
  balance DECIMAL(10,2),
  metadata_key VARCHAR(50),     -- 'account_tier_1', etc.
  metadata_value TEXT,
  PRIMARY KEY (id, created_at)
);
```

### Query Optimization Demonstration

The test query filters on multiple columns:

``` sql
SELECT metadata_key, metadata_value
FROM accounts7
WHERE account_type = 'savings'
  AND region_code = 'US'
  AND status = 'active'
  AND metadata_key = 'account_tier_1'
  AND created_at <= CURRENT_TIMESTAMP
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year'
```

**Performance Results:**
- **Before optimization**: ~456ms execution time using basic `account_type` index
- **After optimization**: ~257ms execution time using composite `metadata_key, account_type` index
- **Improvement**: 1.8x faster with 5.2x fewer rows scanned

### Optimization Steps

1. **Initial setup**: Single index on `account_type` only
   ``` sql
   CREATE INDEX ASYNC idx_accounts7_type ON accounts7(account_type);
   ```

2. **Better composite index**: Leading with most selective column
   ``` sql
   CREATE INDEX ASYNC idx_accounts7_metadata ON accounts7(metadata_key, account_type);
   ```

3. **Covering index**: Includes all query columns for index-only scans
   ``` sql
   CREATE INDEX ASYNC idx_accounts7_metadata_covering 
   ON accounts7(metadata_key, account_type) 
   INCLUDE (region_code, status, created_at, metadata_value);
   ```

### Deploy and Test

Deploy the Lambda function:

``` sh
$ export CLUSTER_ENDPOINT=your-cluster-id.dsql.us-east-1.on.aws
$ cd ch07/cdk
$ npm run cdk deploy -- -c clusterEndpoint=$CLUSTER_ENDPOINT
```

Test the optimization workflow:

``` sh
# 1. Setup database with 5,000 rows
$ aws lambda invoke --function-name ch07 \
  --payload '{"operation": "setup"}' response.json

# 2. Run query with suboptimal index
$ aws lambda invoke --function-name ch07 \
  --payload '{"operation": "query", "metadata_key": "account_tier_1", "account_type": "savings", "region_code": "US", "status": "active"}' \
  response.json

# 3. Create optimized indexes
$ aws lambda invoke --function-name ch07 \
  --payload '{"operation": "optimize"}' response.json

# 4. Run query with optimized indexes
$ aws lambda invoke --function-name ch07 \
  --payload '{"operation": "query_optimized", "metadata_key": "account_tier_1", "account_type": "savings", "region_code": "US", "status": "active"}' \
  response.json
```

### Key Learning Points

1. **Index selectivity**: Leading with the most selective column improves performance
2. **Covering indexes**: Including all needed columns enables index-only scans
3. **DSQL query planner**: EXPLAIN ANALYZE provides detailed execution plans with actual timing and row counts
4. **Composite indexes**: Column order significantly affects performance

The response includes execution time, rows returned, and detailed query plans showing the optimization impact.
