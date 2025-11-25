# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a re:Invent talk repository (DAT401) demonstrating Amazon Aurora DSQL development patterns through a progressive tutorial. Each chapter builds a money transfer API, demonstrating connection management, optimistic concurrency control (OCC), primary key selection, and query performance analysis.

## Repository Structure

- **Chapter Directories (`ch01/`, `ch02/`, `ch03/`)**: Self-contained snapshots showing the complete state after each chapter. Each contains:
  - `cdk/` - AWS CDK infrastructure with DSQL cluster and Lambda function deployment
  - `lambda/` - TypeScript Lambda function source code

- **`starter-kit/`**: Base project template used as starting point for the talk

- **`helper/`**: Rust CLI tool providing test harness and stress testing capabilities

## Common Development Commands

### Building and Running the Helper CLI Tool

Build the Rust helper CLI:

```bash
cargo build --release
```

The compiled binary will be at `target/release/helper`.

### Testing Chapters

```bash
# Test a specific chapter (0, 1, 2, 3, or 4)
cargo run --release -- --test-chapter <N>

# Setup Chapter 4 (creates 1M accounts for stress testing)
cargo run --release -- --setup-ch04
```

Note: Using `cargo run --release --` automatically builds if needed and runs the binary with arguments after `--`.

### CDK Deployment

From within any chapter's `cdk/` directory:

```bash
# First time only (per account/region)
npx cdk bootstrap

# Deploy or update the stack
npx cdk deploy

# View the synthesized CloudFormation template
npx cdk synth
```

### Lambda Development

Each chapter's `lambda/` directory:

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# The CDK deployment automatically bundles and deploys using esbuild
```

### Database Operations

Connect to DSQL with psql:

```bash
# Set environment variables
export CLUSTER_ENDPOINT=<from-cdk-output>
export PGHOST=$CLUSTER_ENDPOINT
export PGUSER=admin
export PGDATABASE=postgres
export PGSSLMODE=require

# Generate admin auth token and connect
export PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token --hostname $PGHOST)
psql
```

## Key Architecture Patterns

### DSQL Connection Management

All Lambda functions use a singleton connection pool pattern with IAM authentication:

```typescript
// db.ts pattern used across all chapters
import { Pool } from "pg";
import { DsqlSigner } from "@aws-sdk/dsql-signer";

let pool: Pool | null = null;

export async function getPool(): Promise<Pool> {
  if (pool) return pool;

  const signer = new DsqlSigner({
    hostname: process.env.CLUSTER_ENDPOINT!,
    region: process.env.AWS_REGION!,
  });

  pool = new Pool({
    host: process.env.CLUSTER_ENDPOINT!,
    port: 5432,
    database: "postgres",
    user: "myapp",
    password: async () => await signer.getDbConnectAuthToken(),
    ssl: true,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  return pool;
}
```

**Important**: The connection pool is created once and reused across Lambda invocations. IAM auth tokens are automatically refreshed on each connection.

### Optimistic Concurrency Control (OCC) Retry Pattern

DSQL uses optimistic concurrency control. Applications must implement retry logic for PostgreSQL error code `40001` (serialization failure):

```typescript
// Helper functions in db.ts
export function isPgError(error: unknown): error is { code: string; message: string } {
  return typeof error === 'object' && error !== null && 'code' in error;
}

export function isOccError(error: unknown): boolean {
  return isPgError(error) && error.code === '40001';
}

// Retry loop pattern
while (true) {
  try {
    await performTransfer(client, ...);
    break; // Success
  } catch (error: unknown) {
    await client.query("ROLLBACK");

    if (!isPgError(error)) {
      throw error; // Re-throw non-PostgreSQL errors
    }

    if (isOccError(error)) {
      retryCount++;
      continue; // Retry on OCC conflict
    }

    // Return other PostgreSQL errors
    return { error: error.message, errorCode: error.code };
  }
}
```

**Important**: Always catch errors as `unknown`, use type guards to narrow to PostgreSQL errors, and implement infinite retry loops for OCC conflicts.

### Transaction Management

Explicit transaction control with robust error handling:

```typescript
try {
  await client.query("BEGIN");
  // ... perform queries ...
  await client.query("COMMIT");
  client.release();
} catch (error) {
  try {
    await client.query("ROLLBACK");
    client.release();
  } catch (rollbackError) {
    // If rollback fails, connection is corrupted - destroy it
    client.release(true); // true = destroy connection
    throw rollbackError;
  }
  throw error;
}
```

**Important**: If `ROLLBACK` fails, the connection is corrupted and must be destroyed with `client.release(true)`.

### Primary Key Selection for Distributed Databases

- **Use UUIDs** for high-write tables (e.g., transaction logs) to avoid hotspots:
  ```sql
  CREATE TABLE transactions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    -- other columns
  );
  ```

- **Use Integers** for reference tables with low write rates (e.g., accounts table):
  ```sql
  CREATE TABLE accounts (
    id INT PRIMARY KEY,
    balance INT
  );
  ```

**Rationale**: Sequential integer PKs create write hotspots in distributed databases. UUID PKs distribute writes evenly across partitions.

### Composite Indexes for Date Range Queries

Create composite indexes with the filter column first, then the sort column:

```sql
CREATE INDEX ASYNC idx_transactions_payer ON transactions(payer_id, created_at);
CREATE INDEX ASYNC idx_transactions_payee ON transactions(payee_id, created_at);
```

This enables efficient queries like:
```sql
SELECT * FROM transactions
WHERE payer_id = 1
ORDER BY created_at DESC
LIMIT 5;
```

**Note**: Use `CREATE INDEX ASYNC` for non-blocking index creation. Monitor with `SELECT * FROM sys.jobs;`.

### IAM Role Authorization

After creating a database role, authorize the Lambda IAM role to use it:

```sql
-- Create application role
CREATE ROLE myapp WITH LOGIN;

-- Grant permissions
GRANT ALL ON public.accounts TO myapp;
GRANT ALL ON public.transactions TO myapp;

-- Authorize Lambda IAM role to assume this database role
AWS IAM GRANT myapp TO 'arn:aws:iam::123456789012:role/Lambda-Role-Name';

-- Verify
SELECT * FROM sys.iam_pg_role_mappings;
```

### CDK Stack Pattern

Standard stack for each chapter:

```typescript
import * as dsql from "aws-cdk-lib/aws-dsql";
import * as nodejs from "aws-cdk-lib/aws-lambda-nodejs";

// Create DSQL cluster
const cluster = new dsql.CfnCluster(this, "DsqlCluster", {
  deletionProtectionEnabled: false,
});

// Lambda function with NodejsFunction construct (uses esbuild)
const lambdaFunction = new nodejs.NodejsFunction(this, "Function", {
  runtime: lambda.Runtime.NODEJS_20_X,
  entry: path.join(__dirname, "../../lambda/src/index.ts"),
  handler: "handler",
  functionName: "reinvent-dat401",
  timeout: cdk.Duration.seconds(30),
  memorySize: 512,
  environment: {
    CLUSTER_ENDPOINT: `${cluster.attrIdentifier}.dsql.${this.region}.on.aws`,
  },
});

// Grant DSQL access
lambdaFunction.addToRolePolicy(
  new iam.PolicyStatement({
    effect: iam.Effect.ALLOW,
    actions: ["dsql:DbConnect"],
    resources: [cluster.attrResourceArn],
  })
);
```

**Important**: Use `NodejsFunction` construct which bundles TypeScript with esbuild automatically, avoiding Docker dependency.

## Testing Architecture

The Rust helper tool provides:

- **Single transaction tests**: `--test-chapter <N>`
- **Stress tests**: 10K parallel requests (ch02), 1M parallel requests with 50 workers (ch04)
- **Setup operations**: `--setup-ch04` creates 1M test accounts

The stress tests use:
- 64 Tokio worker threads for parallelism
- AWS Lambda SDK for direct function invocation
- Indicatif for real-time progress bars
- Detailed statistics on success rates, OCC retries, and latency

## Development Workflow

When modifying a chapter:

1. Make changes to `lambda/src/index.ts` or `lambda/src/db.ts`
2. Deploy with `cd cdk && npx cdk deploy` (handles TypeScript compilation automatically)
3. If database schema changes, run SQL commands via psql
4. Test with `cargo run --release -- --test-chapter <N>`

**Note**: CDK automatically detects Lambda code changes and redeploys. No need to manually build TypeScript.

## Important Notes

- **Workspaces**: Root Cargo.toml defines a Rust workspace for the helper tool
- **Each chapter is independent**: Changes to one chapter don't affect others
- **TypeScript config**: `tsconfig.base.json` provides shared compiler options
- **No Docker required**: Using esbuild bundling instead of Docker containers for Lambda deployment
- **Lambda function name**: Always `reinvent-dat401` across all chapters for consistent testing
- **Stack name**: Always `ReinventDat401Stack` or `Dat401Stack` depending on chapter

## DSQL-Specific Considerations

1. **No auto-increment**: Use `gen_random_uuid()` or manually assign IDs
2. **OCC is mandatory**: Applications must handle error code 40001
3. **IAM authentication**: Use `@aws-sdk/dsql-signer` for password generation
4. **Async index creation**: Use `CREATE INDEX ASYNC` to avoid blocking writes
5. **Query analysis**: Use `EXPLAIN ANALYZE` to verify index usage
6. **System tables**: `sys.jobs` for background jobs, `sys.iam_pg_role_mappings` for auth mappings
