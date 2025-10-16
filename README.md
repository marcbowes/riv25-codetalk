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
cp -r ch00 my-dsql-app
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
SELECT * FROM aws_dsql.list_iam_principal_database_role_authorizations();
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

## Project Structure

Each chapter is a self-contained workspace with:
- `package.json` - Workspace configuration with helper.js dependencies
- `helper.js` - Testing and setup utility
- `cdk/` - AWS CDK infrastructure code for deploying the Lambda function
- `lambda/` - Lambda function source code

```
ch00/
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
```
