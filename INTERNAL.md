# Internal Documentation

This document contains detailed reproduction steps for recreating the project structure. This is for internal use and not intended for customers following along with the talk.

## Chapter 00: Pre-Setup

This chapter sets up the base infrastructure that should be deployed **before the talk begins**.

**Setup time:** ~5 minutes (including ~1 min CDK deploy)

### What Gets Deployed

- Lambda function named `reinvent-dat401` with a simple greeter handler
- Database connection module using `pg` library (placeholder, no DSQL auth yet)

**Note:** The DSQL cluster will be created during Chapter 01.

### Reproduction Steps

These are the exact commands used to create the starter-kit:

### 1. Create Lambda Directory

```sh
# Create directory structure
mkdir -p starter-kit/lambda/src
cd starter-kit/lambda

# Initialize npm project
npm init -y

# Install dependencies
npm install pg @aws-sdk/dsql-signer

# Install dev dependencies
npm install -D @types/node @types/aws-lambda @types/pg typescript

# Create TypeScript config
npx tsc --init \
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
```

Add build scripts to `lambda/package.json`:

```json
{
  "scripts": {
    "build": "tsc",
    "clean": "rm -rf dist"
  }
}
```

### 2. Create CDK Directory

```sh
# Go to starter-kit root
cd ..

# Create and initialize CDK app
mkdir cdk
cd cdk
npx cdk init app --language typescript

# Install esbuild for bundling (avoids Docker requirement)
npm install --save-dev esbuild
```

### 3. Customize CDK Stack

Edit `lib/cdk-stack.ts` to create the Lambda function with name `reinvent-dat401`.

Edit `bin/cdk.ts` to rename the stack to `ReinventDat401Stack`.

See the actual files in this directory for the complete implementation.

### 4. Deploy

From the `starter-kit/cdk` directory:

```sh
# Bootstrap CDK (only needed once per account and region)
npx cdk bootstrap

# Deploy the stack (~1 minute)
npx cdk deploy
```

CloudFormation stack name: `ReinventDat401Stack`

### 5. Test the Lambda

```sh
aws lambda invoke \
  --function-name reinvent-dat401 \
  --cli-binary-format raw-in-base64-out \
  --payload '{"name":"reinvent"}' \
  /tmp/response.json

cat /tmp/response.json
# Expected: {"greeting":"hello reinvent"}
```

### File Structure

```
starter-kit/
├── lambda/
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── index.ts       # Lambda handler (greeter only)
│       └── db.ts          # Connection module (pg Pool, no DSQL auth)
└── cdk/
    ├── bin/
    │   └── cdk.ts    # CDK app entry
    ├── lib/
    │   └── cdk-stack.ts  # Stack definition (Lambda only)
    ├── cdk.json
    ├── package.json
    └── tsconfig.json
```

### Important Notes

- The `db.ts` file intentionally does NOT include DSQL authentication yet
- This will be added during Chapter 01 of the talk
- The Lambda function currently only returns a greeting, not database operations
