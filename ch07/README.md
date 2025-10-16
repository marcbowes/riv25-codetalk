# Chapter 07: Query Optimization with Aurora DSQL

This chapter demonstrates query optimization techniques with Aurora DSQL, showing how proper indexing can dramatically improve query performance.

## Overview

The Lambda function provides four operations:
- `setup`: Creates the database table and initial suboptimal index
- `query`: Runs a query with the suboptimal index and shows performance
- `optimize`: Creates better indexes for the query
- `query_optimized`: Runs the same query with optimized indexes

## Database Schema

The `accounts7` table contains:
- `id` (INT) - Account identifier
- `account_type` (VARCHAR) - 'savings' or 'checking'
- `region_code` (CHAR(2)) - 'US', 'UK', 'DE', 'FR'
- `status` (VARCHAR) - 'active' or 'inactive'
- `created_at` (TIMESTAMP) - Account creation time
- `balance` (DECIMAL) - Account balance
- `metadata_key` (VARCHAR) - Metadata key like 'account_tier_1'
- `metadata_value` (TEXT) - Associated metadata value

## Optimization Demonstration

### Initial Setup
- Creates table with 5,000 rows
- Single index on `account_type` only
- Query filters on multiple columns but can only use the suboptimal index

### Query Pattern
```sql
SELECT metadata_key, metadata_value
FROM accounts7
WHERE account_type = 'savings'
  AND region_code = 'US'
  AND status = 'active'
  AND metadata_key = 'account_tier_1'
  AND created_at <= CURRENT_TIMESTAMP
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year'
```

### Optimization Steps
1. **Better composite index**: `(metadata_key, account_type)`
2. **Covering index**: `(metadata_key, account_type) INCLUDE (region_code, status, created_at, metadata_value)`

The covering index enables index-only scans, eliminating the need for table lookups.

## Deployment

```bash
# Set your cluster endpoint
export CLUSTER_ENDPOINT=your-cluster-id.dsql.us-west-2.on.aws

# Deploy the Lambda function
cd ch07/cdk
npm run cdk deploy -- -c clusterEndpoint=$CLUSTER_ENDPOINT
```

## Usage Examples

### 1. Setup Database
```bash
aws lambda invoke --function-name ch07 \
  --payload '{"operation": "setup"}' \
  response.json
```

### 2. Run Query with Suboptimal Index
```bash
aws lambda invoke --function-name ch07 \
  --payload '{"operation": "query", "metadata_key": "account_tier_1", "account_type": "savings", "region_code": "US", "status": "active"}' \
  response.json
```

### 3. Create Optimized Indexes
```bash
aws lambda invoke --function-name ch07 \
  --payload '{"operation": "optimize"}' \
  response.json
```

### 4. Run Query with Optimized Indexes
```bash
aws lambda invoke --function-name ch07 \
  --payload '{"operation": "query_optimized", "metadata_key": "account_tier_1", "account_type": "savings", "region_code": "US", "status": "active"}' \
  response.json
```

## Expected Performance Improvement

- **Before optimization**: ~60ms execution time with index scan + table lookups
- **After optimization**: ~3-12ms execution time with index-only scan
- **Performance gain**: 5-20x faster query execution

The response includes:
- `execution_time_ms`: Query execution time
- `rows_returned`: Number of rows returned
- `query_plan`: EXPLAIN ANALYZE output showing the execution plan

## Key Learning Points

1. **Index selectivity matters**: Leading with the most selective column (`metadata_key`) improves performance
2. **Covering indexes eliminate lookups**: Including all needed columns in the index enables index-only scans
3. **DSQL query planner**: Shows detailed execution plans with actual timing and row counts
4. **Composite indexes**: Order of columns in composite indexes significantly affects performance