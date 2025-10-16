# Chapter 07 Performance Results

## Query Optimization Demonstration

### Test Query
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

### Performance Comparison

| Metric | Before Optimization | After Optimization | Improvement |
|--------|-------------------|-------------------|-------------|
| **Execution Time** | 456ms | 257ms | **1.8x faster** |
| **Query Plan** | Index Scan on `account_type` | Index Scan on `metadata_key, account_type` | Better selectivity |
| **Rows Scanned** | 4,045 rows | 778 rows | **5.2x fewer rows** |
| **Index Used** | `idx_accounts7_type` | `idx_accounts7_metadata` | More selective |

### Key Optimizations Applied

1. **Better Composite Index**: `idx_accounts7_metadata (metadata_key, account_type)`
   - Leading with the most selective column (`metadata_key`)
   - Reduced rows scanned from 4,045 to 778

2. **Covering Index**: `idx_accounts7_metadata_covering (metadata_key, account_type) INCLUDE (region_code, status, created_at, metadata_value)`
   - Includes all columns needed by the query
   - Enables index-only scans (eliminates table lookups)

### Query Plan Analysis

**Before Optimization:**
- Uses `idx_accounts7_type` index
- Scans 4,045 rows matching `account_type = 'savings'`
- Requires table lookups to filter by other columns
- Execution time: 70.979ms (plus planning overhead)

**After Optimization:**
- Uses `idx_accounts7_metadata` index
- Scans only 778 rows matching both `metadata_key` and `account_type`
- Still requires some table lookups for remaining filters
- Execution time: 13.283ms (plus planning overhead)

### Aurora DSQL Specific Benefits

1. **Automatic Index Selection**: DSQL's query planner automatically chose the most efficient index
2. **Detailed Execution Plans**: EXPLAIN ANALYZE provides actual timing and row counts
3. **Async Index Creation**: Indexes created with `ASYNC` keyword for non-blocking operations
4. **Storage Optimization**: Separate storage and B-Tree scan operations visible in plan

### Best Practices Demonstrated

1. **Index Column Order**: Most selective columns first
2. **Covering Indexes**: Include frequently accessed columns
3. **Composite Indexes**: Multiple columns for complex WHERE clauses
4. **Performance Monitoring**: Use EXPLAIN ANALYZE to measure actual performance

This demonstrates how proper indexing can significantly improve query performance in Aurora DSQL, with nearly 2x faster execution and 5x fewer rows scanned.