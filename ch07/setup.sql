-- Chapter 07: Query Optimization Setup
-- This script demonstrates query optimization with Aurora DSQL

-- Create the table
CREATE TABLE accounts7 (
  id INT,
  account_type VARCHAR(20),
  region_code CHAR(2),
  status VARCHAR(10),
  created_at TIMESTAMP DEFAULT NOW(),
  balance DECIMAL(10,2),
  metadata_key VARCHAR(50),
  metadata_value TEXT,
  PRIMARY KEY (id, created_at)
);

-- Create a suboptimal index
CREATE INDEX ASYNC idx_accounts7_type ON accounts7(account_type);

-- Insert batch 1 (2500 rows)
INSERT INTO accounts7 (id, account_type, region_code, status, created_at, balance, metadata_key, metadata_value)
SELECT 
  generate_series,
  CASE WHEN random() < 0.8 THEN 'savings' ELSE 'checking' END,
  CASE floor(random() * 4)::int 
    WHEN 0 THEN 'US'
    WHEN 1 THEN 'UK'
    WHEN 2 THEN 'DE'
    ELSE 'FR'
  END,
  CASE WHEN random() < 0.9 THEN 'active' ELSE 'inactive' END,
  TIMESTAMP '2023-01-01 00:00:00' + (random() * INTERVAL '365 days'),
  (random() * 10000)::decimal(10,2),
  'account_tier_' || (floor(random() * 5) + 1)::text,
  'tier_' || (floor(random() * 5) + 1)::text || '_metadata'
FROM generate_series(1, 2500);

-- Insert batch 2 (2500 rows)
INSERT INTO accounts7 (id, account_type, region_code, status, created_at, balance, metadata_key, metadata_value)
SELECT 
  generate_series + 2500,
  CASE WHEN random() < 0.8 THEN 'savings' ELSE 'checking' END,
  CASE floor(random() * 4)::int 
    WHEN 0 THEN 'US'
    WHEN 1 THEN 'UK'
    WHEN 2 THEN 'DE'
    ELSE 'FR'
  END,
  CASE WHEN random() < 0.9 THEN 'active' ELSE 'inactive' END,
  TIMESTAMP '2023-01-01 00:00:00' + (random() * INTERVAL '365 days'),
  (random() * 10000)::decimal(10,2),
  'account_tier_' || (floor(random() * 5) + 1)::text,
  'tier_' || (floor(random() * 5) + 1)::text || '_metadata'
FROM generate_series(1, 2500);

-- Query that will show suboptimal performance
EXPLAIN ANALYZE
SELECT 'account_tier_1' as metadata_key, metadata_value
FROM accounts7
WHERE account_type = 'savings'
  AND region_code = 'US'
  AND status = 'active'
  AND metadata_key = 'account_tier_1'
  AND created_at <= CURRENT_TIMESTAMP
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year';

-- Better index to fix the issue
CREATE INDEX ASYNC idx_accounts7_metadata ON accounts7(metadata_key, account_type);

-- Even better index with INCLUDE
CREATE INDEX ASYNC idx_accounts7_metadata_covering 
ON accounts7(metadata_key, account_type) 
INCLUDE (region_code, status, created_at, metadata_value);

-- Same query should now perform better
EXPLAIN ANALYZE
SELECT 'account_tier_1' as metadata_key, metadata_value
FROM accounts7
WHERE account_type = 'savings'
  AND region_code = 'US'
  AND status = 'active'
  AND metadata_key = 'account_tier_1'
  AND created_at <= CURRENT_TIMESTAMP
  AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year';