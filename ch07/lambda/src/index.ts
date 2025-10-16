import { Handler } from 'aws-lambda';
import { DsqlSigner } from '@aws-sdk/dsql-signer';
import postgres from 'postgres';

interface Request {
  operation: 'setup' | 'query' | 'optimize' | 'query_optimized';
  metadata_key?: string;
  account_type?: string;
  region_code?: string;
  status?: string;
}

interface Response {
  operation: string;
  execution_time_ms?: number;
  rows_returned?: number;
  query_plan?: string;
  message?: string;
}

const CLUSTER_ENDPOINT = process.env.CLUSTER_ENDPOINT || '';
const REGION = process.env.REGION || 'us-west-2';
const USER = 'admin';

// Connection reuse - create once and reuse across invocations
let cachedClient: postgres.Sql | null = null;

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

async function getConnection(clusterEndpoint: string, user: string, region: string): Promise<postgres.Sql> {
  if (cachedClient) {
    return cachedClient;
  }

  cachedClient = postgres({
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

  return cachedClient;
}

async function setupDatabase(client: postgres.Sql): Promise<string> {
  try {
    // Drop table if exists
    await client`DROP TABLE IF EXISTS accounts7 CASCADE`;
    
    // Create the table
    await client`
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
      )
    `;

    // Create initial suboptimal index
    await client`CREATE INDEX ASYNC idx_accounts7_type ON accounts7(account_type)`;

    // Insert batch 1 (2500 rows)
    await client`
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
      FROM generate_series(1, 2500)
    `;

    // Insert batch 2 (2500 rows)
    await client`
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
      FROM generate_series(1, 2500)
    `;

    return 'Database setup completed with 5000 rows and suboptimal index';
  } catch (error) {
    throw new Error(`Setup failed: ${error}`);
  }
}

async function runQuery(client: postgres.Sql, metadata_key: string, account_type: string, region_code: string, status: string): Promise<{ execution_time: number, rows: any[], query_plan: string }> {
  const startTime = Date.now();
  
  // Get query plan
  const planResult = await client`
    EXPLAIN ANALYZE
    SELECT ${metadata_key} as metadata_key, metadata_value
    FROM accounts7
    WHERE account_type = ${account_type}
      AND region_code = ${region_code}
      AND status = ${status}
      AND metadata_key = ${metadata_key}
      AND created_at <= CURRENT_TIMESTAMP
      AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year'
  `;

  // Execute actual query
  const rows = await client`
    SELECT ${metadata_key} as metadata_key, metadata_value
    FROM accounts7
    WHERE account_type = ${account_type}
      AND region_code = ${region_code}
      AND status = ${status}
      AND metadata_key = ${metadata_key}
      AND created_at <= CURRENT_TIMESTAMP
      AND created_at > CURRENT_TIMESTAMP - INTERVAL '1 year'
  `;

  const endTime = Date.now();
  const execution_time = endTime - startTime;

  return {
    execution_time,
    rows,
    query_plan: planResult.map(row => row['QUERY PLAN']).join('\n')
  };
}

async function optimizeIndexes(client: postgres.Sql): Promise<string> {
  try {
    // Create better index
    await client`CREATE INDEX ASYNC idx_accounts7_metadata ON accounts7(metadata_key, account_type)`;
    
    // Create covering index
    await client`
      CREATE INDEX ASYNC idx_accounts7_metadata_covering 
      ON accounts7(metadata_key, account_type) 
      INCLUDE (region_code, status, created_at, metadata_value)
    `;

    return 'Optimization indexes created: idx_accounts7_metadata and idx_accounts7_metadata_covering';
  } catch (error) {
    throw new Error(`Optimization failed: ${error}`);
  }
}

export const handler: Handler<Request, Response> = async (event) => {
  let client;

  try {
    client = await getConnection(CLUSTER_ENDPOINT, USER, REGION);

    switch (event.operation) {
      case 'setup':
        const setupMessage = await setupDatabase(client);
        return {
          operation: 'setup',
          message: setupMessage
        };

      case 'query':
        const metadata_key = event.metadata_key || 'account_tier_1';
        const account_type = event.account_type || 'savings';
        const region_code = event.region_code || 'US';
        const status = event.status || 'active';

        const queryResult = await runQuery(client, metadata_key, account_type, region_code, status);
        
        return {
          operation: 'query',
          execution_time_ms: queryResult.execution_time,
          rows_returned: queryResult.rows.length,
          query_plan: queryResult.query_plan
        };

      case 'optimize':
        const optimizeMessage = await optimizeIndexes(client);
        return {
          operation: 'optimize',
          message: optimizeMessage
        };

      case 'query_optimized':
        const metadata_key_opt = event.metadata_key || 'account_tier_1';
        const account_type_opt = event.account_type || 'savings';
        const region_code_opt = event.region_code || 'US';
        const status_opt = event.status || 'active';

        const optimizedResult = await runQuery(client, metadata_key_opt, account_type_opt, region_code_opt, status_opt);
        
        return {
          operation: 'query_optimized',
          execution_time_ms: optimizedResult.execution_time,
          rows_returned: optimizedResult.rows.length,
          query_plan: optimizedResult.query_plan
        };

      default:
        throw new Error(`Unknown operation: ${event.operation}`);
    }
  } catch (error) {
    console.error('Error:', error);
    throw error;
  }
};