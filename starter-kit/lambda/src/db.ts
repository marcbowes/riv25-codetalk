import { Pool } from 'pg';

let pool: Pool | null = null;

export async function getPool(): Promise<Pool> {
  if (pool) {
    return pool;
  }

  pool = new Pool({
    host: process.env.CLUSTER_ENDPOINT!,
    port: 5432,
    database: 'postgres',
    user: 'admin',
    password: 'placeholder', // TODO: Replace with DSQL auth in Ch01
    ssl: true,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  return pool;
}
