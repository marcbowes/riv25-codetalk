import { Pool } from "pg";
import { DsqlSigner } from "@aws-sdk/dsql-signer";

export interface PgError extends Error {
  code: string;
  detail?: string;
  hint?: string;
  position?: string;
  internalPosition?: string;
  internalQuery?: string;
  where?: string;
  schema?: string;
  table?: string;
  column?: string;
  dataType?: string;
  constraint?: string;
}

export function isPgError(error: unknown): error is PgError {
  return error instanceof Error && "code" in error;
}

export function isOccError(error: PgError): boolean {
  return error.code === "40001";
}

let pool: Pool | null = null;

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
