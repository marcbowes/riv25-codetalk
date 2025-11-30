import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { DsqlSigner } from "@aws-sdk/dsql-signer";
import * as schema from "./schema";

let pool: Pool | null = null;
let db: ReturnType<typeof drizzle<typeof schema>> | null = null;

export async function getDb() {
  if (db) {
    return db;
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

  db = drizzle(pool, { schema });

  return db;
}

export function isOccError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code: string }).code === "40001"
  );
}

export function isPgError(
  error: unknown,
): error is { code: string; message: string } {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    "message" in error
  );
}

export interface OccResult<T> {
  result: T;
  retries: number;
}

/**
 * Executes an async operation with automatic OCC retry logic.
 * Retries indefinitely on PostgreSQL error code 40001 (serialization failure).
 * Re-throws all other errors.
 */
export async function withOccRetry<T>(
  operation: () => Promise<T>,
): Promise<OccResult<T>> {
  let retries = 0;

  while (true) {
    try {
      const result = await operation();
      return { result, retries };
    } catch (error: unknown) {
      if (isOccError(error)) {
        retries++;
        continue;
      }
      throw error;
    }
  }
}
