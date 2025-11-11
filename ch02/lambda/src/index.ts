import { Handler } from "aws-lambda";
import { getPool } from "./db";

interface Request {
  payer_id: number;
  payee_id: number;
  amount: number;
}

interface Response {
  balance?: number;
  error?: string;
  errorCode?: string;
  duration: number;
  retries?: number;
}

function isOccError(error: any): boolean {
  // Check for DSQL OCC error code (PostgreSQL serialization failure)
  return error?.code === "40001";
}

async function performTransfer(
  client: any,
  payerId: number,
  payeeId: number,
  amount: number,
): Promise<number> {
  // Begin transaction
  await client.query("BEGIN");

  // Deduct from payer
  const deductResult = await client.query(
    "UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance",
    [amount, payerId],
  );

  if (deductResult.rows.length === 0) {
    throw new Error("Payer account not found");
  }

  const payerBalance = deductResult.rows[0].balance;

  if (payerBalance < 0) {
    throw new Error("Insufficient balance");
  }

  // Add to payee
  const addResult = await client.query(
    "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
    [amount, payeeId],
  );

  if (addResult.rowCount === 0) {
    throw new Error("Payee account not found");
  }

  // Commit transaction
  await client.query("COMMIT");

  return payerBalance;
}

export const handler: Handler<Request, Response> = async (event) => {
  const startTime = Date.now();
  const pool = await getPool();
  const client = await pool.connect();

  let retryCount = 0;

  try {
    // Retry loop for OCC conflicts - retry indefinitely
    while (true) {
      try {
        const balance = await performTransfer(
          client,
          event.payer_id,
          event.payee_id,
          event.amount,
        );

        const duration = Date.now() - startTime;
        return {
          balance,
          duration,
          retries: retryCount,
        };
      } catch (error) {
        // Rollback on any error
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          // Ignore rollback errors
        }

        // Check if it's an OCC error - if so, retry
        if (isOccError(error)) {
          retryCount++;
          continue;
        }

        // If not an OCC error, return the error
        const duration = Date.now() - startTime;
        return {
          error: error instanceof Error ? error.message : "Unknown error",
          errorCode: error?.code,
          duration,
          retries: retryCount,
        };
      }
    }
  } finally {
    client.release();
  }
};
