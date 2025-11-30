import { Handler } from "aws-lambda";
import { eq, sql } from "drizzle-orm";
import { getDb, withOccRetry, isPgError } from "./db";
import { accounts, transactions } from "./schema";

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

export const handler: Handler<Request, Response> = async (event) => {
  const startTime = Date.now();
  const db = await getDb();

  try {
    const { result: balance, retries } = await withOccRetry(() =>
      db.transaction(async (tx) => {
        // Deduct from payer
        const deductResult = await tx
          .update(accounts)
          .set({ balance: sql`${accounts.balance} - ${event.amount}` })
          .where(eq(accounts.id, event.payer_id))
          .returning({ balance: accounts.balance });

        if (deductResult.length === 0) {
          throw new Error("Payer account not found");
        }

        const payerBalance = deductResult[0].balance;

        if (payerBalance < 0) {
          throw new Error("Insufficient balance");
        }

        // Add to payee
        const addResult = await tx
          .update(accounts)
          .set({ balance: sql`${accounts.balance} + ${event.amount}` })
          .where(eq(accounts.id, event.payee_id))
          .returning({ id: accounts.id });

        if (addResult.length === 0) {
          throw new Error("Payee account not found");
        }

        // Record transaction history
        await tx.insert(transactions).values({
          payerId: event.payer_id,
          payeeId: event.payee_id,
          amount: event.amount,
        });

        return payerBalance;
      }),
    );

    return {
      balance,
      duration: Date.now() - startTime,
      retries,
    };
  } catch (error: unknown) {
    const duration = Date.now() - startTime;

    if (isPgError(error)) {
      return {
        error: error.message,
        errorCode: error.code,
        duration,
      };
    }

    return {
      error: error instanceof Error ? error.message : "Unknown error",
      duration,
    };
  }
};
