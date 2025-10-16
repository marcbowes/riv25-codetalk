import { Handler } from 'aws-lambda';
import { getPool } from './db';

interface Request {
  payer_id: number;
  payee_id: number;
  amount: number;
}

interface Response {
  balance: number;
}

export const handler: Handler<Request, Response> = async (event) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    // Begin transaction
    await client.query('BEGIN');

    // Deduct from payer
    const deductResult = await client.query(
      'UPDATE accounts SET balance = balance - $1 WHERE id = $2 RETURNING balance',
      [event.amount, event.payer_id]
    );

    if (deductResult.rows.length === 0) {
      throw new Error('Payer account not found');
    }

    const payerBalance = deductResult.rows[0].balance;

    if (payerBalance < 0) {
      throw new Error('Insufficient balance');
    }

    // Add to payee
    const addResult = await client.query(
      'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
      [event.amount, event.payee_id]
    );

    if (addResult.rowCount === 0) {
      throw new Error('Payee account not found');
    }

    // Commit transaction
    await client.query('COMMIT');

    return {
      balance: payerBalance
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
};
