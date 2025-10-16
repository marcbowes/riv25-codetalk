import { Handler } from 'aws-lambda';
import { getPool } from './db';

interface Request {
  name: string;
}

interface Response {
  greeting: string;
}

export const handler: Handler<Request, Response> = async (event) => {
  const pool = await getPool();

  const result = await pool.query('SELECT 1');

  return {
    greeting: `Hello ${event.name}, connected to DSQL successfully!`
  };
};
