import { Handler } from 'aws-lambda';
import { DsqlSigner } from '@aws-sdk/dsql-signer';
import postgres, { Sql, PostgresError } from 'postgres';

interface Request {
  payer_id: string;  // UUID
  payee_id: string;  // UUID
  amount: string;
}

interface Response {
  payer_balance: string;
  transaction_time: string;
  attempts: number;
}

const CLUSTER_ENDPOINT = process.env.CLUSTER_ENDPOINT || 'YOUR_CLUSTER_ENDPOINT';
const REGION = process.env.REGION || 'us-west-2';
const USER = 'admin';

// Connection reuse - create once and reuse across invocations
let cachedClient: Sql | null = null;

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

async function getConnection(clusterEndpoint: string, user: string, region: string): Promise<Sql> {
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

function isOccError(error: any): boolean {
  // PostgreSQL serialization failure error code
  return error?.code === '40001';
}

async function executeTransfer(sql: Sql, request: Request): Promise<string> {
  // Deduct from payer and check balance
  const payerRows = await sql`
    UPDATE accounts2
    SET balance = balance - ${request.amount}
    WHERE id = ${request.payer_id}
    RETURNING balance
  `;

  if (payerRows.length === 0) {
    throw new Error('Payer account not found');
  }

  const payerBalance = parseFloat(payerRows[0].balance);
  if (payerBalance < 0) {
    throw new Error(`Insufficient balance: ${payerBalance}`);
  }

  // Add to payee
  const payeeResult = await sql`
    UPDATE accounts2
    SET balance = balance + ${request.amount}
    WHERE id = ${request.payee_id}
  `;

  if (payeeResult.count !== 1) {
    throw new Error('Payee account not found');
  }

  return payerBalance.toString();
}

export const handler: Handler<Request, Response> = async (event) => {
  const start = Date.now();

  if (event.payer_id === event.payee_id) {
    throw new Error('Payer and payee must be different accounts');
  }

  try {
    const client = await getConnection(CLUSTER_ENDPOINT, USER, REGION);

    // Retry loop for OCC failures
    let attempts = 0;
    let payerBalance: string;

    while (true) {
      attempts++;

      try {
        // Execute transaction with retry on OCC error
        payerBalance = await client.begin(async (sql) => {
          return await executeTransfer(sql, event);
        });

        // Transaction committed successfully
        break;
      } catch (error) {
        // Check if this is an OCC error (serialization failure)
        if (isOccError(error)) {
          // Retry on OCC error
          continue;
        }

        // For non-OCC errors, rethrow
        throw error;
      }
    }

    const elapsed = Date.now() - start;
    const transactionTime = `${elapsed.toFixed(3)}ms`;

    return {
      payer_balance: payerBalance,
      transaction_time: transactionTime,
      attempts
    };

  } catch (error) {
    console.error('Error:', error);
    throw error;
  }
};
