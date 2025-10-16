import { Handler } from 'aws-lambda';
import { DsqlSigner } from '@aws-sdk/dsql-signer';
import postgres, { Sql } from 'postgres';

interface Request {
  payer_id: number;
  payee_id: number;
  amount: string;
}

interface Response {
  payer_balance: string;
  transaction_time: string;
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

export const handler: Handler<Request, Response> = async (event) => {
  const start = Date.now();

  if (event.payer_id === event.payee_id) {
    throw new Error('Payer and payee must be different accounts');
  }

  try {
    const client = await getConnection(CLUSTER_ENDPOINT, USER, REGION);

    // Begin transaction and execute transfer
    const result = await client.begin(async (sql) => {
      // Deduct from payer and check balance
      const payerRows = await sql`
        UPDATE accounts
        SET balance = balance - ${event.amount}
        WHERE id = ${event.payer_id}
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
        UPDATE accounts
        SET balance = balance + ${event.amount}
        WHERE id = ${event.payee_id}
      `;

      if (payeeResult.count !== 1) {
        throw new Error('Payee account not found');
      }

      return {
        payer_balance: payerBalance.toString()
      };
    });

    const elapsed = Date.now() - start;
    const transactionTime = `${elapsed.toFixed(3)}ms`;

    return {
      payer_balance: result.payer_balance,
      transaction_time: transactionTime
    };

  } catch (error) {
    console.error('Error:', error);
    throw error;
  }
};
