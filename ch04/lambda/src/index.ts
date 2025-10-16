import { Handler } from 'aws-lambda';
import { DsqlSigner } from '@aws-sdk/dsql-signer';
import postgres from 'postgres';

interface Request {
  id: number;
}

interface Response {
  balance: string;
}

const CLUSTER_ENDPOINT = process.env.CLUSTER_ENDPOINT || 'YOUR_CLUSTER_ENDPOINT';
const REGION = process.env.REGION || 'us-west-2';
const USER = 'admin';

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

async function getConnection(clusterEndpoint: string, user: string, region: string) {
  const client = postgres({
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

  return client;
}

export const handler: Handler<Request, Response> = async (event) => {
  let client;

  try {
    client = await getConnection(CLUSTER_ENDPOINT, USER, REGION);

    const rows = await client`
      SELECT balance FROM accounts WHERE id = ${event.id}
    `;

    if (rows.length === 0) {
      throw new Error(`Account ${event.id} not found`);
    }

    const balance = rows[0].balance;

    return {
      balance: balance.toString()
    };
  } catch (error) {
    console.error('Error:', error);
    throw error;
  } finally {
    await client?.end();
  }
};
