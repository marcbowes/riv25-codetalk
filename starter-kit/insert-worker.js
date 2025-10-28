const { parentPort, workerData } = require("worker_threads");
const { Client } = require("pg");
const { DsqlSigner } = require("@aws-sdk/dsql-signer");

async function getDsqlClient() {
    const clusterEndpoint = process.env.CLUSTER_ENDPOINT;
    if (!clusterEndpoint) {
        throw new Error("CLUSTER_ENDPOINT environment variable not set");
    }

    const region = process.env.AWS_REGION || "us-west-2";
    const signer = new DsqlSigner({
        hostname: clusterEndpoint,
        region,
    });

    const client = new Client({
        host: clusterEndpoint,
        port: 5432,
        database: "postgres",
        user: "admin",
        password: async () => await signer.getDbConnectAdminAuthToken(),
        ssl: {
            rejectUnauthorized: true,
            maxVersion: "TLSv1.2",
        },
    });

    await client.connect();
    return client;
}

async function insertAccounts() {
    const { workerId, startId, endId, batchSize } = workerData;
    const client = await getDsqlClient();

    try {
        const numBatches = Math.ceil((endId - startId + 1) / batchSize);

        for (let batch = 0; batch < numBatches; batch++) {
            const batchStartId = startId + batch * batchSize;
            const batchEndId = Math.min(
                startId + (batch + 1) * batchSize - 1,
                endId,
            );

            await client.query(
                `INSERT INTO accounts (id, balance)
                 SELECT id, 100
                 FROM generate_series($1::INTEGER, $2::INTEGER) AS id`,
                [batchStartId, batchEndId],
            );

            parentPort.postMessage({
                type: "progress",
                workerId,
                startId: batchStartId,
                endId: batchEndId,
            });
        }

        parentPort.postMessage({ type: "done", workerId });
    } catch (error) {
        parentPort.postMessage({
            type: "error",
            workerId,
            error: error.message,
        });
    } finally {
        await client.end();
    }
}

insertAccounts();
