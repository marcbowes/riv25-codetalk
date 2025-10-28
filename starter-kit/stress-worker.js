const { parentPort, workerData } = require("worker_threads");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const { NodeHttpHandler } = require("@smithy/node-http-handler");

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function runInvocations() {
    const {
        workerId,
        parallelCalls,
        iterations,
        numAccounts,
        functionName,
    } = workerData;

    // Configure Lambda client with enough sockets for high concurrency
    const client = new LambdaClient({
        requestHandler: new NodeHttpHandler({
            connectionTimeout: 5000,
            socketTimeout: 30000,
            maxSockets: Math.max(parallelCalls * 2, 1000),
            socketAcquisitionWarningTimeout: 60000,
        }),
    });

    const stats = {
        success: 0,
        errors: 0,
        totalDuration: 0,
        minDuration: Infinity,
        maxDuration: 0,
        totalRetries: 0,
        maxRetries: 0,
        transactionsWithRetries: 0,
        errorTypes: {},
    };

    for (let iteration = 0; iteration < iterations; iteration++) {
        const promises = [];

        for (let i = 0; i < parallelCalls; i++) {
            const payerId = randomInt(1, numAccounts);
            let payeeId = randomInt(1, numAccounts);
            while (payeeId === payerId) {
                payeeId = randomInt(1, numAccounts);
            }

            const payload = {
                payer_id: payerId,
                payee_id: payeeId,
                amount: 1,
            };

            const command = new InvokeCommand({
                FunctionName: functionName,
                Payload: JSON.stringify(payload),
            });

            const promise = client
                .send(command)
                .then((response) => {
                    const responsePayload = JSON.parse(
                        Buffer.from(response.Payload).toString(),
                    );

                    if (responsePayload.error) {
                        stats.errors++;
                        const errorKey = responsePayload.errorCode
                            ? `${responsePayload.error} (${responsePayload.errorCode})`
                            : responsePayload.error;
                        stats.errorTypes[errorKey] =
                            (stats.errorTypes[errorKey] || 0) + 1;
                    } else {
                        stats.success++;
                    }

                    if (responsePayload.duration !== undefined) {
                        stats.totalDuration += responsePayload.duration;
                        stats.minDuration = Math.min(
                            stats.minDuration,
                            responsePayload.duration,
                        );
                        stats.maxDuration = Math.max(
                            stats.maxDuration,
                            responsePayload.duration,
                        );
                    }

                    if (responsePayload.retries !== undefined) {
                        const retries = responsePayload.retries;
                        stats.totalRetries += retries;
                        stats.maxRetries = Math.max(stats.maxRetries, retries);
                        if (retries > 0) {
                            stats.transactionsWithRetries++;
                        }
                    }
                })
                .catch((err) => {
                    stats.errors++;
                    const errorType = err.message || "Unknown error";
                    stats.errorTypes[errorType] =
                        (stats.errorTypes[errorType] || 0) + 1;
                });

            promises.push(promise);
        }

        await Promise.all(promises);

        // Report progress after each iteration
        parentPort.postMessage({
            type: "progress",
            workerId,
            completed: (iteration + 1) * parallelCalls,
        });
    }

    // Send final stats
    parentPort.postMessage({
        type: "done",
        workerId,
        stats,
    });
}

runInvocations().catch((error) => {
    parentPort.postMessage({
        type: "error",
        workerId: workerData.workerId,
        error: error.message,
    });
    process.exit(1);
});
