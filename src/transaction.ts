import pg from "pg";
import { Promisable } from "type-fest";

export async function withTransaction<T>(
    poolOrClient: pg.Pool | pg.ClientBase,
    job: (client: pg.ClientBase) => Promisable<T>,
): Promise<T> {
    if (poolOrClient instanceof pg.Pool) {
        const client = await poolOrClient.connect();
        try {
            const result = withTransaction(client, job);
            client.release();
            return result;
        }
        catch (error) {
            client.release(true);
            throw error;
        }

    }

    const client = poolOrClient;
    await client.query("BEGIN TRANSACTION;");
    try {
        const result = await job(client);
        await client.query("COMMIT TRANSACTION;");
        return result;
    }
    catch (err) {
        await client.query("ROLLBACK TRANSACTION;");
        throw err;
    }
}
