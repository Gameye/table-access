import pg from "pg";
import { Promisable } from "type-fest";
import { Access } from "./access.js";

export async function withTransaction<T>(
    poolOrClient: pg.Pool | pg.ClientBase,
    job: (access: Access) => Promisable<T>,
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
        const access = new Access(client);
        const result = await job(access);
        await client.query("COMMIT TRANSACTION;");
        return result;
    }
    catch (err) {
        await client.query("ROLLBACK TRANSACTION;");
        throw err;
    }
}
