import pg from "pg";
import { Promisable } from "type-fest";
import { Access } from "./access.js";

export async function withTransaction<T>(
    pool: pg.Pool,
    job: (context: Access) => Promisable<T>,
): Promise<T> {
    const client = await pool.connect();
    const context = new Access(client);

    await client.query("BEGIN TRANSACTION;");
    try {
        const result = await job(context);
        await client.query("COMMIT TRANSACTION;");
        client.release();
        return result;
    }
    catch (err) {
        await client.query("ROLLBACK TRANSACTION;");
        client.release(true);
        throw err;
    }
}
