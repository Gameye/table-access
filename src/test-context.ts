import pg from "pg";

export interface TestContext {
    pool: pg.Pool
}

export async function withTestContext(job: (context: TestContext) => Promise<void>) {
    const dbName = `db_${new Date().valueOf()}`
    const client = new pg.Client({
        user: "postgres",
        host: "localhost",
    })
    await client.connect();
    try {
        await client.query(`CREATE DATABASE ${client.escapeIdentifier(dbName)};`);
        try {
            const pool = new pg.Pool({
                user: "postgres",
                host: "localhost",
                database: dbName,
            });
            try {
                await job({ pool });
            }
            finally {
                await pool.end()
            }

        }
        finally {
            await client.query(`DROP DATABASE ${client.escapeIdentifier(dbName)};`);
        }
    }
    finally {
        await client.end()
    }

}