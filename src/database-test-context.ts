import * as pg from "pg";
import { DisposableComposition } from "using-disposable";

let key = new Date().valueOf();

export class DatabaseTestContext extends DisposableComposition {
    public static async create(
        sql: string,
        poolConfig: pg.PoolConfig = {},
    ): Promise<DatabaseTestContext> {
        const context = new DatabaseTestContext(
            sql,
            poolConfig,
        );
        await context.setupDatabase();
        await context.setupPool();
        return context;
    }

    public pool: pg.Pool;
    private databaseName = `${this.poolConfig.database || ""}_${(++key).toString(36)}`;

    private constructor(
        private sql: string,
        private poolConfig: pg.PoolConfig = {},
    ) {
        super();
    }

    private async setupDatabase() {
        const { databaseName, poolConfig } = this;
        const adminPool = new pg.Pool(poolConfig);
        try {
            await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}";`);
            await adminPool.query(`CREATE DATABASE "${databaseName}";`);
            this.registerDisposable({ dispose: () => this.teardownDatabase() });
        }
        finally {
            await adminPool.end();
        }
    }

    private async teardownDatabase() {
        const { databaseName, poolConfig } = this;
        const adminPool = new pg.Pool(poolConfig);
        try {
            await adminPool.query(`DROP DATABASE "${databaseName}";`);
        }
        finally {
            await adminPool.end();
        }
    }

    private async setupPool() {
        const { databaseName, poolConfig } = this;
        this.pool = new pg.Pool({ ...poolConfig, ...{ database: databaseName } });
        this.registerDisposable({ dispose: () => this.teardownPool() });
        await this.applySql();
    }

    private async teardownPool() {
        await this.pool.end();
    }

    private async applySql() {
        const { pool, sql } = this;
        if (pool === null) throw new Error("pool not initialized");

        await pool.query(sql);
    }
}
