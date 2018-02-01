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
        await context.initialize();
        return context;
    }

    private pool: pg.Pool = undefined as any;
    private databaseName = `${this.poolConfig.database || ""}_${(++key).toString(36)}`;

    private constructor(
        private sql: string,
        private poolConfig: pg.PoolConfig = {},
    ) {
        super();
    }

    public getPool() {
        if (this.pool === undefined) throw new Error("pool not initialized");
        return this.pool;
    }

    private async initialize() {
        await this.setupDatabase();
        this.registerDisposable({ dispose: () => this.teardownDatabase() });

        await this.setupPool();
        this.registerDisposable({ dispose: () => this.teardownPool() });

        await this.applySql();
    }

    private async setupDatabase() {
        const { databaseName, poolConfig } = this;
        const adminPool = new pg.Pool(poolConfig);
        try {
            await adminPool.query(`DROP DATABASE IF EXISTS "${databaseName}";`);
            await adminPool.query(`CREATE DATABASE "${databaseName}";`);
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
    }

    private async teardownPool() {
        const { pool } = this;
        await pool.end();
    }

    private async applySql() {
        const { pool, sql } = this;
        await pool.query(sql);
    }
}
