import { Conflict, NotFound } from "http-errors";
import * as pg from "pg";
import { makeRowFilterPg, RowFilter } from "./row-filter";
import { TableDescriptor } from "./table-descriptor";

export class TableQuery {

    public static async query<T>(
        pool: pg.Pool,
        job: (context: TableQuery) => Promise<T>,
    ): Promise<T> {
        const client = await pool.connect();
        const context = new this(client);

        await client.query("BEGIN TRANSACTION;");
        try {
            const result = await job(context);
            await client.query("COMMIT TRANSACTION;");
            return result;
        }
        catch (err) {
            await client.query("ROLLBACK TRANSACTION;");
            throw err;
        }
        finally {
            client.release();
        }
    }

    private constructor(
        private readonly client: pg.Client,
    ) { }

    public async single<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: RowFilter<Row> | Partial<Row>,
    ): Promise<Row> {
        const { schema, table } = descriptor;
        const record = await this.singleOrNull(descriptor, filter);
        if (!record) throw new NotFound(
            `No result for ${schema}.${table} select`,
        );
        return record;
    }

    public async singleOrNull<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: RowFilter<Row> | Partial<Row>,
    ): Promise<Row | null> {
        const { schema, table } = descriptor;
        const records = await this.multiple(descriptor, filter);

        if (records.length < 1) return null;
        if (records.length > 1) throw new Conflict(
            `More than one result for ${schema}.${table} select`,
        );

        const [record] = records;
        return record;
    }

    public async multiple<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: RowFilter<Row> | Partial<Row>,
    ): Promise<Row[]> {
        const { client } = this;
        const { schema, table } = descriptor;
        const filterResult = makeRowFilterPg(filter, "r");

        const result = await client.query(`
SELECT row_to_json(r) AS o
FROM "${schema}"."${table}" AS r
${filterResult.paramCount ? `WHERE ${filterResult.filterSql}` : ""}
;`, filterResult.param);

        const { rows } = result;

        return rows.map(row => row.o);
    }

    public async insert<Row extends object>(
        descriptor: TableDescriptor<Row>,
        item: Partial<Row>,
    ): Promise<Row> {
        const { client } = this;
        const { schema, table } = descriptor;

        const itemFields = Object.keys(item) as Array<keyof Row>;
        const itemValues = itemFields.map(f => item[f]);

        const result = await client.query(`
WITH r AS (
    INSERT INTO "${schema}"."${table}" (${itemFields.map(f => `"${f}"`).join(",")})
    VALUES (${itemFields.map((f, i) => `$${i + 1}`).join(",")})
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, itemValues);

        const { rows } = result;
        if (rows.length < 1)
            throw new NotFound(
                `No result for ${schema}.${table} insert`,
            );
        if (rows.length > 1)
            throw new Conflict(
                `More than one result for ${schema}.${table} insert`,
            );

        const [row] = rows;

        return row.o;
    }

    public async update<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: Partial<Row>,
        item: Partial<Row>,
    ): Promise<Row> {
        const { client } = this;
        const { schema, table } = descriptor;

        const filterFields = Object.keys(filter) as Array<keyof Row>;
        const filterValues = filterFields.map(f => filter[f]);

        const itemFields = Object.keys(item) as Array<keyof Row>;
        const itemValues = itemFields.map(f => item[f]);

        const result = await client.query(`
WITH r AS (
    UPDATE "${schema}"."${table}"
    SET ${itemFields.map((f, i) => `"${f}"=$${i + 1 + filterFields.length}`).join(",")}
    WHERE ${filterFields.map((f, i) => `"${f}"=$${i + 1}`).join(" AND ")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, [...filterValues, ...itemValues]);

        const { rows } = result;
        if (rows.length < 1) throw new NotFound(
            `No result for ${schema}.${table} update`,
        );
        if (rows.length > 1) throw new Conflict(
            `More than one result for ${schema}.${table} update`,
        );

        const [row] = rows;

        return row.o;
    }

    public async upsert<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: Partial<Row>,
        item: Partial<Row>,
    ): Promise<Row> {
        const { client } = this;
        const { schema, table } = descriptor;

        const filterFields = Object.keys(filter) as Array<keyof Row>;
        const filterValues = filterFields.map(f => filter[f]);

        const itemFields = Object.keys(item) as Array<keyof Row>;
        const itemValues = itemFields.map(f => item[f]);

        const result = await client.query(`
WITH r AS (
    INSERT INTO "${schema}"."${table}" (
        ${[...filterFields, ...itemFields].map(f => `"${f}"`).join(",")}
    )
    VALUES (
        ${[...filterFields, ...itemFields].map((f, i) => `$${i + 1}`).join(",")}
    )
    ON CONFLICT (${filterFields.map(f => `"${f}"`).join(",")}) DO UPDATE
    SET ${itemFields.map((f, i) => `"${f}"=EXCLUDED.${f}`).join(",")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, [...filterValues, ...itemValues]);

        const { rows } = result;
        if (rows.length < 1) throw new NotFound(
            `No result for ${schema}.${table} upsert`,
        );
        if (rows.length > 1) throw new Conflict(
            `More than one result for ${schema}.${table} upsert`,
        );

        const [row] = rows;

        return row.o;
    }

    public async ensure<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: Partial<Row>,
        item: Partial<Row>,
    ): Promise<Row | null> {
        const { client } = this;
        const { schema, table } = descriptor;

        const filterFields = Object.keys(filter) as Array<keyof Row>;
        const filterValues = filterFields.map(f => filter[f]);

        const itemFields = Object.keys(item) as Array<keyof Row>;
        const itemValues = itemFields.map(f => item[f]);

        const result = await client.query(`
WITH r AS (
    INSERT INTO "${schema}"."${table}" (
        ${[...filterFields, ...itemFields].map(f => `"${f}"`).join(",")}
    )
    VALUES (
        ${[...filterFields, ...itemFields].map((f, i) => `$${i + 1}`).join(",")}
    )
    ON CONFLICT (${filterFields.map(f => `"${f}"`).join(",")}) DO NOTHING
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, [...filterValues, ...itemValues]);

        const { rows } = result;
        if (rows.length < 1) return null;
        if (rows.length > 1) throw new Conflict(
            `More than one result for ${schema}.${table} ensure`,
        );

        const [row] = rows;

        return row.o;
    }

    public async delete<Row extends object>(
        descriptor: TableDescriptor<Row>,
        filter: Partial<Row>,
    ): Promise<Row> {
        const { client } = this;
        const { schema, table } = descriptor;

        const filterFields = Object.keys(filter) as Array<keyof Row>;
        const filterValues = filterFields.map(f => filter[f]);

        const result = await client.query(`
WITH r AS (
    DELETE FROM "${schema}"."${table}"
    WHERE ${filterFields.map((f, i) => `"${f}"=$${i + 1}`).join(" AND ")}
    RETURNING *
)
SELECT row_to_json(r) AS o
FROM r
;`, filterValues);

        const { rows } = result;
        if (rows.length < 1) throw new NotFound(
            `No result for ${schema}.${table} delete`,
        );
        if (rows.length > 1) throw new Conflict(
            `More than one result for ${schema}.${table} delete`,
        );

        const [row] = rows;

        return row.o;
    }
}
